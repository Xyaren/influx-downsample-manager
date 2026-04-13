import datetime
import json
from abc import ABC, abstractmethod

from pytimeparse.timeparse import timeparse

from .model import FieldData, DownsampleConfiguration
from .utils import timedelta_to_flux_duration, hash_to_integer


class BaseQueryGenerator(ABC):
    """Shared scaffolding for Flux downsampling query generation."""

    def __init__(self,
                 source_bucket: str,
                 target_bucket: str,
                 downsample_config: DownsampleConfiguration,
                 measurement: str,
                 fields: dict[str, FieldData],
                 task_prefix: str = "gen_"):
        self.task_prefix = task_prefix
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket
        self.downsample_config = downsample_config
        self.measurement = measurement
        self.fields = fields

        self.interval = self.downsample_config["interval"]
        self.every = self.downsample_config["every"]
        self.expires = self.downsample_config.get("expires")

        self.numeric_fields = [k for k, v in fields.items() if v.numeric]
        self.non_numeric_fields = [k for k, v in fields.items() if not v.numeric]

    def task_name(self) -> str:
        return f"{self.task_prefix}{self.target_bucket}_{self.measurement}"

    def offset_with_predictable_factor(self) -> datetime.timedelta:
        """
        Prevent all tasks from running at the exact same time.
        Hashes into millisecond space for better distribution, then rounds
        to whole seconds since InfluxDB task offsets don't support sub-second precision.
        """
        min_offset = timeparse(self.downsample_config["offset"])
        if "max_offset" not in self.downsample_config:
            return datetime.timedelta(seconds=min_offset)

        max_offset = timeparse(self.downsample_config["max_offset"] or min_offset)
        if min_offset == max_offset:
            return datetime.timedelta(seconds=min_offset)

        min_ms = min_offset * 1000
        max_ms = max_offset * 1000
        ms = hash_to_integer(self.task_name(), min_ms, max_ms)
        seconds = round(ms / 1000)
        return datetime.timedelta(seconds=seconds)

    def _build_non_numeric_query(self, range_expr: str, suffix: str = "") -> str:
        """Build Flux for non-numeric fields. Uses ``last`` regardless of variant."""
        fields_json = json.dumps(self.non_numeric_fields)
        lines = [
            f"nonNumericFields = {fields_json}",
            f'from(bucket:"{self.source_bucket}")',
            f"  |> range({range_expr})",
            f'  |> filter(fn: (r) => r._measurement == "{self.measurement}")',
            f"  |> filter(fn: (r) => contains(value: r._field, set: nonNumericFields))",
            f"  |> aggregateWindow(every: {self.interval}, fn: last)",
            f'  |> to(bucket:"{self.target_bucket}")',
        ]
        if suffix:
            lines.append(suffix)
        return "\n".join(lines) + "\n"

    @abstractmethod
    def _build_numeric_query(self, range_expr: str, suffix: str = "") -> str:
        """Build Flux for numeric fields — strategy differs per variant."""

    def _build_flux_body(self, range_expr: str, suffix_numeric: str = "", suffix_non_numeric: str = "") -> list[str]:
        """Assemble all Flux fragments for a complete query body."""
        fragments: list[str] = []
        if self.numeric_fields:
            fragments.append(self._build_numeric_query(range_expr, suffix_numeric))
        if self.non_numeric_fields:
            fragments.append(self._build_non_numeric_query(range_expr, suffix_non_numeric))
        return fragments

    def generate_task(self) -> str:
        imports = (
            'import "influxdata/influxdb/tasks"\n'
            'import "date"\n'
            '\n'
        )
        offset_as_influx_duration = timedelta_to_flux_duration(self.offset_with_predictable_factor())
        task_def = f'option task = {{name: "{self.task_name()}", every: {self.every}, offset: {offset_as_influx_duration}}}\n\n'
        prep = (
            "start = date.truncate(t: tasks.lastSuccess(orTime: -task.every), unit: 1m)\n"
            "\n"
        )
        fragments = self._build_flux_body(range_expr="start: start")
        return imports + task_def + prep + "\n\n".join(fragments)

    def generate_query(self, start: str, stop: str) -> str:
        preamble = (
            f'start = time(v:"{start}")\n'
            f'stop = time(v:"{stop}")\n'
            '\n'
        )
        fragments = self._build_flux_body(
            range_expr="start: start, stop: stop",
            suffix_numeric='  |> limit(n:1)\n  |> yield(name: "numeric")',
            suffix_non_numeric='  |> limit(n:1)\n  |> yield(name: "non_numeric")',
        )
        return preamble + "\n\n".join(fragments)

    def __str__(self) -> str:
        return f"{self.source_bucket} -> {self.target_bucket}: {self.measurement}"


class SourceQueryGenerator(BaseQueryGenerator):
    """Reads directly from the raw source bucket.

    Aggregates numeric fields with ``mean`` and non-numeric fields with
    ``last``.
    """

    def _build_numeric_query(self, range_expr: str, suffix: str = "") -> str:
        fields_json = json.dumps(self.numeric_fields)
        lines = [
            f"numericFields = {fields_json}",
            f'from(bucket:"{self.source_bucket}")',
            f"  |> range({range_expr})",
            f'  |> filter(fn: (r) => r._measurement == "{self.measurement}")',
            f"  |> filter(fn: (r) => contains(value: r._field, set: numericFields))",
            f"  |> aggregateWindow(every: {self.interval}, fn: mean)",
            f'  |> to(bucket:"{self.target_bucket}")',
        ]
        if suffix:
            lines.append(suffix)
        return "\n".join(lines) + "\n"


class ChainedQueryGenerator(BaseQueryGenerator):
    """Reads from a pre-aggregated (upstream) bucket.

    Applies the same ``mean`` / ``last`` aggregation as the source variant.
    This is a mean-of-means for numeric fields, which is acceptable for
    monitoring data with fairly uniform sample rates.  The key benefit is
    reduced query load — the coarser tier scans pre-aggregated data instead
    of raw points.

    Note: because no auxiliary ``__count`` fields are involved, switching a
    tier between ``chained: false`` and ``chained: true`` is safe at any
    time without data migration.
    """

    def _build_numeric_query(self, range_expr: str, suffix: str = "") -> str:
        fields_json = json.dumps(self.numeric_fields)
        lines = [
            f"numericFields = {fields_json}",
            f'from(bucket:"{self.source_bucket}")',
            f"  |> range({range_expr})",
            f'  |> filter(fn: (r) => r._measurement == "{self.measurement}")',
            f"  |> filter(fn: (r) => contains(value: r._field, set: numericFields))",
            f"  |> aggregateWindow(every: {self.interval}, fn: mean)",
            f'  |> to(bucket:"{self.target_bucket}")',
        ]
        if suffix:
            lines.append(suffix)
        return "\n".join(lines) + "\n"
