import datetime
import json

from pytimeparse.timeparse import timeparse

from .model import FieldData, DownsampleConfiguration
from .utils import timedelta_to_flux_duration, hash_to_integer


class QueryGenerator:
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
        self.expires = self.downsample_config["expires"] if "expires" in downsample_config else None

        self.numeric_fields = [k for k, v in fields.items() if v.numeric]
        self.non_numeric_fields = [k for k, v in fields.items() if v.numeric is False]

    def task_name(self) -> str:
        return f"{self.task_prefix}{self.target_bucket}_{self.measurement}"

    def offset_with_predictable_factor(self) -> datetime.timedelta:
        """
        Prevent all tasks from running at the exact same time. Dynamic offset should spread the load.
        """
        min_offset = timeparse(self.downsample_config["offset"])
        if "max_offset" not in self.downsample_config:
            return datetime.timedelta(seconds=min_offset)

        max_offset = timeparse(self.downsample_config["max_offset"] or min_offset)
        if min_offset == max_offset:
            return datetime.timedelta(seconds=min_offset)

        seconds = hash_to_integer(self.task_name(), min_offset, max_offset)
        return datetime.timedelta(seconds=seconds)

    def _build_field_query(
        self,
        field_names: list[str],
        var_name: str,
        agg_fn: str,
        range_expr: str,
        suffix: str = "",
    ) -> str:
        """Build a Flux sub-query for a set of fields with a given aggregation function."""
        lines = [
            f"{var_name} = {json.dumps(field_names)}",
            f'from(bucket:"{self.source_bucket}")',
            f"  |> range({range_expr})",
            f'  |> filter(fn: (r) => r._measurement == "{self.measurement}")',
            f'  |> filter(fn: (r) => contains(value: r._field, set: {var_name}))',
            f"  |> aggregateWindow(every: {self.interval}, fn: {agg_fn})",
            f'  |> to(bucket:"{self.target_bucket}")',
        ]
        if suffix:
            lines.append(suffix)
        return "\n".join(lines) + "\n"

    def _build_flux_body(self, range_expr: str, suffix_numeric: str = "", suffix_non_numeric: str = "") -> list[str]:
        """Build the shared Flux query fragments for both task and ad-hoc query modes."""
        fragments: list[str] = []
        if self.numeric_fields:
            fragments.append(
                self._build_field_query(self.numeric_fields, "numericFields", "mean", range_expr, suffix_numeric)
            )
        if self.non_numeric_fields:
            fragments.append(
                self._build_field_query(self.non_numeric_fields, "nonNumericFields", "last", range_expr, suffix_non_numeric)
            )
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
