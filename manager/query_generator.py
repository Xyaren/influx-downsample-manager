import datetime
import json

from pytimeparse.timeparse import timeparse

from .model import FieldData, DownsampleConfiguration
from .utils import hash_to_decimal, timedelta_to_flux_duration, hash_to_integer


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

    def task_name(self):
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

    def generate_task(self):
        imports = (
            f'import "influxdata/influxdb/tasks"\n'
            f'import "date"\n'
            f'\n'
        )
        prep = (
            f"start = date.truncate(t: tasks.lastSuccess(orTime: -task.every), unit: 1m)\n"
            # f"shift_by = duration(v: (int(v: task.every) / 2))\n"
            "\n"
        )
        flux_queries: list[str] = []
        if len(self.numeric_fields) > 0:
            flux_query = (
                f"numericFields = {json.dumps(self.numeric_fields)}\n"
                f"from(bucket:\"{self.source_bucket}\")\n"
                f"  |> range(start: start)\n"
                f'  |> filter(fn: (r) => r._measurement == "{self.measurement}")\n'
                f'  |> filter(fn: (r) => contains(value: r._field, set: numericFields))\n'
                f'  |> aggregateWindow(every: {self.interval}, fn: mean)\n'
                # f'  |> timeShift(duration: shift_by)\n'
                f'  |> to(bucket:"{self.target_bucket}")\n'
            )
            flux_queries.append(flux_query)

        if len(self.non_numeric_fields) > 0:
            flux_query = (
                f"nonNumericFields = {json.dumps(self.non_numeric_fields)}\n"
                f"from(bucket:\"{self.source_bucket}\")\n"
                f"  |> range(start: start)\n"
                f'  |> filter(fn: (r) => r._measurement == "{self.measurement}")\n'
                f'  |> filter(fn: (r) => contains(value: r._field, set: nonNumericFields))\n'
                f'  |> aggregateWindow(every: {self.interval}, fn: last)\n'
                # f'  |> timeShift(duration: shift_by)\n'
                f'  |> to(bucket:"{self.target_bucket}")\n'
            )
            flux_queries.append(flux_query)

        # Define the task properties and create the task
        offset_as_influx_duration = timedelta_to_flux_duration(self.offset_with_predictable_factor())
        task_def = f'option task = {{name: "{self.task_name()}", every: {self.every}, offset: {offset_as_influx_duration}}}\n\n'
        return imports + task_def + prep + "\n\n".join(flux_queries)

    def generate_query(self, start: str, stop: str):
        imports = (
            f'start = time(v:\"{start}\")\n'
            f'stop = time(v:\"{stop}\")\n'
            # f"shift_by = duration(v: (int(v: {self.interval}) / 2))\n"
            f'\n'
        )
        flux_queries: list[str] = []
        if len(self.numeric_fields) > 0:
            flux_query = (
                f"numericFields = {json.dumps(self.numeric_fields)}\n"
                f"from(bucket:\"{self.source_bucket}\")\n"
                f"  |> range(start: start, stop: stop)\n"
                f'  |> filter(fn: (r) => r._measurement == "{self.measurement}")\n'
                f'  |> filter(fn: (r) => contains(value: r._field, set: numericFields))\n'
                f'  |> aggregateWindow(every: {self.interval}, fn: mean)\n'
                # f'  |> timeShift(duration: shift_by)\n'
                f'  |> to(bucket:"{self.target_bucket}")\n'
                f'  |> limit(n:1)\n'
                f'  |> yield(name: "numeric")\n'
            )
            flux_queries.append(flux_query)

        if len(self.non_numeric_fields) > 0:
            flux_query = (
                f"nonNumericFields = {json.dumps(self.non_numeric_fields)}\n"
                f"from(bucket:\"{self.source_bucket}\")\n"
                f"  |> range(start: start, stop: stop)\n"
                f'  |> filter(fn: (r) => r._measurement == "{self.measurement}")\n'
                f'  |> filter(fn: (r) => contains(value: r._field, set: nonNumericFields))\n'
                f'  |> aggregateWindow(every: {self.interval}, fn: last)\n'
                # f'  |> timeShift(duration: shift_by)\n'
                f'  |> to(bucket:"{self.target_bucket}")\n'
                f'  |> limit(n:1)\n'
                f'  |> yield(name: "non_numeric")\n'
            )
            flux_queries.append(flux_query)

        # Define the task properties and create the task
        return imports + "\n\n".join(flux_queries)

    def __str__(self):
        return f"{self.source_bucket} -> {self.target_bucket}: {self.measurement}"
