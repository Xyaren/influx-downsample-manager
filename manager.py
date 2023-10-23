import datetime
import json
import logging
import sys
from dataclasses import dataclass
from typing import Set

import coloredlogs as coloredlogs
from influxdb_client import Bucket, BucketRetentionRules, BucketsService, InfluxDBClient, Label, LabelMapping, \
    Organization, Task, \
    TaskCreateRequest, \
    TaskUpdateRequest, TasksService
from influxdb_client.client.flux_table import TableList
from pytimeparse.timeparse import timeparse

coloredlogs.install(stream=sys.stdout)  # install a handler on the root logger

logger = logging.getLogger(__name__)  # get a specific logger object

# Replace with your own credentials
org = "example.com"
token = "REDACTED_TOKEN=="

buckets = ["telegraf/autogen", "teamspeak"]
# buckets = ["teamspeak"]
# buckets = []
task_prefix = "gen_"
# Define the bucket and downsampling configurations for each time range and resolution
bucket_configs = {
    "1w": {
        "expires": "1w",
        "interval": "1m",
        "every": "15m",
        "offset": "30s"
    },
    "31d": {
        "expires": "31d",
        "interval": "10m",
        "every": "1h",
        "offset": "1m"
    },
    "inf": {
        "interval": "1h",
        "every": "1d",
        "offset": "5m"
    }
}

# Define the InfluxDB client
client = InfluxDBClient(url="http://example.com:8086", token=token)
organization: Organization = client.organizations_api().find_organizations(org=org)[0]


@dataclass(unsafe_hash=True)
class FieldData:
    data_type: str
    numeric: bool


@dataclass(unsafe_hash=True)
class LabelDef:
    name: str
    description: str
    color: str


LABEL_DOWNSAMPLING = LabelDef(name="Downsampling", description="Downsampling", color="#ffff00")

Mapping = dict[str, dict[str, FieldData]]

buckets_service = BucketsService(client.api_client)
tasks_service = TasksService(client.api_client)


def create_or_get_label(label: LabelDef):
    existing_labels = client.labels_api().find_label_by_org(org_id=organization.id)
    matching_labels = [x for x in existing_labels if x.name == label.name]
    existing: Label | None = matching_labels[0] if len(matching_labels) == 1 else None

    if existing is None:
        created_label = client.labels_api().create_label(name=label.name, org_id=organization.id, properties={
            "color": label.color,
            "description": label.description,
            "creator": "influx-downsample-manager"})
        return created_label
    else:
        if "creator" in existing.properties and existing.properties["creator"] == "influx-downsample-manager":
            existing.properties["color"] = label.color
            existing.properties["description"] = label.description
            existing.properties["creator"] = "influx-downsample-manager"
            client.labels_api().update_label(existing)
            return existing
        else:
            raise Exception("Label " + label.name + " exists, but is not managed by this script")


def create_bucket(bucket_name: str, expires: str):
    rules = []
    if expires is not None:
        every_seconds = timeparse(expires)
        if every_seconds is None:
            raise Exception("Can't parse " + expires)
        rules = [BucketRetentionRules(type='expire', every_seconds=every_seconds)]

    bucket: Bucket | None = client.buckets_api().find_bucket_by_name(bucket_name=bucket_name)

    if bucket is not None:
        bucket.retention_rules = rules
        return client.buckets_api().update_bucket(bucket)
    else:
        bucket = Bucket(name=bucket_name,
                        retention_rules=rules,
                        org_id=organization.id)
        return client.buckets_api().create_bucket(bucket)


def add_label_to_bucket(bucket: Bucket, label: Label):
    labels: list[Label] = buckets_service.get_buckets_id_labels(bucket.id).labels
    if len([x for x in labels if x.id == label.id]) == 0:
        buckets_service.post_buckets_id_labels(bucket.id, LabelMapping(label.id))


def add_label_to_task(task: Task, label: Label):
    labels: list[Label] = tasks_service.get_tasks_id_labels(task.id).labels
    if len([x for x in labels if x.id == label.id]) == 0:
        tasks_service.post_tasks_id_labels(task.id, LabelMapping(label.id))


def get_measurements_and_fields(bucket: str) -> Mapping:
    flux = f"""
    import "types"

    from(bucket: "{bucket}")
        |> range(start: -1h)
        |> keep(columns: ["_measurement","_field","_value"])
        |> group(columns: ["_measurement","_field"])
        |> last()
        |> map(fn: (r) => ({{
            r with
            type: if types.isType(v: r._value, type: "float") then "float"
                    else if types.isType(v: r._value, type: "int") then "int"
                    else if types.isType(v: r._value, type: "double") then "double"
                    else if types.isType(v: r._value, type: "uint") then "uint"
                    else if types.isType(v: r._value, type: "string") then "string"
                    else if types.isType(v: r._value, type: "bytes") then "bytes"
                    else if types.isType(v: r._value, type: "bool") then "bool"
                    else "unknown",
            numeric: if types.isNumeric(v: r._value) then true else false
        }}))
        |> drop(columns: ["_value"])
        |> group(columns: ["_measurement"])
    """
    result: TableList = client.query_api().query(query=flux, org=organization)
    results: dict[str, dict[str, FieldData]] = {}
    for table in result:
        measurement = table.records[0].get_measurement()
        fields = dict()
        for record in table.records:
            field = record.get_field()
            numeric = record["numeric"]
            data_type = record["type"]
            fields[field] = FieldData(data_type=data_type, numeric=numeric)
        results[measurement] = fields
    return results


def cleanup_labels(created_label_ids):
    labels: list[Label] = client.labels_api().find_label_by_org(organization.id)
    for label in labels:
        if "creator" in label.properties and label.properties["creator"] == "influx-downsample-manager":
            if label.id not in created_label_ids:
                client.labels_api().delete_label(label)
                logger.info("Deleted Label: %s", label.name)


def cleanup_tasks(created_tasks):
    tasks: list[Task] = client.tasks_api().find_tasks(org_id=organization.id)
    for task in tasks:
        if task.name.startswith(task_prefix) and task.id not in created_tasks:
            client.tasks_api().delete_task(task_id=task.id)
            logger.info("Deleted Task: %s", task.name)


def process(label_downsampling: Label, source_bucket: str, created_tasks: Set[str], created_label_ids: Set[str]):
    source_label = create_or_get_label(
        LabelDef(name="Source: " + source_bucket,
                 description="Source: " + source_bucket,
                 color="#383e42"))
    created_label_ids.add(source_label.id)

    bucket_to_generators: dict[str, list[QueryGenerator]] = {}

    # Loop through each bucket configuration and create a task for each downsampling configuration
    for suffix, bucket_config in bucket_configs.items():
        downsample_bucket_name = source_bucket + "_" + suffix

        target_bucket_label = create_or_get_label(
            LabelDef(name="Bucket: " + downsample_bucket_name,
                     description="Bucket: " + downsample_bucket_name,
                     color="#ADFF2F"))
        created_label_ids.add(target_bucket_label.id)

        interval = bucket_config['interval']
        every = bucket_config['every']
        offset = bucket_config['offset']

        expires = bucket_config['expires'] if 'expires' in bucket_config else None
        bucket = create_bucket(downsample_bucket_name, expires)
        add_label_to_bucket(bucket, target_bucket_label)
        add_label_to_bucket(bucket, label_downsampling)
        add_label_to_bucket(bucket, source_label)

        interval_label = create_or_get_label(
            LabelDef(name="Interval: " + interval, description="Interval: " + interval, color="#800080"))
        created_label_ids.add(interval_label.id)
        add_label_to_bucket(bucket, interval_label)

        expiry_label = create_or_get_label(
            LabelDef(name="Retention: " + (expires or "Infinity"),
                     description="Retention: " + (expires or "Infinity"),
                     color="#d22b2b"))
        created_label_ids.add(expiry_label.id)
        add_label_to_bucket(bucket, expiry_label)

        mapping: Mapping = get_measurements_and_fields(source_bucket)

        labels = [source_label, target_bucket_label, interval_label, source_label, label_downsampling]
        generators = create_tasks(source_bucket, downsample_bucket_name, interval, expires, every, offset, mapping,
                                  created_tasks,
                                  created_label_ids, labels)

        bucket_to_generators[downsample_bucket_name] = generators
    return bucket_to_generators


class QueryGenerator:
    def __init__(self,
                 source_bucket: str,
                 target_bucket: str,
                 interval: str,
                 expires: str,
                 every: str,
                 offset: str,
                 measurement: str,
                 fields: dict[str, FieldData]):
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket
        self.interval = interval
        self.expires = expires
        self.every = every
        self.offset = offset
        self.measurement = measurement
        self.fields = fields

        self.numeric_fields = [k for k, v in fields.items() if v.numeric]
        self.non_numeric_fields = [k for k, v in fields.items() if v.numeric is False]

    def task_name(self):
        return f"{task_prefix}{self.target_bucket}_{self.measurement}"

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
        task_def = f'option task = {{name: "{self.task_name()}", every: {self.every}, offset: {self.offset}}}\n\n'
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


def create_tasks(source_bucket, target_bucket, interval, expires, every, offset, mapping, created_tasks,
                 created_label_ids,
                 labels):
    generators = []
    # Loop through each measurement and add it to the Flux query with the appropriate aggregation function
    for measurement, fields in mapping.items():

        generator = QueryGenerator(source_bucket, target_bucket, interval, expires, every, offset, measurement, fields)
        generators.append(generator)

        task = create_or_update_tasks(created_tasks, generator.generate_task(), generator.task_name())
        for label in labels:
            add_label_to_task(task, label)

        measurement_label = create_or_get_label(
            LabelDef("Measurement: " + measurement, "Measurement: " + measurement, color="#008080"))
        created_label_ids.add(measurement_label.id)
        add_label_to_task(task, measurement_label)

    return generators


def create_or_update_tasks(created_tasks, flux_query, task_name):
    existing: list[Task] = client.tasks_api().find_tasks(org_id=organization.id, name=task_name)
    if len(existing) == 1:
        _existing_task = existing[0]

        if _existing_task.flux == flux_query:
            created_tasks.add(_existing_task.id)
            logger.info("Task already up to date: %s", _existing_task.name)
            return _existing_task

        updated_task = client.tasks_api().update_task_request(
            _existing_task.id, task_update_request=TaskUpdateRequest(flux=flux_query))
        created_tasks.add(updated_task.id)
        logger.info("Updated Task: %s", updated_task.name)
        return updated_task
    else:
        create_task = client.tasks_api().create_task(
            task_create_request=(TaskCreateRequest(org_id=organization.id, flux=flux_query)))
        created_tasks.add(create_task.id)
        logger.info("Created Task: %s", create_task.name)
        return create_task


def post_import(bucket_to_generators: dict[str, dict[str, list[QueryGenerator]]]):
    interval = datetime.timedelta(hours=2)
    empty_query_result_limit = datetime.timedelta(days=1) / interval

    latest_stop = datetime.datetime.now(tz=datetime.timezone.utc).replace(microsecond=0, second=0, minute=0) \
                  + datetime.timedelta(hours=1)

    for source_bucket, collected_generators in bucket_to_generators.items():
        for target_bucket, generators in collected_generators.items():
            for generator in generators:

                stop = latest_stop
                empty_query_result_count = 0

                earliest_start = None
                if generator.expires is not None:
                    earliest_start = datetime.datetime.now(tz=datetime.timezone.utc) - \
                                     datetime.timedelta(seconds=timeparse(generator.expires))

                while empty_query_result_count < empty_query_result_limit:
                    start = (stop - interval)
                    if earliest_start is not None and start < earliest_start:
                        logger.info("Finished")
                        break
                    logger.info("Query %s/%s/%s/%s-%s",
                                source_bucket, target_bucket, generator.measurement, start, stop)
                    query = generator.generate_query(start.isoformat(), stop.isoformat())
                    query_result = client.query_api().query(query=query, org=organization.id)
                    stop = stop - interval
                    if len(query_result) == 0:
                        empty_query_result_count = empty_query_result_count + 1


def main():
    created_tasks = set()
    created_label_ids = set()
    label_downsampling = create_or_get_label(LABEL_DOWNSAMPLING)
    created_label_ids.add(label_downsampling.id)

    bucket_to_generators: dict[str, dict[str, list[QueryGenerator]]] = {}

    for source_bucket in buckets:
        bucket_queries = process(label_downsampling, source_bucket, created_tasks, created_label_ids)
        bucket_to_generators[source_bucket] = bucket_queries

    cleanup_tasks(created_tasks)
    cleanup_labels(created_label_ids)

    # post_import(bucket_to_generators)

    logger.info("Done")


main()
