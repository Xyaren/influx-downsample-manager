import datetime
import logging
from collections.abc import Sequence
from types import TracebackType

from influxdb_client.client.flux_table import TableList
from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.domain.bucket import Bucket
from influxdb_client.domain.bucket_retention_rules import BucketRetentionRules
from influxdb_client.domain.label import Label
from influxdb_client.domain.label_mapping import LabelMapping
from influxdb_client.domain.organization import Organization
from influxdb_client.domain.task import Task
from influxdb_client.domain.task_create_request import TaskCreateRequest
from influxdb_client.domain.task_update_request import TaskUpdateRequest
from influxdb_client.service.buckets_service import BucketsService
from influxdb_client.service.tasks_service import TasksService
from pytimeparse.timeparse import timeparse

from .model import DownsampleConfiguration, FieldData, LabelDef, Mapping
from .query_generator import BaseQueryGenerator, ChainedQueryGenerator, SourceQueryGenerator

logger = logging.getLogger(__name__)

LABEL_DOWNSAMPLING = LabelDef(name="Downsampling", description="Downsampling", color="#ffff00")


class DownsampleManager:
    def __init__(
        self,
        org: str,
        token: str,
        buckets: Sequence[str],
        bucket_configs: dict[str, DownsampleConfiguration],
        url: str,
        metric_detection_duration: str = "1d",
    ):
        self._metric_detection_duration = metric_detection_duration
        self._buckets = buckets
        self._bucket_configs = bucket_configs

        self._task_prefix = "gen_"

        self._client = InfluxDBClient(
            url=url,
            token=token,
            timeout=int(datetime.timedelta(minutes=1).total_seconds() * 1000),
        )
        self._organization: Organization = self._client.organizations_api().find_organizations(org=org)[0]

        self._buckets_service = BucketsService(self._client.api_client)
        self._tasks_service = TasksService(self._client.api_client)

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def create_or_get_label(self, label: LabelDef):
        existing_labels = self._client.labels_api().find_label_by_org(org_id=self._organization.id)
        matching_labels = [x for x in existing_labels if x.name == label.name]
        existing: Label | None = matching_labels[0] if len(matching_labels) == 1 else None

        if existing is None:
            return self._client.labels_api().create_label(
                name=label.name,
                org_id=self._organization.id,
                properties={
                    "color": label.color,
                    "description": label.description,
                    "creator": "influx-downsample-manager",
                },
            )
        else:
            if "creator" in existing.properties and existing.properties["creator"] == "influx-downsample-manager":
                existing.properties["color"] = label.color
                existing.properties["description"] = label.description
                existing.properties["creator"] = "influx-downsample-manager"
                self._client.labels_api().update_label(existing)
                return existing
            else:
                raise Exception("Label " + label.name + " exists, but is not managed by this script")

    def create_bucket(self, bucket_name: str, expires: str, bucket_shard_group_interval: str):
        rules = []
        if expires is not None:
            every_seconds = timeparse(expires)
            if every_seconds is None:
                raise Exception("Can't parse " + expires)

            shard_group_duration_seconds = None
            if bucket_shard_group_interval is not None:
                shard_group_duration_seconds = timeparse(bucket_shard_group_interval)
                if shard_group_duration_seconds is None:
                    raise Exception("Can't parse " + expires)
            rules = [
                BucketRetentionRules(
                    type="expire",
                    every_seconds=every_seconds,
                    shard_group_duration_seconds=shard_group_duration_seconds,
                )
            ]

        bucket: Bucket | None = self._client.buckets_api().find_bucket_by_name(bucket_name=bucket_name)

        if bucket is not None:
            bucket.retention_rules = rules
            return self._client.buckets_api().update_bucket(bucket)
        else:
            bucket = Bucket(name=bucket_name, retention_rules=rules, org_id=self._organization.id)
            return self._client.buckets_api().create_bucket(bucket)

    def add_label_to_bucket(self, bucket: Bucket, label: Label):
        labels: list[Label] = self._buckets_service.get_buckets_id_labels(bucket.id).labels
        if len([x for x in labels if x.id == label.id]) == 0:
            self._buckets_service.post_buckets_id_labels(bucket.id, LabelMapping(label.id))

    def add_label_to_task(self, task: Task, label: Label):
        labels: list[Label] = self._tasks_service.get_tasks_id_labels(task.id).labels
        if len([x for x in labels if x.id == label.id]) == 0:
            self._tasks_service.post_tasks_id_labels(task.id, LabelMapping(label.id))

    def get_measurements_and_fields(self, bucket: str) -> Mapping:
        flux = f"""
        import "types"

        from(bucket: "{bucket}")
            |> range(start: -{self._metric_detection_duration})
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
        result: TableList = self._client.query_api().query(query=flux, org=self._organization)
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

    def cleanup_labels(self, created_label_ids):
        labels: list[Label] = self._client.labels_api().find_label_by_org(self._organization.id)
        for label in labels:
            if (
                "creator" in label.properties
                and label.properties["creator"] == "influx-downsample-manager"
                and label.id not in created_label_ids
            ):
                self._client.labels_api().delete_label(label)
                logger.info("Deleted Label: %s", label.name)

    def cleanup_tasks(self, label_downsampling: Label, created_tasks: set[str]):
        tasks: list[Task] = list(self._client.tasks_api().find_tasks_iter(org_id=self._organization.id))
        for task in tasks:
            if (
                label_downsampling in task.labels
                and task.name.startswith(self._task_prefix)
                and task.id not in created_tasks
            ):
                self._client.tasks_api().delete_task(task_id=task.id)
                logger.info("Deleted Task: %s", task.name)

    @staticmethod
    def _sorted_bucket_configs(
        bucket_configs: dict[str, DownsampleConfiguration],
    ) -> list[tuple[str, DownsampleConfiguration]]:
        """Sort bucket configs by interval duration (ascending) so chaining reads from the finest granularity first."""

        def _interval_seconds(cfg: DownsampleConfiguration) -> int:
            parsed = timeparse(cfg["interval"])
            if parsed is None:
                raise Exception(f"Can't parse interval: {cfg['interval']}")
            return parsed

        return sorted(bucket_configs.items(), key=lambda item: _interval_seconds(item[1]))

    def process(
        self, label_downsampling: Label, source_bucket: str, created_tasks: set[str], created_label_ids: set[str]
    ):
        source_label = self.create_or_get_label(
            LabelDef(name="Source: " + source_bucket, description="Source: " + source_bucket, color="#383e42")
        )
        created_label_ids.add(source_label.id)

        bucket_to_generators: dict[str, list[BaseQueryGenerator]] = {}
        # Sort configs by interval so chaining always reads from the next-finer tier
        sorted_configs = self._sorted_bucket_configs(self._bucket_configs)

        # Track the previous tier for chaining
        prev_bucket_name: str | None = None

        # Loop through each bucket configuration (finest interval first)
        for _i, (suffix, bucket_config) in enumerate(sorted_configs):
            downsample_bucket_name = source_bucket + "_" + suffix
            chained = bucket_config.get("chained", False)

            # Determine which bucket this tier reads from
            if chained and prev_bucket_name is not None:
                effective_source = prev_bucket_name
                logger.info(
                    "Chaining: %s reads from %s (instead of %s)",
                    downsample_bucket_name,
                    effective_source,
                    source_bucket,
                )
            else:
                effective_source = source_bucket

            target_bucket_label = self.create_or_get_label(
                LabelDef(
                    name="Bucket: " + downsample_bucket_name,
                    description="Bucket: " + downsample_bucket_name,
                    color="#ADFF2F",
                )
            )
            created_label_ids.add(target_bucket_label.id)

            interval = bucket_config["interval"]
            expires = bucket_config.get("expires")
            bucket = self.create_bucket(downsample_bucket_name, expires, bucket_config["bucket_shard_group_interval"])
            self.add_label_to_bucket(bucket, target_bucket_label)
            self.add_label_to_bucket(bucket, label_downsampling)
            self.add_label_to_bucket(bucket, source_label)

            interval_label = self.create_or_get_label(
                LabelDef(name="Interval: " + interval, description="Interval: " + interval, color="#800080")
            )
            created_label_ids.add(interval_label.id)
            self.add_label_to_bucket(bucket, interval_label)

            expiry_label = self.create_or_get_label(
                LabelDef(
                    name="Retention: " + (expires or "Infinity"),
                    description="Retention: " + (expires or "Infinity"),
                    color="#d22b2b",
                )
            )
            created_label_ids.add(expiry_label.id)
            self.add_label_to_bucket(bucket, expiry_label)

            # Detect measurements from the original source bucket (always has the full schema)
            mapping: Mapping = self.get_measurements_and_fields(source_bucket)

            labels = [source_label, target_bucket_label, interval_label, label_downsampling]
            generators = self.create_tasks(
                effective_source,
                downsample_bucket_name,
                bucket_config,
                mapping,
                created_tasks,
                created_label_ids,
                labels,
                chained=chained,
            )

            bucket_to_generators[downsample_bucket_name] = generators
            prev_bucket_name = downsample_bucket_name
        return bucket_to_generators

    def create_tasks(
        self,
        source_bucket,
        target_bucket,
        downsample_config: DownsampleConfiguration,
        mapping,
        created_tasks,
        created_label_ids,
        labels,
        chained: bool = False,
    ):
        generators: list[BaseQueryGenerator] = []
        # Loop through each measurement and add it to the Flux query with the appropriate aggregation function
        for measurement, fields in mapping.items():
            generator_cls = ChainedQueryGenerator if chained else SourceQueryGenerator
            generator = generator_cls(
                source_bucket, target_bucket, downsample_config, measurement, fields, self._task_prefix
            )
            generators.append(generator)

            task = self.create_or_update_tasks(created_tasks, generator.generate_task(), generator.task_name())
            for label in labels:
                self.add_label_to_task(task, label)

            measurement_label = self.create_or_get_label(
                LabelDef("Measurement: " + measurement, "Measurement: " + measurement, color="#008080")
            )
            created_label_ids.add(measurement_label.id)
            self.add_label_to_task(task, measurement_label)

        return generators

    def create_or_update_tasks(self, created_tasks: set[str], flux_query: str, task_name: str) -> Task:
        existing: list[Task] = self._client.tasks_api().find_tasks(org_id=self._organization.id, name=task_name)

        if len(existing) > 1:
            # Duplicates found — keep the first, delete the rest
            logger.warning("Found %d duplicate tasks named '%s', cleaning up", len(existing), task_name)
            for duplicate in existing[1:]:
                self._client.tasks_api().delete_task(task_id=duplicate.id)
                logger.info("Deleted duplicate Task: %s (%s)", duplicate.name, duplicate.id)

        if len(existing) >= 1:
            _existing_task = existing[0]

            if _existing_task.flux == flux_query:
                created_tasks.add(_existing_task.id)
                logger.info("Task already up to date: %s", _existing_task.name)
                return _existing_task

            updated_task = self._client.tasks_api().update_task_request(
                _existing_task.id, task_update_request=TaskUpdateRequest(flux=flux_query)
            )
            created_tasks.add(updated_task.id)
            logger.info("Updated Task: %s", updated_task.name)
            return updated_task
        else:
            create_task = self._client.tasks_api().create_task(
                task_create_request=TaskCreateRequest(org_id=self._organization.id, flux=flux_query)
            )
            created_tasks.add(create_task.id)
            logger.info("Created Task: %s", create_task.name)
            return create_task

    def post_import(self, bucket_to_generators: dict[str, dict[str, list[BaseQueryGenerator]]]):
        interval = datetime.timedelta(hours=2)
        empty_query_result_limit = datetime.timedelta(days=1) / interval

        latest_stop = datetime.datetime.now(tz=datetime.timezone.utc).replace(
            microsecond=0, second=0, minute=0
        ) + datetime.timedelta(hours=1)

        for source_bucket, collected_generators in bucket_to_generators.items():
            for target_bucket, generators in collected_generators.items():
                for generator in generators:
                    stop = latest_stop
                    empty_query_result_count = 0

                    earliest_start = None
                    if generator.expires is not None:
                        earliest_start = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(
                            seconds=timeparse(generator.expires)
                        )

                    while empty_query_result_count < empty_query_result_limit:
                        start = stop - interval
                        if earliest_start is not None and start < earliest_start:
                            logger.info("Finished")
                            break
                        logger.info(
                            "Query %s/%s/%s/%s-%s", source_bucket, target_bucket, generator.measurement, start, stop
                        )
                        query = generator.generate_query(start.isoformat(), stop.isoformat())
                        query_result = self._client.query_api().query(query=query, org=self._organization.id)
                        stop = stop - interval
                        if len(query_result) == 0:
                            empty_query_result_count = empty_query_result_count + 1

    def run(self):
        created_tasks = set()
        created_label_ids = set()
        label_downsampling = self.create_or_get_label(LABEL_DOWNSAMPLING)
        created_label_ids.add(label_downsampling.id)

        bucket_to_generators: dict[str, dict[str, list[BaseQueryGenerator]]] = {}

        for source_bucket in self._buckets:
            bucket_queries = self.process(label_downsampling, source_bucket, created_tasks, created_label_ids)
            bucket_to_generators[source_bucket] = bucket_queries

        self.cleanup_tasks(label_downsampling, created_tasks)
        self.cleanup_labels(created_label_ids)

        # self.post_import(bucket_to_generators)
        pass
