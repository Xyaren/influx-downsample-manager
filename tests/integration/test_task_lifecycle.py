"""Integration tests: task creation, update, and deletion against a real InfluxDB."""

import datetime

import pytest
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

from manager import DownsampleManager
from manager.model import DownsampleConfiguration

from .conftest import INFLUX_BUCKET, INFLUX_ORG, INFLUX_TOKEN, make_influx_client

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_manager(url: str, bucket_configs: dict[str, DownsampleConfiguration]) -> DownsampleManager:
    return DownsampleManager(
        org=INFLUX_ORG,
        token=INFLUX_TOKEN,
        buckets=[INFLUX_BUCKET],
        bucket_configs=bucket_configs,
        url=url,
        metric_detection_duration="1d",
    )


def _seed_data(url: str, measurement: str = "cpu", field: str = "usage", n_points: int = 5):
    """Write a handful of points so the manager can discover measurements."""
    with make_influx_client(url) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        now = datetime.datetime.now(tz=datetime.UTC)
        points = []
        for i in range(n_points):
            ts = now - datetime.timedelta(minutes=i * 10)
            points.append(Point(measurement).field(field, float(50 + i)).tag("host", "srv1").time(ts))
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)


SINGLE_CONFIG: dict[str, DownsampleConfiguration] = {
    "1h": DownsampleConfiguration(
        interval="1h",
        every="1h",
        offset="10s",
        expires="7d",
        bucket_shard_group_interval="1d",
    ),
}


# ---------------------------------------------------------------------------
# Task creation
# ---------------------------------------------------------------------------


class TestTaskCreation:
    def test_creates_target_bucket_and_task(self, influx_url):
        """First run should create the target bucket and at least one task."""
        _seed_data(influx_url)

        with _make_manager(influx_url, SINGLE_CONFIG) as mgr:
            mgr.run()

        with make_influx_client(influx_url) as client:
            bucket = client.buckets_api().find_bucket_by_name(f"{INFLUX_BUCKET}_1h")
            assert bucket is not None, "Target bucket was not created"

            tasks = client.tasks_api().find_tasks()
            task_names = [t.name for t in tasks]
            assert any("cpu" in n for n in task_names), f"No cpu task found in {task_names}"

    def test_idempotent_rerun(self, influx_url):
        """Running twice with the same config should not duplicate tasks."""
        _seed_data(influx_url)

        with _make_manager(influx_url, SINGLE_CONFIG) as mgr:
            mgr.run()
            mgr.run()

        with make_influx_client(influx_url) as client:
            tasks = client.tasks_api().find_tasks()
            cpu_tasks = [t for t in tasks if "cpu" in t.name]
            assert len(cpu_tasks) == 1, f"Expected 1 cpu task, got {len(cpu_tasks)}"


# ---------------------------------------------------------------------------
# Task update
# ---------------------------------------------------------------------------


class TestTaskUpdate:
    def test_updates_task_when_config_changes(self, influx_url):
        """Changing the interval should update the existing task's Flux query."""
        _seed_data(influx_url)

        # First run with 1h interval
        with _make_manager(influx_url, SINGLE_CONFIG) as mgr:
            mgr.run()

        with make_influx_client(influx_url) as client:
            tasks_before = client.tasks_api().find_tasks()
            cpu_task_before = next(t for t in tasks_before if "cpu" in t.name)
            flux_before = cpu_task_before.flux

        # Second run with a different every (triggers a flux change)
        updated_config: dict[str, DownsampleConfiguration] = {
            "1h": DownsampleConfiguration(
                interval="1h",
                every="2h",
                offset="10s",
                expires="7d",
                bucket_shard_group_interval="1d",
            ),
        }
        with _make_manager(influx_url, updated_config) as mgr:
            mgr.run()

        with make_influx_client(influx_url) as client:
            tasks_after = client.tasks_api().find_tasks()
            cpu_task_after = next(t for t in tasks_after if "cpu" in t.name)

        # Same task id, different flux
        assert cpu_task_after.id == cpu_task_before.id
        assert cpu_task_after.flux != flux_before


# ---------------------------------------------------------------------------
# Task deletion (cleanup of orphaned tasks)
# ---------------------------------------------------------------------------


class TestTaskDeletion:
    def test_removes_orphaned_tasks_when_measurement_disappears(self, influx_url):
        """If a measurement no longer exists, its task should be cleaned up."""
        # Seed two measurements
        _seed_data(influx_url, measurement="cpu")
        _seed_data(influx_url, measurement="mem", field="used")

        with _make_manager(influx_url, SINGLE_CONFIG) as mgr:
            mgr.run()

        with make_influx_client(influx_url) as client:
            tasks = client.tasks_api().find_tasks()
            assert any("mem" in t.name for t in tasks), "mem task should exist"

        # Delete all data from the 'mem' measurement so it won't be discovered
        with make_influx_client(influx_url) as client:
            delete_api = client.delete_api()
            delete_api.delete(
                start=datetime.datetime(2000, 1, 1, tzinfo=datetime.UTC),
                stop=datetime.datetime(2099, 1, 1, tzinfo=datetime.UTC),
                predicate='_measurement="mem"',
                bucket=INFLUX_BUCKET,
                org=INFLUX_ORG,
            )

        # Re-run — manager should clean up the orphaned mem task
        with _make_manager(influx_url, SINGLE_CONFIG) as mgr:
            mgr.run()

        with make_influx_client(influx_url) as client:
            tasks = client.tasks_api().find_tasks()
            mem_tasks = [t for t in tasks if "mem" in t.name]
            assert len(mem_tasks) == 0, f"Orphaned mem task(s) not cleaned up: {[t.name for t in mem_tasks]}"
