"""Integration test: verify downsampling query logic WITHOUT waiting for the task scheduler.

Strategy: seed raw data, run the manager to create tasks, then manually execute
the generated Flux query body against a concrete time range.  This proves the
query logic produces correct aggregated output in < 1 second — no scheduler wait.
"""

import datetime

import pytest
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

from manager import ChainedQueryGenerator, DownsampleManager, SourceQueryGenerator
from manager.model import DownsampleConfiguration, FieldData

from .conftest import INFLUX_BUCKET, INFLUX_ORG, INFLUX_TOKEN, make_influx_client

pytestmark = pytest.mark.integration


def _seed_known_data(url: str) -> tuple[str, str]:
    """Write points with known values so we can assert exact aggregation results.

    Returns (start_iso, stop_iso) covering the seeded time range.
    """
    with make_influx_client(url) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        # Align to the start of the current hour so all points fall in one aggregateWindow(every: 1h)
        base = datetime.datetime.now(tz=datetime.UTC).replace(microsecond=0, second=0, minute=0)

        # 6 points across 1 hour — mean should be (10+20+30+40+50+60)/6 = 35.0
        points = []
        for i, val in enumerate([10.0, 20.0, 30.0, 40.0, 50.0, 60.0]):
            ts = base + datetime.timedelta(minutes=i * 10)
            points.append(
                Point("sensor").field("temperature", val).field("status", "ok").tag("location", "lab").time(ts)
            )
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)

    start = base.isoformat().replace("+00:00", "Z")
    stop = (base + datetime.timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    return start, stop


TARGET_BUCKET = f"{INFLUX_BUCKET}_1h"

CONFIG_1H: dict[str, DownsampleConfiguration] = {
    "1h": DownsampleConfiguration(
        interval="1h",
        every="1h",
        offset="10s",
        expires="7d",
        bucket_shard_group_interval="1d",
    ),
}


class TestSourceQueryDownsampling:
    """Execute the SourceQueryGenerator Flux manually and verify aggregated output."""

    def test_numeric_mean_aggregation(self, influx_url):
        start, stop = _seed_known_data(influx_url)

        with _make_manager(influx_url) as mgr:
            mgr.run()

        fields = {
            "temperature": FieldData(data_type="float", numeric=True),
            "status": FieldData(data_type="string", numeric=False),
        }
        gen = SourceQueryGenerator(
            source_bucket=INFLUX_BUCKET,
            target_bucket=TARGET_BUCKET,
            downsample_config=CONFIG_1H["1h"],
            measurement="sensor",
            fields=fields,
        )
        query = gen.generate_query(start, stop)

        with make_influx_client(influx_url) as client:
            tables = client.query_api().query(query, org=INFLUX_ORG)

        numeric_records = []
        for table in tables:
            for record in table.records:
                if record.get_field() == "temperature":
                    numeric_records.append(record)

        assert len(numeric_records) >= 1, "Expected at least one aggregated temperature record"
        assert numeric_records[0].get_value() == pytest.approx(35.0)

    def test_non_numeric_last_aggregation(self, influx_url):
        start, stop = _seed_known_data(influx_url)

        with _make_manager(influx_url) as mgr:
            mgr.run()

        fields = {
            "temperature": FieldData(data_type="float", numeric=True),
            "status": FieldData(data_type="string", numeric=False),
        }
        gen = SourceQueryGenerator(
            source_bucket=INFLUX_BUCKET,
            target_bucket=TARGET_BUCKET,
            downsample_config=CONFIG_1H["1h"],
            measurement="sensor",
            fields=fields,
        )
        query = gen.generate_query(start, stop)

        with make_influx_client(influx_url) as client:
            tables = client.query_api().query(query, org=INFLUX_ORG)

        status_records = []
        for table in tables:
            for record in table.records:
                if record.get_field() == "status":
                    status_records.append(record)

        assert len(status_records) >= 1
        assert status_records[0].get_value() == "ok"


class TestChainedQueryDownsampling:
    """Verify chained query reads from the pre-aggregated bucket."""

    def test_chained_reads_from_upstream(self, influx_url):
        start, stop = _seed_known_data(influx_url)

        with _make_manager(influx_url) as mgr:
            mgr.run()

        fields = {
            "temperature": FieldData(data_type="float", numeric=True),
        }

        # Write a known aggregated point into the 1h bucket (simulating the task output)
        base = datetime.datetime.now(tz=datetime.UTC).replace(microsecond=0, second=0, minute=0)
        with make_influx_client(influx_url) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)
            point = Point("sensor").field("temperature", 35.0).tag("location", "lab").time(base)
            write_api.write(bucket=TARGET_BUCKET, org=INFLUX_ORG, record=point)

        # Ensure the 6h bucket exists
        chained_target = f"{INFLUX_BUCKET}_6h"
        config_6h = DownsampleConfiguration(
            interval="6h",
            every="6h",
            offset="10s",
            expires="30d",
            bucket_shard_group_interval="1d",
        )
        with make_influx_client(influx_url) as client:
            existing = client.buckets_api().find_bucket_by_name(chained_target)
            if existing is None:
                from influxdb_client.domain.bucket import Bucket

                org = client.organizations_api().find_organizations(org=INFLUX_ORG)[0]
                client.buckets_api().create_bucket(Bucket(name=chained_target, retention_rules=[], org_id=org.id))

        chained_gen = ChainedQueryGenerator(
            source_bucket=TARGET_BUCKET,
            target_bucket=chained_target,
            downsample_config=config_6h,
            measurement="sensor",
            fields=fields,
        )
        query = chained_gen.generate_query(start, stop)

        with make_influx_client(influx_url) as client:
            tables = client.query_api().query(query, org=INFLUX_ORG)

        records = []
        for table in tables:
            for record in table.records:
                if record.get_field() == "temperature":
                    records.append(record)

        assert len(records) >= 1
        assert records[0].get_value() == pytest.approx(35.0)


def _make_manager(url: str) -> DownsampleManager:
    return DownsampleManager(
        org=INFLUX_ORG,
        token=INFLUX_TOKEN,
        buckets=[INFLUX_BUCKET],
        bucket_configs=CONFIG_1H,
        url=url,
        metric_detection_duration="1d",
    )
