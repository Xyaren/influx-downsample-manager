"""Shared fixtures for integration tests against a real InfluxDB 2.x instance."""

import logging
import time

import pytest
import urllib3
from influxdb_client import InfluxDBClient
from testcontainers.influxdb2 import InfluxDb2Container

logger = logging.getLogger(__name__)

INFLUX_ORG = "test-org"
INFLUX_BUCKET = "raw"
INFLUX_TOKEN = "integration-test-token"
INFLUX_USER = "admin"
INFLUX_PASS = "password12345678"
# Timeout as (connect_ms, read_ms) — separate budgets so shard creation doesn't eat the read timeout
INFLUX_TIMEOUT = (500, 5_000)


def make_influx_client(url: str) -> InfluxDBClient:
    """Create an InfluxDBClient with a generous timeout for integration tests."""
    logger.debug("Creating InfluxDB client -> %s (timeout=%s)", url, INFLUX_TIMEOUT)
    return InfluxDBClient(url=url, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=INFLUX_TIMEOUT)


def _wait_for_influxdb(url: str, retries: int = 15, delay: float = 2.0):
    """Poll the InfluxDB /health endpoint until it responds or retries are exhausted."""
    http = urllib3.PoolManager()
    for attempt in range(1, retries + 1):
        try:
            resp = http.request("GET", f"{url}/health", timeout=3.0)
            if resp.status == 200:
                logger.info("InfluxDB healthy after attempt %d/%d", attempt, retries)
                return
            logger.warning("Health check attempt %d/%d returned status %d", attempt, retries, resp.status)
        except Exception as exc:
            logger.warning("Health check attempt %d/%d failed: %s", attempt, retries, exc)
        time.sleep(delay)
    raise RuntimeError(f"InfluxDB at {url} did not become healthy after {retries * delay}s")


@pytest.fixture(scope="session")
def influxdb_container():
    """Start a real InfluxDB 2.7 container once for the entire test session."""
    logger.info("Starting InfluxDB 2.7 container...")
    container = InfluxDb2Container(
        image="influxdb:2.7",
        init_mode="setup",
        username=INFLUX_USER,
        password=INFLUX_PASS,
        org_name=INFLUX_ORG,
        bucket=INFLUX_BUCKET,
        admin_token=INFLUX_TOKEN,
    )
    container.start()
    url = container.get_url()
    logger.info("Container started, exposed at %s", url)
    logger.info("Waiting for InfluxDB to become healthy...")
    _wait_for_influxdb(url)
    logger.info("InfluxDB is ready")
    yield container
    logger.info("Stopping InfluxDB container...")
    container.stop()
    logger.info("Container stopped")


@pytest.fixture(scope="session")
def influx_url(influxdb_container):
    url = influxdb_container.get_url()
    logger.info("Using InfluxDB URL: %s", url)
    return url
