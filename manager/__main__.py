"""Allow running the package directly: python -m manager"""

import logging
import os
import sys

import coloredlogs

from .config import build_bucket_configs, load_config, parse_source_buckets
from .downsample_manager import DownsampleManager

coloredlogs.install(stream=sys.stdout)

logger = logging.getLogger(__name__)


def main() -> None:
    config = load_config()

    influx_cfg = config.get("influxdb") or {}
    token = os.environ.get("INFLUXDB_TOKEN", influx_cfg.get("token", ""))
    org = os.environ.get("INFLUXDB_ORG", influx_cfg.get("org", ""))
    url = os.environ.get("INFLUXDB_URL", influx_cfg.get("url", ""))

    if not token:
        logger.error("No InfluxDB token provided. Set INFLUXDB_TOKEN env var or token in config.yaml")
        sys.exit(1)
    if not org:
        logger.error("No InfluxDB org provided. Set INFLUXDB_ORG env var or org in config.yaml")
        sys.exit(1)
    if not url:
        logger.error("No InfluxDB URL provided. Set INFLUXDB_URL env var or url in config.yaml")
        sys.exit(1)

    bucket_configs = build_bucket_configs(config["downsample_configs"])
    bucket_names, measurement_configs = parse_source_buckets(config["source_buckets"])

    with DownsampleManager(
        org=org,
        token=token,
        buckets=bucket_names,
        bucket_configs=bucket_configs,
        url=url,
        metric_detection_duration=config.get("metric_detection_duration", "1d"),
        measurement_configs=measurement_configs,
    ) as mgr:
        mgr.run()

    logger.info("Done")


if __name__ == "__main__":
    main()
