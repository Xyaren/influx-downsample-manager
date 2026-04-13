"""Allow running the package directly: python -m manager"""

import logging
import os
import sys

import coloredlogs
import yaml

from .downsample_manager import DownsampleManager
from .model import DownsampleConfiguration

coloredlogs.install(stream=sys.stdout)

logger = logging.getLogger(__name__)


def load_config(path: str = "config.yaml") -> dict:
    config_path = os.environ.get("CONFIG_PATH", path)
    with open(config_path) as f:
        return yaml.safe_load(f)


def build_bucket_configs(raw: dict[str, dict]) -> dict[str, DownsampleConfiguration]:
    configs: dict[str, DownsampleConfiguration] = {}
    for suffix, entry in raw.items():
        cfg = DownsampleConfiguration(
            interval=entry["interval"],
            every=entry["every"],
            offset=entry["offset"],
        )
        if "max_offset" in entry:
            cfg["max_offset"] = entry["max_offset"]
        if "expires" in entry:
            cfg["expires"] = entry["expires"]
        if "bucket_shard_group_interval" in entry:
            cfg["bucket_shard_group_interval"] = entry["bucket_shard_group_interval"]
        if "chained" in entry:
            cfg["chained"] = bool(entry["chained"])
        configs[suffix] = cfg
    return configs


def main() -> None:
    config = load_config()

    influx_cfg = config["influxdb"]
    token = os.environ.get("INFLUXDB_TOKEN", influx_cfg.get("token", ""))
    if not token:
        logger.error("No InfluxDB token provided. Set INFLUXDB_TOKEN env var or token in config.yaml")
        sys.exit(1)

    bucket_configs = build_bucket_configs(config["downsample_configs"])

    with DownsampleManager(
        org=influx_cfg["org"],
        token=token,
        buckets=config["source_buckets"],
        bucket_configs=bucket_configs,
        url=influx_cfg["url"],
        metric_detection_duration=config.get("metric_detection_duration", "1d"),
    ) as mgr:
        mgr.run()

    logger.info("Done")


if __name__ == "__main__":
    main()
