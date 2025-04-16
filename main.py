import logging
import sys
from typing import Dict

import coloredlogs as coloredlogs

from manager import DownsampleManager, DownsampleConfiguration

coloredlogs.install(stream=sys.stdout)  # install a handler on the root logger

logger = logging.getLogger(__name__)  # get a specific logger object

if __name__ == '__main__':
    buckets = ["telegraf/autogen", "teamspeak", "crowdsec", "api"]

    # Define the bucket and downsampling configurations for each time range and resolution
    bucket_configs: dict[str, DownsampleConfiguration] = {
        "1w": DownsampleConfiguration(bucket_shard_group_interval="1d", interval="1m", every="15m", offset="30s", max_offset="5m", expires="1w"),
        "31d": DownsampleConfiguration(bucket_shard_group_interval="3d", interval="10m", every="1h", offset="1m", max_offset="30m", expires="31d"),
        "inf": DownsampleConfiguration(bucket_shard_group_interval="30d", interval="1h", every="1d", offset="5m", max_offset="1h")
    }

    # Define the InfluxDB client
    manager = DownsampleManager(
        "example.com",
        "REDACTED_TOKEN==",
        buckets,
        bucket_configs,
        "https://influxdb.example.com",
        "1d"
    )

    manager.run()

    logger.info("Done")
