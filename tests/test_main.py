from unittest.mock import mock_open, patch

from manager.__main__ import build_bucket_configs, load_config

SAMPLE_YAML = """
influxdb:
  org: "my-org"
  url: "http://localhost:8086"
  token: "my-token"
source_buckets:
  - "telegraf"
metric_detection_duration: "2d"
downsample_configs:
  "1w":
    interval: "1m"
    every: "15m"
    offset: "30s"
    max_offset: "13m"
    expires: "1w"
    bucket_shard_group_interval: "1d"
  "31d":
    interval: "10m"
    every: "1h"
    offset: "1m"
    expires: "31d"
    bucket_shard_group_interval: "3d"
    chained: true
"""


class TestLoadConfig:
    @patch("builtins.open", mock_open(read_data=SAMPLE_YAML))
    def test_loads_yaml(self):
        config = load_config("config.yaml")
        assert config["influxdb"]["org"] == "my-org"
        assert config["source_buckets"] == ["telegraf"]

    @patch.dict("os.environ", {"CONFIG_PATH": "/custom/path.yaml"})
    @patch("builtins.open", mock_open(read_data=SAMPLE_YAML))
    def test_respects_env_var(self):
        config = load_config()
        assert "influxdb" in config


class TestBuildBucketConfigs:
    def test_required_fields(self):
        configs = build_bucket_configs({"1w": {"interval": "1m", "every": "15m", "offset": "30s"}})
        assert configs["1w"]["interval"] == "1m"
        assert configs["1w"]["every"] == "15m"
        assert configs["1w"]["offset"] == "30s"

    def test_optional_fields(self):
        raw = {
            "31d": {
                "interval": "10m",
                "every": "1h",
                "offset": "1m",
                "max_offset": "55m",
                "expires": "31d",
                "bucket_shard_group_interval": "3d",
                "chained": True,
            },
        }
        cfg = build_bucket_configs(raw)["31d"]
        assert cfg["max_offset"] == "55m"
        assert cfg["expires"] == "31d"
        assert cfg["chained"] is True

    def test_missing_optional_fields(self):
        cfg = build_bucket_configs({"inf": {"interval": "1h", "every": "1d", "offset": "5m"}})["inf"]
        assert "max_offset" not in cfg
        assert "expires" not in cfg
        assert "chained" not in cfg
