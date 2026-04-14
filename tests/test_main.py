from unittest.mock import mock_open, patch

from manager.config import build_bucket_configs, load_config, parse_source_buckets

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


class TestParseSourceBuckets:
    def test_plain_strings(self):
        names, configs = parse_source_buckets(["telegraf", "teamspeak"])
        assert names == ["telegraf", "teamspeak"]
        assert configs == {}

    def test_object_with_include_fields(self):
        raw = [
            {
                "name": "telegraf/autogen",
                "measurements": {
                    "procstat": {"include_fields": ["cpu_*", "memory_*"]},
                },
            },
        ]
        names, configs = parse_source_buckets(raw)
        assert names == ["telegraf/autogen"]
        assert configs["telegraf/autogen"]["procstat"]["include_fields"] == ["cpu_*", "memory_*"]

    def test_object_with_include_false(self):
        raw = [
            {
                "name": "telegraf/autogen",
                "measurements": {
                    "kernel": {"include": False},
                },
            },
        ]
        _, configs = parse_source_buckets(raw)
        assert configs["telegraf/autogen"]["kernel"]["include"] is False

    def test_object_without_measurements(self):
        raw = [{"name": "mybucket"}]
        names, configs = parse_source_buckets(raw)
        assert names == ["mybucket"]
        assert configs == {}

    def test_mixed_entries(self):
        raw = [
            "simple_bucket",
            {
                "name": "telegraf/autogen",
                "measurements": {
                    "procstat": {"include_fields": ["cpu_*"]},
                    "disk": {"exclude_fields": ["inodes_*"]},
                },
            },
            "another_bucket",
        ]
        names, configs = parse_source_buckets(raw)
        assert names == ["simple_bucket", "telegraf/autogen", "another_bucket"]
        assert "simple_bucket" not in configs
        assert "another_bucket" not in configs
        assert configs["telegraf/autogen"]["procstat"]["include_fields"] == ["cpu_*"]
        assert configs["telegraf/autogen"]["disk"]["exclude_fields"] == ["inodes_*"]

    def test_include_fields_and_exclude_fields(self):
        raw = [
            {
                "name": "b",
                "measurements": {
                    "m": {"include_fields": ["a_*"], "exclude_fields": ["a_bad"]},
                },
            },
        ]
        _, configs = parse_source_buckets(raw)
        assert configs["b"]["m"]["include_fields"] == ["a_*"]
        assert configs["b"]["m"]["exclude_fields"] == ["a_bad"]
