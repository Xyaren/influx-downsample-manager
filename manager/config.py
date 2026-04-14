"""Configuration loading and parsing."""

import os

import yaml

from .model import DownsampleConfiguration, MeasurementConfig


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


def _parse_measurements(raw: dict | None) -> dict[str, MeasurementConfig]:
    """Parse a measurement-keyed config block into typed configs."""
    if not raw:
        return {}
    result: dict[str, MeasurementConfig] = {}
    for measurement, config_def in raw.items():
        cfg = MeasurementConfig()
        if "include" in config_def:
            cfg["include"] = bool(config_def["include"])
        if "include_fields" in config_def:
            cfg["include_fields"] = list(config_def["include_fields"])
        if "exclude_fields" in config_def:
            cfg["exclude_fields"] = list(config_def["exclude_fields"])
        result[measurement] = cfg
    return result


def parse_source_buckets(
    raw: list[str | dict],
) -> tuple[list[str], dict[str, dict[str, MeasurementConfig]]]:
    """Parse the ``source_buckets`` config section.

    Accepts both plain strings (backward-compatible) and objects with
    ``name`` and optional ``measurements``::

        source_buckets:
          - "teamspeak"                       # plain string — all measurements
          - name: "telegraf/autogen"           # object form
            measurements:
              procstat:
                include_fields: ["cpu_*", "memory_*"]
              kernel:
                include: false                # skip entirely

    Returns a tuple of (bucket_names, measurement_configs_by_bucket).
    """
    bucket_names: list[str] = []
    measurement_configs: dict[str, dict[str, MeasurementConfig]] = {}

    for entry in raw:
        if isinstance(entry, str):
            bucket_names.append(entry)
        else:
            name = entry["name"]
            bucket_names.append(name)
            configs = _parse_measurements(entry.get("measurements"))
            if configs:
                measurement_configs[name] = configs

    return bucket_names, measurement_configs
