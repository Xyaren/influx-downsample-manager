from dataclasses import dataclass
from typing import NotRequired, TypedDict


@dataclass(unsafe_hash=True)
class FieldData:
    data_type: str
    numeric: bool


@dataclass(unsafe_hash=True)
class LabelDef:
    name: str
    description: str
    color: str


class DownsampleConfiguration(TypedDict):
    interval: str
    every: str
    offset: str
    max_offset: NotRequired[str]
    expires: NotRequired[str]
    bucket_shard_group_interval: NotRequired[str]
    chained: NotRequired[bool]


class MeasurementConfig(TypedDict, total=False):
    include: bool
    include_fields: list[str]
    exclude_fields: list[str]


class SourceBucketConfig(TypedDict, total=False):
    name: str
    measurements: dict[str, MeasurementConfig]


Mapping = dict[str, dict[str, FieldData]]
