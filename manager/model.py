from dataclasses import dataclass
from typing_extensions import TypedDict, NotRequired


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


Mapping = dict[str, dict[str, FieldData]]
