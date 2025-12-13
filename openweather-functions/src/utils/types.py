from polars import DataType
from typing import TypedDict, Literal, Any


class Location(TypedDict):
    search_name: str
    name: str
    country_code: str
    lat: str
    lon: str


class EndpointConfig(TypedDict):
    url: str
    extra_params: dict[str, Any]


type AvailableEndpoints = Literal["weather", "air_pollution"]


type NestedKeyPath = list[str]

type DictRow = dict[str, Any]

type Batch = list[DictRow]

type ColumnName = str
type ColumnType = DataType


class ColumnDefinition(TypedDict):
    name: ColumnName
    type: ColumnType
