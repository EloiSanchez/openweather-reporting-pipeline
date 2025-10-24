from typing import TypedDict, Literal, Any


class Location(TypedDict):
    id: int
    name: str
    lat: int
    lon: int


class EndpointConfig(TypedDict):
    url: str
    extra_params: dict[str, Any]


type AvailableEndpoints = Literal["weather", "air_pollution"]


type NestedKeyPath = list[str]

type DictRow = dict[str, Any]

type Batch = list[DictRow]
