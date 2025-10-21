import datetime
import functools
from typing import Any, Literal, TypedDict


class Location(TypedDict):
    id: int
    name: str
    lat: int
    lon: int


class EndpointConfig(TypedDict):
    url: str
    extra_params: dict[str, Any]


type AvailableEndpoints = Literal["weather", "air_pollution"]

type Batch = list[dict[str, Any]]


@functools.total_ordering
class Timestamp:

    def __init__(
        self,
        value: str | float | int | datetime.datetime | datetime.date,
        date_format: str = "%Y-%m-%d",
        time_format: str = "%H:%M:%S",
    ) -> None:
        self.format = f"{date_format} {time_format}"
        self.date_format = date_format
        self.time_format = time_format
        if isinstance(value, str):
            self.datetime = datetime.datetime.strptime(value, self.format)
        elif isinstance(value, int):
            self.datetime = datetime.datetime.fromtimestamp(value)
        elif isinstance(value, datetime.datetime):
            self.datetime = value
        elif isinstance(value, datetime.date):
            self.datetime = datetime.datetime(
                value.year, value.month, value.day, 0, 0, 0
            )
        else:
            raise ValueError(
                f"Cannot parse {value} of type {type(value)} as Timestamp."
            )

    @property
    def unix(self) -> int:
        return int(self.datetime.timestamp())

    @property
    def value(self) -> str:
        return self.datetime.strftime(self.format)

    @property
    def date(self) -> str:
        return self.datetime.date().strftime(self.date_format)

    def __hash__(self) -> int:
        return hash(self.datetime)

    def __lt__(self, other: "Timestamp"):
        return self.datetime.__lt__(other.datetime)

    def __repr__(self) -> str:
        return self.datetime.isoformat(sep=" ")

    def __add__(self, other: datetime.timedelta):
        return Timestamp(self.datetime + other)

    def get_as_start(self):
        return Timestamp(
            datetime.datetime(
                self.datetime.year, self.datetime.month, self.datetime.day, 0, 0, 0
            )
        )

    def get_as_end(self):
        return Timestamp(
            datetime.datetime(
                self.datetime.year, self.datetime.month, self.datetime.day, 23, 59, 59
            )
        )
