from collections import defaultdict
import json
import datetime
import requests
import os
from pathlib import Path
from typing import Any, Callable, Iterable, Literal, Self, TypedDict, Sequence


OPENWEATHER_SECRET = os.environ["OPENWEATHER_SECRET_KEY"]


class Location(TypedDict):
    id: int
    name: str
    lat: int
    lon: int


class EndpointConfig(TypedDict):
    url: str
    extra_params: dict[str, Any]


type AvailableEndpoints = Literal["weather", "air_pollution"]


class Timestamp:

    def __init__(
        self,
        value: str | float | int | datetime.datetime,
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


class OpenWeather:

    def __init__(self, secret: str | None) -> None:
        self.secret: str = (
            secret if secret else os.environ.get("OPENWEATHER_SECRET_KEY", "")
        )

        if not self.secret:
            raise ValueError(
                "Open Weather secret not passed nor found in "
                "OPENWEATHER_SECRET_KEY environment variable."
            )

        self.start_date: Timestamp
        self.end_date: Timestamp

        self.endpoints: Iterable[AvailableEndpoints] = ["weather", "air_pollution"]
        self.endpoint_config: dict[AvailableEndpoints, EndpointConfig] = {
            "weather": EndpointConfig(
                url="https://history.openweathermap.org/data/2.5/history/city",
                extra_params={"type": "hour"},
            ),
            "air_pollution": EndpointConfig(
                url="https://api.openweathermap.org/data/2.5/air_pollution/history",
                extra_params={},
            ),
        }
        self.raw_dir: Path

    @property
    def base_params(self) -> dict[str, Any]:
        return {
            "appid": self.secret,
            "start": self.start_date.unix,
            "end": self.end_date.unix,
        }

    def get_params(self, location: Location) -> dict[str, Any]:
        params = self.base_params.copy()
        params.update({"lat": location["lat"], "lon": location["lon"]})
        return params

    def set_date_range(
        self,
        start_date: Timestamp,
        end_date: Timestamp = Timestamp(
            (datetime.datetime.today() - datetime.timedelta(days=1)).replace(
                hour=23, minute=59
            )
        ),
    ) -> Self:
        self.start_date = start_date
        self.end_date = end_date

        return self

    def set_locations(self, locations: Iterable[Location]) -> Self:
        self.locations = locations
        return self

    def set_endpoints(
        self, endpoints: Iterable[AvailableEndpoints] | Literal["all"]
    ) -> Self:
        if endpoints == "all":
            self.endpoints = list(self.endpoint_config.keys())
        else:
            self.endpoints = endpoints
        return self

    def set_raw_dir_path(self, raw_dir_path: str) -> Self:
        self.raw_dir = Path(raw_dir_path)
        return self

    def fetch_endpoint(self, endpoint: AvailableEndpoints, location: Location):
        params = self.get_params(location)
        params.update(self.endpoint_config[endpoint]["extra_params"])

        response = requests.get(self.endpoint_config[endpoint]["url"], params=params)
        response.raise_for_status()

        data = response.json()["list"]

        self.save_raw_data(location, data, endpoint)

    def fetch(self):
        for location in self.locations:
            for endpoint in self.endpoints:
                self.fetch_endpoint(endpoint, location)

    def batch_raw_data(
        self, data: list[dict[str, Any]]
    ) -> dict[str, list[dict[str, Any]]]:
        batched_data = defaultdict(list)
        for row in data:
            dt = Timestamp(row["dt"]).date
            batched_data[dt].append(row)

        return batched_data

    def save_raw_data(
        self, location: Location, data: list[dict[str, Any]], endpoint_name: str
    ):
        for date, batch in self.batch_raw_data(data).items():
            out_dir = self.raw_dir / endpoint_name / date
            if not out_dir.exists():
                out_dir.mkdir(parents=True)
            out_file = out_dir / (location["name"] + ".json")

            with out_file.open("w") as f:
                json.dump(batch, f, indent=4)


def _get_locations(locations_path: str) -> list[Location]:
    with open(locations_path, "r") as f:
        locations = json.load(f)

    return [
        Location(
            id=location["city"]["id"]["$numberLong"],
            name=location["city"]["name"],
            lat=location["city"]["coord"]["lat"],
            lon=location["city"]["coord"]["lon"],
        )
        for location in locations
    ]


def ingest_openweather():
    locations = _get_locations("ingest/config/locations.json")
    start_date = Timestamp("2025-09-09 00:00:00")
    end_date = Timestamp("2025-09-11 23:59:59")

    open_weather = (
        OpenWeather(OPENWEATHER_SECRET)
        .set_date_range(start_date=start_date, end_date=end_date)
        .set_locations(locations)
        .set_endpoints("all")
    )

    open_weather.fetch()


if __name__ == "__main__":
    ingest_openweather()
