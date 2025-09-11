from collections import defaultdict
import json
import datetime
import requests
import os
from pathlib import Path
from typing import Any, Literal, TypedDict


OPENWEATHER_SECRET = os.environ["OPENWEATHER_SECRET_KEY"]


class Location(TypedDict):
    id: int
    name: str
    lat: int
    lon: int


class Timestamp:

    def __init__(
        self,
        value: str | float,
        parse_from: Literal["string", "unix"],
        date_format: str = "%Y-%m-%d",
        time_format: str = "%H:%M:%S",
    ) -> None:
        self.format = f"{date_format} {time_format}"
        self.date_format = date_format
        self.time_format = time_format
        if parse_from == "string" and isinstance(value, str):
            self.datetime = datetime.datetime.strptime(value, self.format)
        elif parse_from == "unix" and isinstance(value, int):
            self.datetime = datetime.datetime.fromtimestamp(value)

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


def batch_raw_data(data: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    batched_data = defaultdict(list)
    print(f"Batching {len(data)} elements")
    for row in data:
        dt = Timestamp(row["dt"], "unix").date
        batched_data[dt].append(row)

    return batched_data


def save_raw_data(location: Location, data: list[dict[str, Any]], name: str):
    raw_dir = Path("raw/")

    for date, batch in batch_raw_data(data).items():
        out_dir = raw_dir / name / date
        if not out_dir.exists():
            out_dir.mkdir(parents=True)
        out_file = out_dir / (location["name"] + ".json")

        with out_file.open("w") as f:
            json.dump(batch, f, indent=4)


def get_historical_weather(
    location: Location,
    start_datetime: Timestamp,
    end_datetime: Timestamp,
) -> list:

    params = {
        "appid": OPENWEATHER_SECRET,
        "start": start_datetime.unix,
        "end": end_datetime.unix,
        "type": "hour",
        "lat": location["lat"],
        "lon": location["lon"],
    }

    response = requests.get(
        "https://history.openweathermap.org/data/2.5/history/city", params=params
    )
    response.raise_for_status()

    data: list = response.json()["list"]

    save_raw_data(location, data, "weather")

    return data


def ingest_openweather():
    locations = _get_locations("ingest/config/locations.json")
    start_date = Timestamp("2025-09-09 00:00:00", "string")
    end_date = Timestamp("2025-09-10 23:59:59", "string")

    for location in locations:
        get_historical_weather(location, start_date, end_date)


if __name__ == "__main__":
    ingest_openweather()
