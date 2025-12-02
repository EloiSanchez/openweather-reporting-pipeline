import os
import datetime
import requests
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Literal, Self

from src.destinations.base_destination import BaseDestination
from src.utils.timestamp import Timestamp
from src.utils.types import (
    Batch,
    EndpointConfig,
    AvailableEndpoints,
    Location,
)


class OpenWeather:

    def __init__(self, secret: str | None) -> None:
        self.name = "OpenWeather"
        self.ingestion_id = self.name + "-" + datetime.datetime.now().isoformat()
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
        self.destinations: list[BaseDestination] = []

    @property
    def base_params(self) -> dict[str, Any]:
        return {
            "appid": self.secret,
            "start": self.start_date.unix,
            "end": self.end_date.unix,
        }

    def get_params(
        self,
        location: Location,
        start_date: Timestamp | None = None,
        end_date: Timestamp | None = None,
    ) -> dict[str, Any]:
        params = self.base_params.copy()
        params.update({"lat": location["lat"], "lon": location["lon"]})
        if start_date:
            params.update({"start": start_date.unix})
        if end_date:
            params.update({"start": end_date.unix})
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

    def set_locations(self, destination: BaseDestination) -> Self:
        files = destination.read_json_file("locations.json", prepend_context=True)

        _, file_content = files

        self.locations = []
        fetched_new_data = False
        for location in file_content:
            if not ("lat" in location and "long" in location):
                params: dict[str, Any] = {
                    "appid": self.base_params["appid"],
                    "q": f'{location["search_name"]},{location["country_code"]}',
                    "limit": 1,
                }
                response = requests.get(
                    f"http://api.openweathermap.org/geo/1.0/direct", params=params
                )
                response.raise_for_status()
                data: dict[str, str] = response.json()[0]
                if "local_names" in data:
                    data.pop("local_names")
                if "country" in data:
                    data.pop("country")
                location.update(data)
                fetched_new_data = True
            self.locations.append(
                Location(
                    search_name=location["search_name"],
                    name=location["name"],
                    country_code=location["country_code"],
                    lat=location["lat"],
                    lon=location["lon"],
                )
            )

        if fetched_new_data:
            destination.save_json(self.locations, "locations.json")

        return self

    def set_endpoints(
        self, endpoints: Iterable[AvailableEndpoints] | Literal["all"]
    ) -> Self:
        if endpoints == "all":
            self.endpoints = list(self.endpoint_config.keys())
        else:
            self.endpoints = endpoints
        return self

    def set_raw_dir_path(self, raw_dir_path: str | Path) -> Self:
        if isinstance(raw_dir_path, str):
            self.raw_dir = Path(raw_dir_path)
        else:
            self.raw_dir = raw_dir_path
        return self

    def set_ingestion_id(self, id: str):
        self.ingestion_id = id

    def set_destinations(self, destinations: list[BaseDestination]) -> Self:
        self.destinations = destinations
        return self

    def add_destination(self, destination: BaseDestination):
        self.destinations.append(destination)

    def fetch(self):

        if not hasattr(self, "destinations"):
            raise RuntimeError("Output location must be set before fetching data")

        try:
            for location in self.locations:
                for endpoint in self.endpoints:
                    self.fetch_endpoint(endpoint, location)
        except Exception as e:
            raise e
        finally:
            for destination in self.destinations:
                destination.clean_up()

    def fetch_endpoint(self, endpoint: AvailableEndpoints, location: Location):
        finished_fetch = False
        max_date = self.start_date
        while not finished_fetch:
            start_date = max_date.get_as_start()
            params = self.get_params(location, start_date=start_date)
            params.update(self.endpoint_config[endpoint]["extra_params"])

            response = requests.get(
                self.endpoint_config[endpoint]["url"], params=params
            )
            response.raise_for_status()

            data = response.json()["list"]
            for row in data:
                row_date = Timestamp(int(row["dt"]))
                if row_date.datetime.date() == self.end_date.datetime.date():
                    finished_fetch = True
                    break
                elif row_date > max_date:
                    max_date = row_date

            self.save_raw_data(location, data, endpoint)
            max_date = max_date + datetime.timedelta(days=1)

    def save_raw_data(
        self, location: Location, data: list[dict[str, Any]], endpoint_name: str
    ):
        for date, batch in self.batch_raw_data(data).items():
            out_file_path = (
                Path(endpoint_name)
                / date
                / (location["search_name"].replace(" ", "_").lower() + ".json")
            )
            for destination in self.destinations:
                destination.save_batch(batch, out_file_path)

    def batch_raw_data(self, data: list[dict[str, Any]]) -> dict[str, Batch]:
        batched_data = defaultdict(list)
        stamp = {"source": self.name, "ingestion_id": self.ingestion_id}
        for row in data:
            row.update(stamp)
            row["ingested_at"] = datetime.datetime.now().isoformat()
            dt = Timestamp(row["dt"]).date
            batched_data[dt].append(row)

        return batched_data
