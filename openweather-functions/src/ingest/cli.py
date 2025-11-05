import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Literal
from argparse import ArgumentParser


from src.ingest.openweather import OpenWeather
from src.destinations.adls import ADLS
from src.destinations.local_directory import LocalDirectory
from src.destinations.base_destination import BaseDestination
from src.utils.timestamp import Timestamp
from src.utils.types import AvailableEndpoints


def ingest_openweather(
    locations_path: str | Path,
    start_date: date | datetime | int | str | None,
    end_date: date | datetime | int | str | None,
    upload_to_adls: bool,
    save_local: bool,
    endpoints: Iterable[AvailableEndpoints] | Literal["all"],
    out_dir: str | Path | None,
):
    OPENWEATHER_SECRET = os.environ["OPENWEATHER_SECRET_KEY"]

    # Handle destionations
    destinations: list[BaseDestination] = []
    if upload_to_adls:
        destinations.append(
            ADLS(
                os.environ["AZURE_ACCOUNT_NAME"],
                os.environ["AZURE_CONTAINER_NAME"],
                directory=out_dir,
            )
        )

    if save_local:
        if out_dir is None:
            out_dir = Path(".")
        destinations.append(LocalDirectory(out_dir))

    if not destinations:
        raise ValueError(
            "Either data is uploaded to adls (-adls), an output directory is given (-o "
            "<out_dir>) or both."
        )

    # Handle start date
    if start_date is None:
        max_dates: list[date] = []
        for destination in destinations:
            max_dates.extend(
                [min_date for min_date in destination.get_last_date_saved().values()]
            )

        if max_dates:
            start = Timestamp(min(max_dates) + timedelta(days=1))
        else:
            raise ValueError(
                "`start_date` was not provided and previous data could not be found in "
                f"destinations ({destinations})"
            )
    else:
        start = Timestamp(start_date)

    # Handle end date
    if end_date is None:
        yesterday = date.today() - timedelta(days=1)
        end = Timestamp(
            datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)
        )
    else:
        end = Timestamp(end_date)

    if start >= end:
        raise ValueError(f"Found start date {start} higher than end date {end} ")

    open_weather = (
        OpenWeather(OPENWEATHER_SECRET)
        .set_date_range(start_date=start, end_date=end)
        .set_locations_path(locations_path)
        .set_endpoints(endpoints)
        .set_destinations(destinations)
    )

    open_weather.fetch()


if __name__ == "__main__":
    parser = ArgumentParser("OpenWeather Ingestion CLI")

    parser.add_argument("--locations-path", "-lp", required=True)
    parser.add_argument("--start-date", "-sd")
    parser.add_argument("--end-date", "-ed")
    parser.add_argument("--upload-to-adls", "-adls", action="store_true")
    parser.add_argument("--save-local", "-sl", action="store_true")
    parser.add_argument("--endpoints", "-e", action="extend", nargs="+", type=str)
    parser.add_argument("--out-directory", "-o")
    args = parser.parse_args()

    locations_path = args.locations_path
    start_date = args.start_date
    end_date = args.end_date
    upload_to_adls = args.upload_to_adls
    save_local = args.save_local
    endpoints = args.endpoints or "all"
    out_directory = args.out_directory

    ingest_openweather(
        locations_path,
        start_date,
        end_date,
        upload_to_adls,
        save_local,
        endpoints,
        out_directory,
    )
