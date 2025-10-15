import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Literal
from argparse import ArgumentParser

from adls_uploader import ADLSUploader
from openweather import OpenWeather
from utils import Timestamp, AvailableEndpoints


OPENWEATHER_SECRET = os.environ["OPENWEATHER_SECRET_KEY"]


def ingest_openweather(
    locations_path: str | Path,
    start_date: date | datetime | int | str | None,
    end_date: date | datetime | int | str | None,
    upload_to_adls: bool,
    endpoints: Iterable[AvailableEndpoints] | Literal["all"],
    out_dir: str | Path,
):

    # Handle ADLS
    if upload_to_adls:
        adls_uploader = ADLSUploader(
            os.environ["AZURE_ACCOUNT_NAME"], os.environ["AZURE_CONTAINER_NAME"]
        )
    else:
        adls_uploader = None

    # Handle start and end dates
    if start_date is None:
        if adls_uploader:
            max_dates = adls_uploader.get_last_date_uploaded()
            if max_dates:
                start = Timestamp(min(max_dates.values()) + timedelta(days=1))
            else:
                raise ValueError(
                    "`start_date` was not provided and previous data could not be found in ADLS"
                )
        else:
            raise ValueError("`start_date` must be provided when not working with adls")
    else:
        start = Timestamp(start_date)

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
        .set_raw_dir_path(out_dir)
    )
    print(open_weather.start_date)
    print(open_weather.end_date)

    if upload_to_adls:
        assert isinstance(adls_uploader, ADLSUploader)
        open_weather = open_weather.set_adls_location(adls_uploader)

    open_weather.fetch()


if __name__ == "__main__":
    parser = ArgumentParser("OpenWeather Fetching CLI")

    parser.add_argument("--locations-path", "-lp", required=True)
    parser.add_argument("--start-date", "-sd")
    parser.add_argument("--end-date", "-ed")
    parser.add_argument("--upload-to-adls", "-adls", action="store_true")
    parser.add_argument("--endpoints", "-e", action="extend", nargs="+", type=str)
    parser.add_argument("--out-directory", "-o")
    args = parser.parse_args()

    locations_path = args.locations_path
    start_date = args.start_date
    end_date = args.end_date
    upload_to_adls = args.upload_to_adls
    endpoints = args.endpoints or "all"
    out_directory = args.out_directory
    if not upload_to_adls and not out_directory:
        raise ValueError(
            "Either data is uploaded to adls (-adls), an output directory is given (-o "
            "<out_dir>) or both."
        )

    if not out_directory and upload_to_adls:
        out_directory = ".tmp_out"

    print(args)
    ingest_openweather(
        locations_path, start_date, end_date, upload_to_adls, endpoints, out_directory
    )
