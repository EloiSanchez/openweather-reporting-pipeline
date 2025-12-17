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


if __name__ == "__main__":
    parser = ArgumentParser("OpenWeather Ingestion CLI")

    parser.add_argument("--locations-dir", "-ld", required=True)
    parser.add_argument("--locations-local", "-ll", action="store_true")
    parser.add_argument("--start-date", "-sd")
    parser.add_argument("--end-date", "-ed")
    parser.add_argument("--upload-to-adls", "-adls", action="store_true")
    parser.add_argument("--save-local", "-sl", action="store_true")
    parser.add_argument("--endpoints", "-e", action="extend", nargs="+", type=str)
    parser.add_argument("--out-directory", "-o")
    parser.add_argument("--ingestion-id", "-id")
    args = parser.parse_args()

    locations_dir = args.locations_dir
    locations_local = args.locations_local
    start_date = args.start_date
    end_date = args.end_date
    upload_to_adls = args.upload_to_adls
    save_local = args.save_local
    endpoints = args.endpoints or "all"
    out_directory = args.out_directory
    ingestion_id = args.ingestion_id

    # Handle destinations
    destinations: list[BaseDestination] = []
    if upload_to_adls:
        destinations.append(ADLS(directory=out_directory))

    if save_local:
        if not out_directory:
            out_directory = Path(".")
        destinations.append(LocalDirectory(out_directory))

    if not destinations:
        raise ValueError(
            "Either data is uploaded to adls (-adls), an output directory is given (-o "
            "<out_dir>) or both."
        )

    # Date range
    start_date = Timestamp(start_date)
    end_date = Timestamp(end_date)

    # Handle locations source
    if locations_local:
        locations = LocalDirectory(locations_dir)
    else:
        locations = ADLS(directory=locations_dir)

    open_weather = (
        OpenWeather()
        .set_date_range(start_date=start_date, end_date=end_date)
        .set_location_directory(locations)
        .set_endpoints(endpoints)
        .set_destinations(destinations)
    )
    if ingestion_id:
        open_weather.set_ingestion_id(ingestion_id)

    open_weather.fetch()
