import logging

import azure.functions as func
import duckdb

from src.destinations.adls import ADLS
from src.destinations.local_directory import LocalDirectory
from src.ingest.openweather import OpenWeather
from src.transform.flattener import Flattener
from src.transform.transformer import Transformer
from src.utils.timestamp import Timestamp


app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="ingest-openweather")
def ingest_openweather(req: func.HttpRequest) -> func.HttpResponse:

    try:
        (
            OpenWeather()
            .set_location_directory(ADLS(directory="locations"))
            .set_destinations([ADLS(directory="raw")])
            .set_endpoints("all")
            .set_date_range(start_date=None, end_date=None)
            .set_ingestion_id(req.headers.get("run_id"))
            .fetch()
        )
    except Exception as e:
        logging.exception("There has been an error ingesting data from OpenWeather")
        return func.HttpResponse(
            f"There has been an error ingesting data from OpenWeather:\n{e}",
            status_code=501,
        )

    return func.HttpResponse(
        "Ingestion took place without errors.",
        status_code=200,
    )


@app.route(route="stage-openweather")
def stage_openweather(req: func.HttpRequest) -> func.HttpResponse:
    try:
        (
            Flattener()
            .set_source(ADLS(directory="raw"))
            .set_target(ADLS(directory="bronze"))
            .set_directories_to_parse("weather", "air_pollution")
            .set_identifier(req.headers.get("run_id"), "staged_id")
            .set_modified_at_column("staged_at")
            .flatten()
        )
    except Exception as e:
        logging.error(e)
        func.HttpResponse(f"ERROR: {e}", status_code=501)

    return func.HttpResponse(f"No erros, nice!", status_code=200)


@app.route(route="transform-openweather")
def transform_openweather(req: func.HttpRequest) -> func.HttpResponse:
    try:
        con = duckdb.connect()

        bronze = ADLS(directory="bronze")
        silver = ADLS(directory="silver")
        gold = ADLS(directory="gold")
        ml = ADLS(directory="ml")

        (
            Transformer(con)
            .set_models(
                [
                    ("sql/silver/weather_parsed.sql", silver),
                    ("sql/silver/weather__weather_parsed.sql", silver),
                    ("sql/silver/air_pollution_parsed.sql", silver),
                    ("sql/silver/weather_rich.sql", silver),
                    ("sql/gold/daily_general_report.sql", gold),
                    ("sql/ml/rain_prediction.sql", ml),
                ]
            )
            .import_tables_from_dir(bronze)
            .execute()
        )
    except Exception as e:
        logging.error(e)
        func.HttpResponse(f"ERROR: {e}", status_code=501)

    return func.HttpResponse("No errors, nice!", status_code=200)


# if __name__ == "__main__":
#     req = func.HttpRequest("get", "smth", body=b"")
#     # stage_openweather(req)
#     transform_openweather(req)
