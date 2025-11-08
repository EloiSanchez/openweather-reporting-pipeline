import logging

import azure.functions as func
import duckdb

from src.destinations.adls import ADLS
from src.ingest.cli import ingest_openweather as ingest
from src.transform.flattener import Flattener


app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="ingest-openweather")
def ingest_openweather(req: func.HttpRequest) -> func.HttpResponse:

    try:
        ingest(
            "ingestion_config/locations.json",
            start_date=None,
            end_date=None,
            upload_to_adls=True,
            save_local=False,
            endpoints="all",
            out_dir="raw",
            ingestion_id=req.headers.get("run_id"),
        )
    except Exception as e:
        logging.exception("There has been an error ingesting data from OpenWeather")
        return func.HttpResponse(
            f"There has been an error ingesting data from OpenWeather:\n{e}",
            status_code=500,
        )

    return func.HttpResponse(
        "Ingestion took place without errors.",
        status_code=200,
    )


@app.route(route="transform-openweather")
def transform_openweather(req: func.HttpRequest) -> func.HttpResponse:
    try:
        con = duckdb.connect()
        flattener = (
            Flattener(con)
            .set_source(ADLS(directory="raw"))
            .set_target(ADLS(directory="bronze"))
            .set_directories_to_parse("weather", "air_pollution")
            .set_identifier(req.headers.get("run_id"), "staged_id")
            .set_modified_at_column("staged_at")
        )
        flattener.flatten()
    except Exception as e:
        logging.error(e)
        func.HttpResponse(f"ERROR: {e}", status_code=500)

    return func.HttpResponse(f"No erros, nice!", status_code=200)


# if __name__ == "__main__":
#     logging.getLogger().setLevel(logging.DEBUG)
#     req = func.HttpRequest("get", "smth", body=b"")
#     transform_openweather(req)
