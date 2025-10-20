import logging

import azure.functions as func

from openweather_src import main

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="ingest-openweather-eloi")
def ingest_openweather_eloi(req: func.HttpRequest) -> func.HttpResponse:

    logging.debug(req.params)

    try:
        main.ingest_openweather(
            "config/locations.json",
            start_date=None,
            end_date=None,
            upload_to_adls=True,
            endpoints="all",
            out_dir=None,
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
