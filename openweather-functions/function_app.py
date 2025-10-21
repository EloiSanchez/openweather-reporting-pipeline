import logging

import azure.functions as func

from openweather_src import main

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="ingest-openweather")
def ingest_openweather(req: func.HttpRequest) -> func.HttpResponse:

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


@app.route(route="transform-openweather")
def transform_openweather(req: func.HttpRequest) -> func.HttpResponse:

    if req.params:
        return func.HttpResponse(
            "Parameters seen: \n"
            + ",\n".join(f"{k}: {v}" for k, v in req.params.items()),
            status_code=200,
        )

    return func.HttpResponse("No erros, nice!", status_code=200)
