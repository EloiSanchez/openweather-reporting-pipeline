# TFM Eloi UOC 2025-2026

This project contains the code deliverable for my Master Thesis at Universitat Oberta de Catalunya, coursed during the Autumn-Winter Semester of 2025-2026.

- **Author**: Eloi Sanchez Ambros
- **Professor**: Francesc Julbe Lopez

## Code structure

The code is structured in the following way

```shell
./
├── media/
│ # └─ Figures and diagrams
├── notebooks/
│ # └─ One-time execution ML/Analysis notebooks
└── openweather-functions/
    ├── function_app.py # <-- Starting point of Azure Functions executions
    ├── ingestion_config/
    │ # └─ Config files required for ingestion, i.e. locations to fetch
    ├── sql/
    │   ├── gold/
    │   ├── ml/
    │   └── silver/
    │ # └─ SQL files used for database transformations in the transform step
    └── src/
        ├── destinations/
        │ # └─ Objects that interact with file storing destinations, i.e. ADLS
        ├── ingest/
        │ # └─ Scripts for data fetching and saving into a destination
        ├── transform/
        │ # └─ Scripts for both initially parsing data and further transforming it
        └── utils/
          # └─ Helper tools used in all other Python scripts and modules
```

## Azure Functions

Even though the code can technically be used fully locally (since there is an implementation for a `LocalDirectory` destination), the `openweather-functions` directory is structured to be compatible with Azure functions.

### Requirements

- [Azure Functions Core Tools](https://learn.microsoft.com/en-us/azure/azure-functions/functions-run-local?tabs=linux%2Cisolated-process%2Cnode-v4%2Cpython-v2%2Chttp-trigger%2Ccontainer-apps&pivots=programming-language-python#install-the-azure-functions-core-tools)

### Local development

In order to develop and test out new features, Azure Functions can be deployed locally for testing purposes, which does not require an Azure Functions Application to exist in the cloud. Ensure you are in the `openweather-functions` directory and execute:

```bash
func start
```

The functions will be deployed to the localhost, where they can be triggered as they would when hosted in the Azure Functions Cloud environment.

### Deploy to Azure Functions

Ensure azure CLI has been logged in:

```bash
az login
```

Create a new deployment for Azure Functions.

```bash
func azure functionapp publish <APP_NAME> --build=local
```

with `<APP_NAME>` being the name, accessible with the logged in

This will deploy an azure function that can be triggered via a GET request to
`https://ingest-openweather-eloi.azurewebsites.net/api/ingest-openweather-eloi` with
the corresponding function code as a parameter for authorization.

## Ingestion CLI

Besides the full project being able to be used via Azure functions, the ingestion step implements a CLI to facilitate fine-grained ingestions.

The CLI is implemented via the `argparse` library.

In order to use, install the required libraries in a new environment. These commands work for linux-based systems, but similar ones exist for other OS.

```bash
cd openweather-functions;
python3 -m venv .venv && source .venv/bin/activate && pip install requirements.txt;
```

The CLI is available at `src/ingest/cli.py`, and can be called with the following syntax:

```bash
usage: OpenWeather Ingestion CLI [-h] --locations-dir LOCATIONS_DIR
                                 [--locations-local] [--start-date START_DATE]
                                 [--end-date END_DATE] [--upload-to-adls]
                                 [--save-local]
                                 [--endpoints ENDPOINTS [ENDPOINTS ...]]
                                 [--out-directory OUT_DIRECTORY]
                                 [--ingestion-id INGESTION_ID]

options:
  -h, --help
    # show this help message and exit
  --locations-dir LOCATIONS_DIR, -ld LOCATIONS_DIR
    # path to the directory where the locations configuration file should be found
  --locations-local, -ll
    # If passed, the locations-dir is looked for in the local system, else, the path is found in the default ADLS directory
  --start-date START_DATE, -sd START_DATE
    # Start ingestion from this date, including. Format "YYYY-MM-DD HH:MM:SS"
  --end-date END_DATE, -ed END_DATE
    # End ingestion at this date, including. Format "YYYY-MM-DD HH:MM:SS"
  --upload-to-adls, -adls
    # Whether to upload the ingested data into ADLS default directory
  --save-local, -sl
    # Whether to save the ingested data locally
  --endpoints ENDPOINTS [ENDPOINTS ...], -e ENDPOINTS [ENDPOINTS ...]
    # List of endpoints to include in the ingestion process
  --out-directory OUT_DIRECTORY, -o OUT_DIRECTORY
    # Out path of where to save the data
  --ingestion-id INGESTION_ID, -id INGESTION_ID
    # Whether to send a specific id to tag this ingestion. If not passed, a default is used
```

### Examples

Fetch data from the 1st December to 7th of December of 2025 using the example configuration for locations in this repository, and saving the data into `data/raw` locally.

```bash
export OPENWEATHER_SECRET_KEY="<your-key>";

python3 -m src.ingest.cli \
    --locations-local \
    --locations-dir ../ingestion_config/ \
    --start-date "2025-12-01 00:00:00" \
    --end-date "2025-12-07 23:59:59"  \
    --save-local \
    --out-directory data/raw
```

After the fetching process has finished, data will be available in the `data/raw/` directory, partitioned by endpoint fetch and day of recording.

The same can be done but getting the locations config from the ADLS container, and also saving the ingested data in ADLS.

```bash
export AZURE_CLIENT_SECRET="<your-secret>";
export AZURE_CLIENT_ID="<your-client-id>";
export AZURE_TENANT_ID="<your-tenant-id>";
export AZURE_ACCOUNT_NAME="<your-azure-account>";
export AZURE_CONTAINER_NAME="<your-container-name>";

python3 -m src.ingest.cli \
    --locations-dir locations/ \
    --start-date "2025-12-01 00:00:00" \
    --end-date "2025-12-07 23:59:59"  \
    --upload-to-adls \
    --out-directory raw
```


## Staging API

The staging code takes the raw data coming from the ingestion and flattens it into analytics-ready parquet files. Here is an example of usage.

```python
# ./openweather-functions/staging_example.py

from src.transform.flattener import Flattener
from src.destinations.local_directory import LocalDirectory


(
    Flattener()
    .set_source(LocalDirectory(dir="data/raw"))
    .set_target(LocalDirectory(dir="data/bronze"))
    .set_directories_to_parse("weather", "air_pollution")
    .set_modified_at_column("staged_at")
    .flatten()
)
```

Changing the `LocalDirectory` source and target for `ADLS` would perform the reading and saving of raw and transformed data into the ADLS container specified by the exported credentials from previous steps.


## Transformation API

Once data is readily available for analytics transformations, we can write sql code with the following syntax:

```sql
-- ./openweather-functions/sql/silver/weather_recordings_agg.sql

select
    count(*) as recording_count
from weather  -- Requires a "weather.parquet" file to exist in the import dir of the Transformer
```

The `Transformer` object will facilitate the execution of the SQL:

```python
# ./openweather-functions/transformation_example.py

import duckdb

from src.transform.transformer import Transformer
from src.destinations.local_directory import LocalDirectory


con = duckdb.connect()  # In memory database

bronze = LocalDirectory(dir="data/bronze")
silver = LocalDirectory(dir="data/silver")

(
    Transformer(con)
    .set_models(
        [
            ("sql/silver/weather_recordings_agg.sql", silver),
            # Put here other models that you would like to create
        ]
    )
    .import_tables_from_dir(bronze) # Make available the tables in this directory the in-memory database
    .execute()
)
```

After executing the following, we should see a new file created in `data/silver/weather_recordings_agg.parquet` with our new aggregated model.

Again, if we want to interact with the ADLS cloud storage, only the `bronze` and `silver` locations above must be changed to use `ADLS` instead of `LocalDirectory`

