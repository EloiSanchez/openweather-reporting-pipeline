import logging
import os
import io
import json

from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Generator

from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from src.destinations.base_destination import BaseDestination


class ADLS(BaseDestination):
    name = "ADLS"

    def __init__(
        self,
        account_name: str | None = None,
        container: str | None = None,
        app_id: str | None = None,
        password: str | None = None,
        tenant_id: str | None = None,
        directory: str | Path | None = None,
    ) -> None:
        super().__init__()

        self.app_id = app_id
        self.password = password
        self.tenant_id = tenant_id
        self.account_name = account_name or os.environ["AZURE_ACCOUNT_NAME"]
        self.container = container or os.environ["AZURE_CONTAINER_NAME"]

        if not (self.app_id and self.password and self.tenant_id):
            self.print("Using default envrionment credentials")
            self.azure_credential = DefaultAzureCredential()
        else:
            self.print("Using given credentials")
            self.azure_credential = ClientSecretCredential(
                self.tenant_id, self.app_id, self.password
            )

        self.print("Initializing DataLakeServiceClient")
        self.service_client = DataLakeServiceClient(
            f"https://{self.account_name}.dfs.core.windows.net", self.azure_credential
        )

        self.filesystem = self.service_client.get_file_system_client(self.container)
        if not self.filesystem.exists():
            raise ValueError(
                f"FileSystem with Container name '{self.container}' does not exist "
                f"in account '{self.account_name}'"
            )

        self.directory = self.filesystem.get_directory_client(
            str(directory) if directory else "/"
        )
        if not self.directory.exists():
            self.directory.create_directory()

    def save_batch(
        self,
        batch: list[dict[str, Any]],
        out_file_path: Path,
    ):
        file_client = self.directory.get_file_client(str(out_file_path))

        with io.BytesIO(json.dumps(batch, indent=4).encode()) as binary_data:
            file_client.upload_data(binary_data, overwrite=True)

    def get_last_date_saved(self) -> dict[str, date]:
        self.print("Getting last date uploaded")
        max_dates = defaultdict(lambda: date(1, 1, 1))
        for path in self.directory.get_paths():
            print(path)
            path_without_root = path.name[len(self.directory.path_name) :].strip("/")
            print(path_without_root)
            if path.is_directory and "/" in path_without_root:
                *dir, date_str = path_without_root.split("/")
                dir = "/".join(dir)
                new_date = date.fromisoformat(date_str)
                max_dates[dir] = max(new_date, max_dates[dir])

        return max_dates or {}

    def iterate_file_data(
        self, dir: Path | str = "."
    ) -> Generator[tuple[str, list[dict[str, Any]]], None, None]:
        dir_client = self.filesystem.get_directory_client(
            "/".join(p.strip(" /") for p in (self.directory.path_name, str(dir)))
        )
        for path in dir_client.get_paths():
            if path.name.endswith(".json"):
                try:
                    yield path.name, json.loads(
                        self.filesystem.get_file_client(path.name)
                        .download_file()
                        .readall()
                    )
                except Exception as e:
                    raise e

    def save_relation_as_parquet(
        self, dir: Path | str, relation: DuckDBPyRelation, table_name: str
    ):
        if isinstance(dir, str):
            dir = Path(dir)
        file_client = self.directory.get_file_client(
            str(dir / (table_name + ".parquet"))
        )

        logging.info(f"Cloud location to save parquet file is {file_client.path_name}")

        tmp_file_name = f"/tmp/ingestion_{self.name}_{table_name}.parquet"
        logging.info(f"Getting tmp file {tmp_file_name}")
        tmp_file = Path(tmp_file_name)
        try:
            logging.info("Writing tmp parquet file")
            relation.to_parquet(tmp_file_name)
            logging.info("Uploading tmp file")
            with open(tmp_file, "rb") as f:
                file_client.upload_data(f, overwrite=True)
        finally:
            logging.info("Cleaning tmp file")
            tmp_file.unlink()

    def iter_dir_as_relations(
        self, con: DuckDBPyConnection, skip_on_error: bool = False
    ) -> Generator[tuple[str, DuckDBPyRelation], None, None]:
        for path in self.directory.get_paths():
            if not path.name.endswith(".parquet"):
                continue

            tmp_file_path = Path(f"/tmp/read_parquet__{path.name.replace('/', '_')}")

            try:
                with open(tmp_file_path, "wb") as tmp_file:
                    tmp_file.write(
                        self.filesystem.get_file_client(path.name)
                        .download_file()
                        .readall()
                    )

                yield Path(path.name).stem, con.from_parquet(str(tmp_file_path))

            except Exception as e:
                if not skip_on_error:
                    raise RuntimeError(
                        f"Found error getting relation from '{path.name}'"
                    ) from e
                logging.warning(f"Could not get relation for file '{path.name}'\n{e}")

            finally:
                tmp_file_path.unlink(missing_ok=True)
