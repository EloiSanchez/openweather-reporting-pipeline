import io
import json
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Generator

from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

from src.destinations.base_destination import BaseDestination


class ADLS(BaseDestination):
    name = "ADLS"

    def __init__(
        self,
        account_name: str,
        container: str,
        app_id: str | None = None,
        password: str | None = None,
        tenant_id: str | None = None,
        directory: str | Path | None = None,
    ) -> None:
        super().__init__()

        self.app_id = app_id
        self.password = password
        self.tenant_id = tenant_id
        self.account_name = account_name
        self.container = container

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

        self.directory = self.filesystem.get_directory_client(str(directory) or "/")
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
            if path.is_directory and "/" in path.name:
                *dir, date_str = path.name.split("/")
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
