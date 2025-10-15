from collections import defaultdict
from datetime import date
import os
from pathlib import Path
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient


class ADLSUploader:

    def __init__(
        self,
        account_name: str,
        container: str,
        app_id: str | None = None,
        password: str | None = None,
        tenant_id: str | None = None,
    ) -> None:
        print("Initializing ADLSUploader")
        self.app_id = app_id
        self.password = password
        self.tenant_id = tenant_id
        self.account_name = account_name
        self.container = container

        if not (self.app_id and self.password and self.tenant_id):
            print("Using default env creds")
            self.azure_credential = DefaultAzureCredential()
        else:
            print("Using given creds")
            self.azure_credential = ClientSecretCredential(
                self.tenant_id, self.app_id, self.password
            )

        print("Initializing DataLakeServiceClient")
        self.service_client = DataLakeServiceClient(
            f"https://{self.account_name}.dfs.core.windows.net", self.azure_credential
        )

        self.filesystem = self.service_client.get_file_system_client(self.container)
        if not self.filesystem.exists():
            raise ValueError(
                f"FileSystem with Container name '{self.container}' does not exist "
                f"in account '{self.account_name}'"
            )

    def upload_file(
        self, local_file_path: Path, cloud_file_path: Path, clear_source: bool = False
    ):
        file_client = self.filesystem.get_file_client(str(cloud_file_path))

        with open(local_file_path, "rb") as f:
            file_client.upload_data(f, overwrite=True)

        if clear_source:
            os.remove(local_file_path)

    def get_last_date_uploaded(self) -> dict[str, date]:
        print("Getting last date uploaded")
        max_dates = defaultdict(lambda: date(1, 1, 1))
        for path in self.filesystem.get_paths():
            if path.is_directory and "/" in path.name:
                *dir, date_str = path.name.split("/")
                dir = "/".join(dir)
                new_date = date.fromisoformat(date_str)
                max_dates[dir] = max(new_date, max_dates[dir])

        return max_dates or {}
