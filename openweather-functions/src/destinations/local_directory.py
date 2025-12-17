import json
import logging
from datetime import date
from pathlib import Path
from typing import Generator

from duckdb import DuckDBPyConnection, DuckDBPyRelation
from polars import DataFrame

from src.destinations.base_destination import BaseDestination
from src.utils.types import Batch, Any


class LocalDirectory(BaseDestination):

    def __init__(self, directory: str | Path) -> None:
        super().__init__()

        if isinstance(directory, str):
            directory = Path(directory)

        self.dir: Path = directory
        self.name = f"Local Directory ({self.dir})"

        if not self.dir.exists():
            self.print("Creating out directory")
            self.dir.mkdir(parents=True)

    def save_batch(self, batch: Batch, out_file_path: Path):
        full_local_path = self.dir / out_file_path
        local_parent = full_local_path.parent
        if not local_parent.exists():
            local_parent.mkdir(exist_ok=True, parents=True)

        self.save_json(batch, out_file_path)

    def get_last_date_saved(self) -> dict[str, date]:
        self.print(
            "`get_last_date_saved` is not implemented, returning a placeholder value"
        )
        return {}

    def read_json_file(
        self, path: Path | str, prepend_context: bool = False
    ) -> tuple[str, Any]:
        path = path if isinstance(path, Path) else Path(path)

        if prepend_context:
            path = self.dir / path

        path = path.resolve()
        with open(path, "r") as f:
            return str(path), json.load(f)

    def iterate_data_in_files(
        self, dir: Path | str = "."
    ) -> Generator[tuple[str, list[dict[str, Any]]], None, None]:
        for date_dir in (self.dir / dir).iterdir():
            for data_file in date_dir.iterdir():
                yield self.read_json_file(data_file)

    def clean_up(self):
        if self.dir.exists():
            for dir in self.dir.iterdir():
                for nested_dir in dir.iterdir():
                    self._safe_rmdir(nested_dir)
                self._safe_rmdir(dir)
            self._safe_rmdir(self.dir)

    @staticmethod
    def _safe_rmdir(dir: Path):
        try:
            dir.rmdir()
        except OSError:
            pass

    def save_relation_as_parquet(
        self, dir: Path | str, df: DataFrame | DuckDBPyRelation, table_name: str
    ):
        # Different method for different df types
        fun = "write_parquet" if isinstance(df, DataFrame) else "to_parquet"

        # Add overwrite arg to duckdb df
        kwargs = {"overwrite": True} if isinstance(df, DuckDBPyRelation) else {}

        # Call method with dynamically generated vars
        getattr(df, fun)(str(self.dir / dir / (table_name + ".parquet")), **kwargs)

    def iter_dir_as_relations(
        self, con: DuckDBPyConnection, skip_on_error: bool = False
    ) -> Generator[tuple[str, DuckDBPyRelation], None, None]:
        for path in self.dir.iterdir():

            if not path.name.endswith(".parquet"):
                continue

            try:
                yield Path(path.name).stem, con.from_parquet(str(path))
            except Exception as e:
                if not skip_on_error:
                    raise RuntimeError(
                        f"Found error getting relation from '{path.name}'"
                    ) from e
                logging.warning(f"Could not get relation for file '{path.name}'\n{e}")

    def save_json(self, data: list[Any], file_name: str | Path):
        with open(self.dir / file_name, "w") as f:
            json.dump(data, f, indent=2)
