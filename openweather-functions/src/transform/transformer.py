import logging
from pathlib import Path
from typing import Iterable, Self
from duckdb import DuckDBPyConnection

from src.destinations.base_destination import BaseDestination
from src.utils.db_model import DBModel


class Transformer:

    def __init__(self, con: DuckDBPyConnection) -> None:
        self.models: list[DBModel] = []
        self.con: DuckDBPyConnection = con

    def import_tables_from_dir(self, destination: BaseDestination) -> Self:
        for table_name, relation in destination.iter_dir_as_relations(
            self.con, skip_on_error=True
        ):
            relation.to_table(table_name)
            logging.info(f"Read table '{table_name}' from {destination.name}")
        return self

    def set_models(
        self, transformations: Iterable[tuple[str | Path, BaseDestination]]
    ) -> Self:
        for path, target_location in transformations:
            path = path if isinstance(path, Path) else Path(path)
            self.models.append(DBModel(self.con, path, path.stem, target_location))
        return self

    def execute(self, write_to_tables: bool = True):
        for model in self.models:
            model.execute(write_to_tables)
