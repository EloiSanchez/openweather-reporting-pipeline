from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import Generator, Any

from duckdb import DuckDBPyRelation

from src.utils.types import Batch, NestedKeyPath, DictRow
from src.utils.dict_table import DictTable


class BaseDestination(ABC):
    name: str

    @abstractmethod
    def save_batch(self, batch: Batch, out_file_path: Path): ...

    @abstractmethod
    def get_last_date_saved(self) -> dict[str, date]: ...

    @abstractmethod
    def iterate_file_data(
        self, dir: Path | str
    ) -> Generator[tuple[str, list[DictRow]], None, None]: ...

    @abstractmethod
    def save_relation_as_parquet(
        self, dir: Path | str, relation: DuckDBPyRelation, table_name: str
    ): ...

    def clean_up(self):
        pass

    def print(self, value: str):
        print(f"{self.name}: {value}")

    def read_tables_from_dir(self, dir: Path | str, root_table_name: str):

        if isinstance(dir, str):
            dir = Path(dir)

        tables: dict[str, DictTable] = {}
        for path, data in self.iterate_file_data(dir):
            found_tables = self.flatten_dict_rows(
                data,
                root_table_name,
                {"path": str(path)},
                [["path"], ["dt"]],
            )
            for table_name, table in found_tables.items():
                if table_name in tables:
                    tables[table_name].merge(table)
                else:
                    tables[table_name] = table
        return tables

    def extract_keys(
        self,
        dictionary: dict,
        all_paths: list | None = None,
        previous_path: list | None = None,
        row_id: Any = None,
    ) -> tuple[NestedKeyPath, list[NestedKeyPath], dict[str, DictTable]]:
        secondary_tables = {}
        if previous_path is None:
            previous_path = []

        if all_paths is None:
            all_paths = []

        for k, v in dictionary.items():
            if isinstance(v, dict):
                _, all_paths, _ = self.extract_keys(
                    v, all_paths, previous_path + [k], row_id
                )
            elif isinstance(v, list):
                secondary_tables = self.flatten_dict_rows(
                    v, "__".join(previous_path + [k]), {"parent_id": row_id}
                )
            else:
                full_path = previous_path + [k]
                if full_path not in all_paths:
                    all_paths.append(full_path)

        all_paths.sort()
        return previous_path, all_paths, secondary_tables

    def flatten_dict_rows(
        self,
        rows: list[DictRow],
        table_name: str = "root",
        id_stamp: dict[str, str] | None = None,
        id_of_rows: list[list[str]] | None = None,
    ) -> dict[str, DictTable]:

        def get_compound_id(dictionary: dict, keys: list[list[str]]) -> str:
            values = []
            for nested_keys in keys:
                values.append(str(DictTable.access_nested_key(dictionary, nested_keys)))
            return "-".join(values)

        tables_found = {table_name: DictTable(table_name)}
        all_paths = []
        for row in rows:
            if id_stamp:
                row.update(id_stamp)
            row_id = None
            if id_of_rows:
                row_id = get_compound_id(row, id_of_rows)
            _, all_paths, secondary_tables = self.extract_keys(
                row, all_paths, [table_name], row_id
            )
            tables_found[table_name].update_columns(all_paths)
            for found_table_name, found_table in secondary_tables.items():
                if found_table_name in tables_found:
                    tables_found[found_table_name].merge(found_table)
                else:
                    tables_found[found_table_name] = found_table

        tables_found[table_name].set_rows(rows)

        return tables_found
