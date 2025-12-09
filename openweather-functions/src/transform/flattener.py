import logging
import datetime
from typing import Self

from duckdb import DuckDBPyConnection

from src.destinations.base_destination import BaseDestination
from src.utils.types import ColumnDefinition


class Flattener:

    def __init__(self, duckdb_connection: DuckDBPyConnection) -> None:
        self.name: str = "flattener"
        self.source: BaseDestination
        self.directories: list[str]
        self.target: BaseDestination
        self.id: str = datetime.datetime.now().isoformat()
        self.column_id: str = "flattener_id"
        self.at_column_name: str
        self.con: DuckDBPyConnection = duckdb_connection
        self.logger = logging.getLogger()

    def set_source(self, source: BaseDestination) -> Self:
        self.source = source
        return self

    def set_target(self, target: BaseDestination) -> Self:
        self.target = target
        return self

    def set_directories_to_parse(self, *dirs: str) -> Self:
        self.directories = list(dirs)
        return self

    def set_identifier(self, id: str | None, column_id: str | None = None) -> Self:
        if id:
            self.id = id
        if column_id:
            self.column_id = column_id
        return self

    def set_modified_at_column(self, column_name: str) -> Self:
        self.at_column_name = column_name
        return self

    def flatten(self):

        for dir in self.directories:
            # Parse tables from directory
            self.logger.info("Flattening files in dir %s", str(dir))
            tables = self.source.read_tables_from_dir(dir, dir)

            for table in tables.values():
                self.logger.info("Flattening table %s", table.name)

                # Create table
                column_types = []
                id_column = ColumnDefinition(name=self.column_id, type="VARCHAR")
                modifed_at_column = ColumnDefinition(
                    name=self.at_column_name,
                    type="VARCHAR",
                )
                for column_definition in table.get_schema() + [
                    id_column,
                    modifed_at_column,
                ]:
                    column_types.append(
                        f"\n{column_definition['name']} {column_definition['type']}"
                    )
                column_types_str = ",".join(column_types)
                query = f"create table {table.name} ({column_types_str});"
                self.con.sql(query)

                # Add data to table
                rel = self.con.table(table.name)
                self.con.begin()
                ingestion_time = datetime.datetime.now().isoformat()
                for row in table.get_data():
                    rel.insert(row + [self.id, ingestion_time])
                self.con.commit()

                self.target.save_relation_as_parquet(".", rel, table.name)
