import logging
import datetime

from typing import Self

import polars as pl

from src.destinations.base_destination import BaseDestination


class Flattener:

    def __init__(self) -> None:
        self.name: str = "flattener"
        self.source: BaseDestination
        self.directories: list[str]
        self.target: BaseDestination
        self.id: str = datetime.datetime.now().isoformat()
        self.column_id: str = "flattener_id"
        self.at_column_name: str
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
                self.logger.info("Found table %s", table.name)

                # Add data to table
                ingestion_time = datetime.datetime.now().isoformat()

                df = pl.DataFrame(
                    table.get_data(),
                    {x["name"]: x["type"] for x in table.get_schema()},
                )
                df = df.with_columns(
                    pl.lit(self.id).alias(self.column_id),
                    pl.lit(ingestion_time).alias(self.at_column_name),
                )

                self.logger.info("Saving as parquet...")
                self.target.save_relation_as_parquet(".", df, table.name)
