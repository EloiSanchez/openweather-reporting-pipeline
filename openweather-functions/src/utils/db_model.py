from pathlib import Path

from duckdb import DuckDBPyConnection, DuckDBPyRelation
from src.destinations.base_destination import BaseDestination


class DBModel:

    def __init__(
        self,
        con: DuckDBPyConnection,
        sql_path: str | Path,
        table_name: str,
        destination: BaseDestination,
    ) -> None:
        self.con: DuckDBPyConnection = con
        self.sql_path: Path = sql_path if isinstance(sql_path, Path) else Path(sql_path)
        self.table_name: str = table_name
        self.destination: BaseDestination = destination
        self.relation: DuckDBPyRelation

    def execute(self, write_to_file: bool = True):

        # Read and execute sql transformation
        with open(self.sql_path, "r") as sql_file:
            self.relation = self.con.sql(sql_file.read())

        # Save to duckdb database
        self.relation.to_table(self.table_name)

        if write_to_file:
            # Save to target destination
            self.destination.save_relation_as_parquet(
                ".", self.relation, self.table_name
            )
        else:
            self.relation.show()
