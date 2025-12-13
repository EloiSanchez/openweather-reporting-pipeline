import json
from typing import Any, Generator, Literal, overload

import polars as pl

from src.utils.types import ColumnDefinition, NestedKeyPath, DictRow


class DictTable:

    def __init__(self, name, rows=None) -> None:
        self.name = name
        self.columns: list[NestedKeyPath] = []
        self.rows: list[DictRow] = []
        if rows:
            self.rows = rows

    def update_columns(self, columns: list[NestedKeyPath]):
        for column in columns:
            if column not in self.columns:
                self.columns.append(column)

    def set_rows(self, rows):
        self.rows = rows.copy()

    def __repr__(self) -> str:
        value = f"\nTable name: {self.name}\n"

        value += "\nColumns\n"
        for column in self.columns:
            value += "- " + "__".join(column[1:]) + "\n"

        value += f"\nNumber of rows: {len(self.rows)}\n"
        if self.rows:
            value += "\nRow example:\n"
            value += json.dumps(self.rows[0], indent=4)

        value += "\n\n"

        return value

    def merge(self, other: "DictTable"):
        self.update_columns(other.columns)
        self.rows.extend(other.rows)

    def get_data(self) -> Generator[list[Any], None, None]:
        for row in self.rows:
            new_row = []
            for column in self.columns:
                # print(f"Accessing {row=} {column=}")
                new_row.append(self.access_nested_key(row, column, True))

            yield new_row

    def get_schema(self) -> list[ColumnDefinition]:
        column_type: pl.DataType = pl.String()
        columns: list[ColumnDefinition] = []
        for keys in self.columns:

            # TODO: This can be extended to identify the datatype of the columns by
            # iterating over their values, instead of always returning pl.String()

            columns.append(ColumnDefinition(name="__".join(keys[1:]), type=column_type))

        return columns

    @staticmethod
    @overload
    def access_nested_key(
        dictionary: dict, nested_keys: NestedKeyPath, safe_return: Literal[True]
    ) -> Any | None: ...

    @staticmethod
    @overload
    def access_nested_key(
        dictionary: dict, nested_keys: NestedKeyPath, safe_return: Literal[False]
    ) -> Any: ...

    @staticmethod
    @overload
    def access_nested_key(dictionary: dict, nested_keys: NestedKeyPath) -> str: ...

    @staticmethod
    def access_nested_key(
        dictionary: dict, nested_keys: NestedKeyPath, safe_return: bool = False
    ) -> Any | None:
        value = dictionary

        for index_count, k in enumerate(nested_keys):
            if k not in value:
                if (value == dictionary) and (
                    index_count + 1 < len(nested_keys)
                ):  # If still at first level and multiple levels to go
                    continue
                elif safe_return:
                    return None
            new_value = value[k]
            if not isinstance(value[k], list):
                value = new_value

        if not isinstance(value, dict):
            return str(value)
