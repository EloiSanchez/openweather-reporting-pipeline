import json
from typing import Any, Literal, overload

from src.utils.types import NestedKeyPath, DictRow


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
            value += "- " + "_".join(column) + "\n"

        value += f"\nNumber of rows: {len(self.rows)}\n"
        if self.rows:
            value += "\nRow example:\n"
            value += json.dumps(self.rows[0], indent=4)

        value += "\n\n"

        return value

    def merge(self, other: "DictTable"):
        self.update_columns(other.columns)
        self.rows.extend(other.rows)

    def get_data(self):
        all_rows = []
        for row in self.rows:
            new_row = []
            for column in self.columns:
                new_row.append(self.access_nested_key(row, column, True))

            if self.name == "weather_root":
                print(new_row)
            all_rows.append(new_row)

        return all_rows

    def get_schema(self) -> str:
        column_type = ""
        columns = []
        for keys in self.columns:
            for row in self.rows:
                value = self.access_nested_key(row, keys, True)
                if value is None:
                    continue
                elif isinstance(value, str):
                    column_type = "string"
                    break
                elif isinstance(value, (int, float)):
                    column_type = "string"
                    break
                else:
                    raise ValueError(
                        f"Found bad type ({type(value)}) inferring type of {row=}"
                    )
            columns.append(f"{'_'.join(keys)}: {column_type}")
        return ", ".join(columns)

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

        for k in nested_keys:
            if (k not in value) and safe_return:
                return None
            value = value[k]

        return str(value)
