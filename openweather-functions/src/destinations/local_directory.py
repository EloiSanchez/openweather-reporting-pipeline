from datetime import date
import json
from pathlib import Path
from typing import Generator

from src.destinations.base_destination import BaseDestination
from src.utils.types import Batch, Any


class LocalDirectory(BaseDestination):

    def __init__(self, dir: str | Path) -> None:
        super().__init__()

        if isinstance(dir, str):
            dir = Path(dir)

        self.dir: Path = dir
        self.name = f"Local Directory ({self.dir})"

        if not self.dir.exists():

            self.print("Creating out directory")
            self.dir.mkdir()

    def save_batch(self, batch: Batch, out_file_path: Path):
        full_local_path = self.dir / out_file_path
        local_parent = full_local_path.parent
        if not local_parent.exists():
            local_parent.mkdir(exist_ok=True, parents=True)

        with (self.dir / out_file_path).open("w") as f:
            json.dump(batch, f, indent=4)

    def get_last_date_saved(self) -> dict[str, date]:
        self.print(
            "`get_last_date_saved` is not implemented, returning a placeholder value"
        )
        return {}

    def iterate_file_data(
        self, dir: Path | str = "."
    ) -> Generator[tuple[str, list[dict[str, Any]]], None, None]:
        for date_dir in (self.dir / dir).iterdir():
            for data_file in date_dir.iterdir():
                path = data_file.resolve()
                with open(path, "r") as f:
                    yield str(path), json.load(f)

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
