from datetime import date
import json
from pathlib import Path

from src.destinations.base_destination import BaseDestination
from src.utils import Batch


class LocalDirectory(BaseDestination):

    def __init__(self, out_dir: str | Path) -> None:
        super().__init__()

        if isinstance(out_dir, str):
            out_dir = Path(out_dir)

        self.out_dir: Path = out_dir
        self.name = f"Local Directory ({self.out_dir})"

        if not self.out_dir.exists():

            self.print("Creating out directory")
            self.out_dir.mkdir()

    def save_batch(self, batch: Batch, out_file_path: Path):
        full_local_path = self.out_dir / out_file_path
        local_parent = full_local_path.parent
        if not local_parent.exists():
            local_parent.mkdir(exist_ok=True, parents=True)

        with (self.out_dir / out_file_path).open("w") as f:
            json.dump(batch, f, indent=4)

    def get_last_date_saved(self) -> dict[str, date]:
        self.print(
            "`get_last_date_saved` is not implemented, returning a placeholder value"
        )
        return {}

    def clean_up(self):
        if self.out_dir.exists():
            for dir in self.out_dir.iterdir():
                for nested_dir in dir.iterdir():
                    self._safe_rmdir(nested_dir)
                self._safe_rmdir(dir)
            self._safe_rmdir(self.out_dir)

    @staticmethod
    def _safe_rmdir(dir: Path):
        try:
            dir.rmdir()
        except OSError:
            pass
