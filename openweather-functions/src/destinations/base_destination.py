from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path

from src.utils import Batch


class BaseDestination(ABC):
    name: str

    @abstractmethod
    def save_batch(self, batch: Batch, out_file_path: Path): ...

    @abstractmethod
    def get_last_date_saved(self) -> dict[str, date]: ...

    def clean_up(self):
        pass

    def print(self, value: str):
        print(f"{self.name}: {value}")
