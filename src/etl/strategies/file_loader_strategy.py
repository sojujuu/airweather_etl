from abc import ABC, abstractmethod
import os
from typing import Tuple

class FileLoaderStrategy(ABC):
    @abstractmethod
    def get_filenames(self) -> Tuple[str,str]:
        """Return (weather_csv_name, ispu_csv_name) inside INCOMING dir"""
        ...

class JakartaFileLoader(FileLoaderStrategy):
    def __init__(self, city_slug: str = "jakarta"):
        self.city_slug = city_slug.lower()

    def get_filenames(self) -> Tuple[str,str]:
        return (f"cuaca_harian_{self.city_slug}.csv", f"ispu_harian_{self.city_slug}.csv")
