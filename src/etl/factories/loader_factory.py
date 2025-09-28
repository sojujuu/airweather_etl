from . import __init__  # type: ignore # noqa: F401
from ..strategies.file_loader_strategy import JakartaFileLoader, FileLoaderStrategy

class LoaderFactory:
    @staticmethod
    def create(city_slug: str) -> FileLoaderStrategy:
        # Here we could switch among different cities/files in the future
        return JakartaFileLoader(city_slug=city_slug)
