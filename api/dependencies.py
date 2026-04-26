from functools import lru_cache
from .config import Settings
from .data import DataStore, get_data_store


@lru_cache
def get_settings() -> Settings:
    """Return cached Settings instance."""
    return Settings()


def get_data() -> DataStore:
    """Return DataStore for dependency injection."""
    return get_data_store()
