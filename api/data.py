from dataclasses import dataclass
from functools import lru_cache
import geopandas as gpd
from shapely import STRtree
import numpy as np

from .config import Settings


@dataclass
class DataStore:
    """Container for loaded data and spatial index."""
    facilities: gpd.GeoDataFrame
    segments: gpd.GeoDataFrame
    facility_tree: STRtree
    stats: dict


def load_data(settings: Settings) -> DataStore:
    """Load geoparquet files and build spatial index."""
    print(f"Loading facilities from {settings.facilities_path}...")
    facilities = gpd.read_parquet(settings.facilities_path)
    print(f"Loaded {len(facilities):,} facilities")

    print(f"Loading segments from {settings.segments_path}...")
    segments = gpd.read_parquet(settings.segments_path)
    print(f"Loaded {len(segments):,} segments")

    # Build spatial index for facilities
    print("Building spatial index...")
    facility_tree = STRtree(facilities.geometry.values)

    # Pre-compute statistics
    print("Computing statistics...")
    country_counts = facilities['countryName'].value_counts().to_dict()
    sector_counts = facilities['EPRTR_SectorName'].value_counts().to_dict()

    stats = {
        "total_facilities": len(facilities),
        "total_segments": len(segments),
        "countries": sorted([c for c in country_counts.keys() if c]),
        "country_counts": country_counts,
        "sector_counts": sector_counts,
    }

    print("Data loading complete!")
    return DataStore(
        facilities=facilities,
        segments=segments,
        facility_tree=facility_tree,
        stats=stats,
    )


_data_store: DataStore | None = None


def get_data_store() -> DataStore:
    """Return the singleton DataStore instance."""
    if _data_store is None:
        raise RuntimeError("DataStore not initialized. Call init_data_store() first.")
    return _data_store


def init_data_store(settings: Settings) -> DataStore:
    """Initialize the global DataStore."""
    global _data_store
    _data_store = load_data(settings)
    return _data_store
