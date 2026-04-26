from dataclasses import dataclass, field
from functools import lru_cache
import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
from shapely import STRtree
import numpy as np
from typing import Dict, List, Optional
from pathlib import Path

from .config import Settings


@dataclass
class DataStore:
    """Container for loaded data and spatial index."""
    facilities: gpd.GeoDataFrame
    segments: gpd.GeoDataFrame
    facility_tree: STRtree
    stats: dict
    # Anomaly data
    events: pd.DataFrame
    canonical_map: Dict[int, int]  # facility_idx -> canonical_facility_id
    canonical_reverse: Dict[int, List[int]]  # canonical_id -> [facility_idx, ...]
    events_by_facility: Dict[int, List[dict]]  # canonical_id -> [events]
    # Bins data
    bins_path: Path
    bins_dates: List[str] = field(default_factory=list)  # sorted list of available dates
    bins_by_date: Dict[str, Dict[int, tuple]] = field(default_factory=dict)  # date -> {facility_id -> (z_ndci, z_turb)}


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

    # Load anomaly events (small file, ~42 rows)
    print(f"Loading anomaly events from {settings.events_path}...")
    events = pd.read_parquet(settings.events_path)
    print(f"Loaded {len(events):,} anomaly events")

    # Load canonical map
    print(f"Loading canonical map from {settings.canonical_map_path}...")
    canonical_df = pd.read_parquet(settings.canonical_map_path)
    canonical_map = dict(zip(canonical_df['facility_idx'], canonical_df['canonical_facility_id']))
    # Build reverse map
    canonical_reverse: Dict[int, List[int]] = {}
    for idx, cid in canonical_map.items():
        if cid not in canonical_reverse:
            canonical_reverse[cid] = []
        canonical_reverse[cid].append(idx)
    print(f"Built canonical map with {len(canonical_map):,} entries")

    # Build facility_idx lookup for joining events with facility info
    facility_idx_col = facilities.get('facility_idx')
    if facility_idx_col is not None:
        facility_by_idx = {int(idx): i for i, idx in enumerate(facility_idx_col) if pd.notna(idx)}
    else:
        facility_by_idx = {i: i for i in range(len(facilities))}

    # Build events by facility index (using canonical IDs) with facility info
    events_by_facility: Dict[int, List[dict]] = {}
    for _, row in events.iterrows():
        fid = int(row['facility_id'])
        cid = canonical_map.get(fid, fid)

        # Look up facility info
        fac_row_idx = facility_by_idx.get(fid)
        if fac_row_idx is not None and fac_row_idx < len(facilities):
            fac = facilities.iloc[fac_row_idx]
            fac_name = fac.get('facilityName', '')
            fac_city = fac.get('city', '')
            fac_country = fac.get('countryName', '')
            fac_lat = float(fac.geometry.y) if hasattr(fac, 'geometry') else None
            fac_lon = float(fac.geometry.x) if hasattr(fac, 'geometry') else None
        else:
            fac_name, fac_city, fac_country, fac_lat, fac_lon = None, None, None, None, None

        event_dict = {
            'event_id': row['event_id'],
            'facility_id': fid,
            'canonical_id': cid,
            'start_date': row['start_date'].isoformat() if pd.notna(row['start_date']) else None,
            'end_date': row['end_date'].isoformat() if pd.notna(row['end_date']) else None,
            'duration_bins': int(row['duration_bins']) if pd.notna(row['duration_bins']) else None,
            'peak_z_ndci': float(row['peak_z_ndci']) if pd.notna(row['peak_z_ndci']) else None,
            'peak_z_turb': float(row['peak_z_turb']) if pd.notna(row['peak_z_turb']) else None,
            'signal_type': row['signal_type'],
            'facilityName': fac_name,
            'city': fac_city,
            'countryName': fac_country,
            'latitude': fac_lat,
            'longitude': fac_lon,
        }
        if cid not in events_by_facility:
            events_by_facility[cid] = []
        events_by_facility[cid].append(event_dict)
    print(f"Indexed events for {len(events_by_facility):,} facilities")

    # Load bins data and build date index for severity calculations
    print(f"Loading bins data from {settings.bins_path}...")
    bins_df = pd.read_parquet(settings.bins_path, columns=['facility_id', 'date', 'z_delta_ndci', 'z_delta_turb'])
    print(f"Loaded {len(bins_df):,} bin records")

    # Build index: date -> {facility_id -> (z_ndci, z_turb)}
    print("Building bins date index...")
    bins_by_date: Dict[str, Dict[int, tuple]] = {}
    for _, row in bins_df.iterrows():
        date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
        fid = int(row['facility_id'])
        z_ndci = float(row['z_delta_ndci']) if pd.notna(row['z_delta_ndci']) else None
        z_turb = float(row['z_delta_turb']) if pd.notna(row['z_delta_turb']) else None
        if date_str not in bins_by_date:
            bins_by_date[date_str] = {}
        bins_by_date[date_str][fid] = (z_ndci, z_turb)

    bins_dates = sorted(bins_by_date.keys())
    print(f"Indexed {len(bins_dates)} dates with z-delta data")
    del bins_df  # Free memory

    # Pre-compute statistics
    print("Computing statistics...")
    country_counts = facilities['countryName'].value_counts().to_dict()
    sector_counts = facilities['EPRTR_SectorName'].value_counts().to_dict()

    stats = {
        "total_facilities": len(facilities),
        "total_segments": len(segments),
        "total_events": len(events),
        "facilities_with_events": len(events_by_facility),
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
        events=events,
        canonical_map=canonical_map,
        canonical_reverse=canonical_reverse,
        events_by_facility=events_by_facility,
        bins_path=settings.bins_path,
        bins_dates=bins_dates,
        bins_by_date=bins_by_date,
    )


def get_facility_bins(store: DataStore, facility_id: int) -> pd.DataFrame:
    """Load bins data for a specific facility (lazy loading)."""
    # Use canonical ID to get all related facility indices
    canonical_id = store.canonical_map.get(facility_id, facility_id)
    related_ids = store.canonical_reverse.get(canonical_id, [facility_id])

    # Read only the rows for this facility using row group filtering
    df = pd.read_parquet(
        store.bins_path,
        filters=[('facility_id', 'in', related_ids)]
    )
    return df


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
