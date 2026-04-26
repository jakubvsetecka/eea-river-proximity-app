from typing import Annotated, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from shapely.geometry import box
import numpy as np
import pandas as pd

from ..dependencies import get_data, get_settings
from ..data import DataStore
from ..config import Settings
from ..models import (
    FacilityListItem,
    FacilityDetail,
    FacilitiesListResponse,
    PaginationMeta,
)
from ..utils.geometry import wkb_to_geojson, point_to_geojson

router = APIRouter(prefix="/facilities", tags=["facilities"])


def safe_get(row, key: str, default: Any = None) -> Any:
    """Get value from row, converting NaN/NaT to None."""
    val = row.get(key, default)
    if val is None or (isinstance(val, float) and np.isnan(val)) or pd.isna(val):
        return None
    return val


def safe_int(row, key: str) -> int | None:
    """Get integer value, converting NaN to None."""
    val = safe_get(row, key)
    if val is None:
        return None
    return int(val)


def safe_float(row, key: str) -> float | None:
    """Get float value, converting NaN to None."""
    val = safe_get(row, key)
    if val is None:
        return None
    return float(val)


@router.get("", response_model=FacilitiesListResponse)
def list_facilities(
    data: Annotated[DataStore, Depends(get_data)],
    settings: Annotated[Settings, Depends(get_settings)],
    search: Annotated[str | None, Query(description="Search facility name, city, or country")] = None,
    country: Annotated[str | None, Query(description="Filter by country name")] = None,
    sector: Annotated[str | None, Query(description="Filter by main activity name")] = None,
    bbox: Annotated[
        str | None,
        Query(description="Bounding box: minLon,minLat,maxLon,maxLat")
    ] = None,
    page: Annotated[int, Query(ge=1, description="Page number")] = 1,
    page_size: Annotated[int | None, Query(ge=1, description="Items per page")] = None,
):
    """List facilities with optional filtering and pagination."""
    if page_size is None:
        page_size = settings.default_page_size
    page_size = min(page_size, settings.max_page_size)

    df = data.facilities

    # Apply filters using boolean masks
    mask = np.ones(len(df), dtype=bool)

    if search:
        # Search across facility name, city, and country
        search_mask = (
            df['facilityName'].str.contains(search, case=False, na=False).values |
            df['city'].str.contains(search, case=False, na=False).values |
            df['countryName'].str.contains(search, case=False, na=False).values
        )
        mask &= search_mask

    if country:
        mask &= df['countryName'].str.contains(country, case=False, na=False).values

    if sector:
        mask &= df['EPRTR_SectorName'].str.contains(sector, case=False, na=False).values

    if bbox:
        try:
            coords = [float(x) for x in bbox.split(",")]
            if len(coords) != 4:
                raise ValueError("bbox must have 4 values")
            min_lon, min_lat, max_lon, max_lat = coords
            bbox_geom = box(min_lon, min_lat, max_lon, max_lat)
            # Use STRtree for efficient spatial query
            indices = data.facility_tree.query(bbox_geom)
            bbox_mask = np.zeros(len(df), dtype=bool)
            bbox_mask[indices] = True
            mask &= bbox_mask
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Invalid bbox: {e}")

    # Get filtered indices
    filtered_indices = np.where(mask)[0]
    total = len(filtered_indices)
    total_pages = (total + page_size - 1) // page_size if total > 0 else 1

    # Paginate
    start = (page - 1) * page_size
    end = start + page_size
    page_indices = filtered_indices[start:end]

    # Build response items
    items = []
    for idx in page_indices:
        row = df.iloc[idx]
        items.append(FacilityListItem(
            id=int(idx),
            facilityName=safe_get(row, 'facilityName', ''),
            city=safe_get(row, 'city'),
            countryName=safe_get(row, 'countryName'),
            EPRTR_SectorName=safe_get(row, 'EPRTR_SectorName'),
            longitude=float(row.geometry.x),
            latitude=float(row.geometry.y),
            closest_river_id=safe_int(row, 'closest_river_id'),
            distance_to_river_m=safe_float(row, 'distance_to_river_m'),
            n_upstream=safe_int(row, 'n_upstream'),
            n_downstream=safe_int(row, 'n_downstream'),
        ))

    return FacilitiesListResponse(
        data=items,
        pagination=PaginationMeta(
            page=page,
            page_size=page_size,
            total=total,
            total_pages=total_pages,
        ),
    )


@router.get("/{facility_id}", response_model=FacilityDetail)
def get_facility(
    facility_id: int,
    data: Annotated[DataStore, Depends(get_data)],
):
    """Get facility detail with decoded geometries."""
    df = data.facilities

    if facility_id < 0 or facility_id >= len(df):
        raise HTTPException(status_code=404, detail="Facility not found")

    row = df.iloc[facility_id]

    # Decode WKB geometries
    upstream_line = wkb_to_geojson(row.get('upstream_line_wkb'))
    downstream_line = wkb_to_geojson(row.get('downstream_line_wkb'))
    upstream_poly = wkb_to_geojson(row.get('upstream_poly_wkb'))
    downstream_poly = wkb_to_geojson(row.get('downstream_poly_wkb'))

    return FacilityDetail(
        id=facility_id,
        facilityName=safe_get(row, 'facilityName', ''),
        city=safe_get(row, 'city'),
        countryName=safe_get(row, 'countryName'),
        EPRTR_SectorName=safe_get(row, 'EPRTR_SectorName'),
        Pollutant=safe_get(row, 'Pollutant'),
        Releases=safe_float(row, 'Releases'),
        longitude=float(row.geometry.x),
        latitude=float(row.geometry.y),
        location=point_to_geojson(row.geometry.x, row.geometry.y),
        closest_river_id=safe_int(row, 'closest_river_id'),
        distance_to_river_m=safe_float(row, 'distance_to_river_m'),
        river_strahler=safe_int(row, 'river_strahler'),
        river_discharge=safe_float(row, 'river_discharge'),
        n_upstream=safe_int(row, 'n_upstream'),
        n_downstream=safe_int(row, 'n_downstream'),
        upstream_line=upstream_line,
        downstream_line=downstream_line,
        upstream_poly=upstream_poly,
        downstream_poly=downstream_poly,
    )
