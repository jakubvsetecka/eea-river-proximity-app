from typing import Any
from pydantic import BaseModel, Field


class FacilityListItem(BaseModel):
    """Lightweight facility for list responses (no geometries)."""
    id: int
    facility_idx: int | None = None
    facility_name: str = Field(alias="facilityName")
    city: str | None = None
    country_name: str | None = Field(None, alias="countryName")
    sector_name: str | None = Field(None, alias="EPRTR_SectorName")
    longitude: float
    latitude: float
    closest_river_id: int | None = None
    distance_to_river_m: float | None = None
    n_upstream: int | None = None
    n_downstream: int | None = None
    has_sentinel_visible_river: bool = False

    class Config:
        populate_by_name = True


class FacilityDetail(BaseModel):
    """Full facility detail with decoded GeoJSON geometries."""
    id: int
    facility_name: str = Field(alias="facilityName")
    city: str | None = None
    country_name: str | None = Field(None, alias="countryName")
    sector_name: str | None = Field(None, alias="EPRTR_SectorName")
    pollutant: str | None = Field(None, alias="Pollutant")
    releases: float | None = Field(None, alias="Releases")
    longitude: float
    latitude: float
    location: dict[str, Any]  # GeoJSON Point
    closest_river_id: int | None = None
    distance_to_river_m: float | None = None
    river_strahler: int | None = None
    river_discharge: float | None = None
    n_upstream: int | None = None
    n_downstream: int | None = None
    upstream_line: dict[str, Any] | None = None  # GeoJSON LineString/MultiLineString
    downstream_line: dict[str, Any] | None = None
    upstream_poly: dict[str, Any] | None = None  # GeoJSON Polygon/MultiPolygon
    downstream_poly: dict[str, Any] | None = None

    class Config:
        populate_by_name = True


class PaginationMeta(BaseModel):
    """Pagination metadata."""
    page: int
    page_size: int
    total: int
    total_pages: int


class FacilitiesListResponse(BaseModel):
    """Paginated facilities response."""
    data: list[FacilityListItem]
    pagination: PaginationMeta


class SegmentItem(BaseModel):
    """River segment."""
    hyriv_id: int = Field(alias="HYRIV_ID")
    direction: str
    geometry: dict[str, Any]  # GeoJSON

    class Config:
        populate_by_name = True


class SegmentsListResponse(BaseModel):
    """Segments response."""
    data: list[SegmentItem]
    total: int


class StatsResponse(BaseModel):
    """Dataset statistics."""
    total_facilities: int
    total_segments: int
    countries: list[str]
    country_counts: dict[str, int]
    sector_counts: dict[str, int]


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    facilities_loaded: int
    segments_loaded: int
