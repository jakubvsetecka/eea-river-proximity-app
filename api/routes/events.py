from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime

from ..data import get_data_store, get_facility_bins

router = APIRouter(prefix="/events", tags=["events"])


class AnomalyEvent(BaseModel):
    event_id: str
    facility_id: int
    canonical_id: int
    start_date: Optional[str]
    end_date: Optional[str]
    duration_bins: Optional[int]
    peak_z_ndci: Optional[float]
    peak_z_turb: Optional[float]
    signal_type: Optional[str]
    # Facility info for map display
    facilityName: Optional[str] = None
    city: Optional[str] = None
    countryName: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


class EventsResponse(BaseModel):
    data: List[AnomalyEvent]
    total: int


class FacilityBin(BaseModel):
    date: str
    delta_ndci: Optional[float]
    delta_turb: Optional[float]
    z_delta_ndci: Optional[float]
    z_delta_turb: Optional[float]
    any_anomaly: bool
    event_key: Optional[str]


class BinsResponse(BaseModel):
    facility_id: int
    canonical_id: int
    data: List[FacilityBin]
    total: int


@router.get("", response_model=EventsResponse)
def list_events(
    facility_id: Optional[int] = Query(None, description="Filter by facility ID"),
    signal_type: Optional[str] = Query(None, description="Filter by signal type"),
):
    """List all anomaly events, optionally filtered by facility."""
    store = get_data_store()

    if facility_id is not None:
        canonical_id = store.canonical_map.get(facility_id, facility_id)
        events = store.events_by_facility.get(canonical_id, [])
    else:
        # Return all events
        events = []
        for ev_list in store.events_by_facility.values():
            events.extend(ev_list)

    if signal_type:
        events = [e for e in events if e.get('signal_type') == signal_type]

    return EventsResponse(data=events, total=len(events))


@router.get("/summary")
def events_summary():
    """Get summary statistics about anomaly events."""
    store = get_data_store()

    all_events = []
    for ev_list in store.events_by_facility.values():
        all_events.extend(ev_list)

    signal_types = {}
    for e in all_events:
        st = e.get('signal_type', 'unknown')
        signal_types[st] = signal_types.get(st, 0) + 1

    return {
        "total_events": len(all_events),
        "facilities_with_events": len(store.events_by_facility),
        "signal_types": signal_types,
    }


@router.get("/facility/{facility_id}", response_model=EventsResponse)
def get_facility_events(facility_id: int):
    """Get all anomaly events for a specific facility."""
    store = get_data_store()
    canonical_id = store.canonical_map.get(facility_id, facility_id)
    events = store.events_by_facility.get(canonical_id, [])
    return EventsResponse(data=events, total=len(events))


@router.get("/bins/{facility_id}", response_model=BinsResponse)
def get_facility_bins_endpoint(facility_id: int):
    """Get time series bin data for a specific facility."""
    store = get_data_store()
    canonical_id = store.canonical_map.get(facility_id, facility_id)

    try:
        df = get_facility_bins(store, facility_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading bins: {str(e)}")

    if df.empty:
        return BinsResponse(facility_id=facility_id, canonical_id=canonical_id, data=[], total=0)

    # Sort by date
    df = df.sort_values('date')

    bins = []
    for _, row in df.iterrows():
        bins.append(FacilityBin(
            date=row['date'].isoformat() if hasattr(row['date'], 'isoformat') else str(row['date']),
            delta_ndci=float(row['delta_ndci']) if pd.notna(row.get('delta_ndci')) else None,
            delta_turb=float(row['delta_turb']) if pd.notna(row.get('delta_turb')) else None,
            z_delta_ndci=float(row['z_delta_ndci']) if pd.notna(row.get('z_delta_ndci')) else None,
            z_delta_turb=float(row['z_delta_turb']) if pd.notna(row.get('z_delta_turb')) else None,
            any_anomaly=bool(row.get('any_anomaly', False)),
            event_key=row.get('event_key') if pd.notna(row.get('event_key')) else None,
        ))

    return BinsResponse(
        facility_id=facility_id,
        canonical_id=canonical_id,
        data=bins,
        total=len(bins)
    )


# Need pandas for notna check
import pandas as pd
from bisect import bisect_left


class SeverityData(BaseModel):
    facility_id: int
    z_ndci: Optional[float]
    z_turb: Optional[float]
    severity: int  # 1-5


class SeverityResponse(BaseModel):
    date: str
    data: dict  # facility_id -> severity


@router.get("/severity")
def get_severity_by_date(
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
):
    """Get z-delta severity for all facilities at a given date."""
    store = get_data_store()

    # Find closest available date
    if not store.bins_dates:
        return SeverityResponse(date=date, data={})

    # Binary search for closest date
    idx = bisect_left(store.bins_dates, date)
    if idx == 0:
        closest_date = store.bins_dates[0]
    elif idx == len(store.bins_dates):
        closest_date = store.bins_dates[-1]
    else:
        before = store.bins_dates[idx - 1]
        after = store.bins_dates[idx]
        # Pick whichever is closer
        closest_date = before if abs(pd.Timestamp(date) - pd.Timestamp(before)) <= abs(pd.Timestamp(date) - pd.Timestamp(after)) else after

    # Get z-deltas for that date
    date_data = store.bins_by_date.get(closest_date, {})

    # Calculate severity for each facility
    def calc_severity(z_ndci, z_turb):
        max_z = max(abs(z_ndci or 0), abs(z_turb or 0))
        if max_z >= 5:
            return 5  # Critical
        if max_z >= 4:
            return 4  # Alert
        if max_z >= 3:
            return 3  # Warning
        if max_z >= 2:
            return 2  # Elevated
        return 1  # Normal

    result = {}
    for fid, (z_ndci, z_turb) in date_data.items():
        result[fid] = calc_severity(z_ndci, z_turb)

    return {"date": closest_date, "data": result}
