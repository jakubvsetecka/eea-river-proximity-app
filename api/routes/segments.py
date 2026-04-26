from typing import Annotated
from fastapi import APIRouter, Depends, Query
from shapely.geometry import mapping
import numpy as np

from ..dependencies import get_data
from ..data import DataStore
from ..models import SegmentItem, SegmentsListResponse

router = APIRouter(prefix="/segments", tags=["segments"])


@router.get("", response_model=SegmentsListResponse)
def list_segments(
    data: Annotated[DataStore, Depends(get_data)],
    direction: Annotated[
        str | None,
        Query(description="Filter by direction: 'upstream' or 'downstream'")
    ] = None,
    ids: Annotated[
        str | None,
        Query(description="Comma-separated HYRIV_IDs to filter")
    ] = None,
):
    """List river segments with optional filtering."""
    df = data.segments

    mask = np.ones(len(df), dtype=bool)

    if direction:
        direction_lower = direction.lower()
        if direction_lower not in ('upstream', 'downstream'):
            # Return empty if invalid direction
            return SegmentsListResponse(data=[], total=0)
        mask &= (df['direction'] == direction_lower).values

    if ids:
        try:
            id_list = [int(x.strip()) for x in ids.split(",")]
            mask &= df['HYRIV_ID'].isin(id_list).values
        except ValueError:
            # Return empty if invalid IDs
            return SegmentsListResponse(data=[], total=0)

    filtered = df[mask]

    items = []
    for idx, row in filtered.iterrows():
        items.append(SegmentItem(
            HYRIV_ID=int(row['HYRIV_ID']),
            direction=row['direction'],
            geometry=mapping(row.geometry),
        ))

    return SegmentsListResponse(data=items, total=len(items))
