from typing import Annotated
from fastapi import APIRouter, Depends

from ..dependencies import get_data
from ..data import DataStore
from ..models import HealthResponse

router = APIRouter(prefix="/health", tags=["health"])


@router.get("", response_model=HealthResponse)
def health_check(
    data: Annotated[DataStore, Depends(get_data)],
):
    """Health check endpoint."""
    return HealthResponse(
        status="healthy",
        facilities_loaded=len(data.facilities),
        segments_loaded=len(data.segments),
    )
