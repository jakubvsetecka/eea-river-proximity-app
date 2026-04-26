from typing import Annotated
from fastapi import APIRouter, Depends

from ..dependencies import get_data
from ..data import DataStore
from ..models import StatsResponse

router = APIRouter(prefix="/stats", tags=["stats"])


@router.get("", response_model=StatsResponse)
def get_stats(
    data: Annotated[DataStore, Depends(get_data)],
):
    """Return pre-computed dataset statistics."""
    return StatsResponse(**data.stats)
