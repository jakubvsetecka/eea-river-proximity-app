from typing import Any
from shapely import wkb
from shapely.geometry import mapping


def wkb_to_geojson(wkb_bytes: bytes | None) -> dict[str, Any] | None:
    """Convert WKB bytes to GeoJSON dict."""
    if wkb_bytes is None:
        return None
    try:
        geom = wkb.loads(wkb_bytes)
        return mapping(geom)
    except Exception:
        return None


def point_to_geojson(lon: float, lat: float) -> dict[str, Any]:
    """Create GeoJSON Point from longitude/latitude."""
    return {
        "type": "Point",
        "coordinates": [lon, lat]
    }
