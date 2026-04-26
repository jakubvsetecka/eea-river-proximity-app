# EEA Industrial Emissions - River Proximity Dataset

This dataset matches **160,576 industrial facilities** from the European Environment Agency (EEA) Industrial Emissions database to nearby river segments from HydroRIVERS, with upstream/downstream flow analysis.

## Dataset Description

For each facility within 1km of a river:
- **Upstream segments**: River parts flowing *toward* the facility (potential source water)
- **Downstream segments**: River parts flowing *away* from the facility (potentially affected by emissions)

The split point is the closest point on the river to each facility.

## Files

### `river_data_facilities.geoparquet`
Main dataset with 160,576 facilities and their river associations.

| Column | Description |
|--------|-------------|
| `facilityName` | Name of the industrial facility |
| `city`, `countryName` | Location |
| `EPRTR_SectorCode/Name` | Industry sector |
| `Pollutant`, `Releases` | Emission data |
| `closest_river_id` | HydroRIVERS segment ID |
| `distance_to_river_m` | Distance to nearest river (meters) |
| `river_strahler` | Strahler stream order |
| `river_discharge` | Average discharge (m³/s) |
| `upstream_segment_ids` | List of upstream HydroRIVERS IDs |
| `downstream_segment_ids` | List of downstream HydroRIVERS IDs |
| `n_upstream`, `n_downstream` | Count of segments |
| `upstream_line_wkb`, `downstream_line_wkb` | River line geometries (WKB) |
| `upstream_poly_wkb`, `downstream_poly_wkb` | Water surface polygons (WKB) |
| `geometry` | Facility point location |

### `river_data_segments.geoparquet`
28,434 river segment geometries with direction labels.

| Column | Description |
|--------|-------------|
| `HYRIV_ID` | HydroRIVERS segment ID |
| `direction` | "upstream" or "downstream" |
| `ORD_STRA` | Strahler stream order |
| `DIS_AV_CMS` | Average discharge (m³/s) |
| `LENGTH_KM` | Segment length (km) |
| `geometry` | LineString geometry |

## Usage

```python
import geopandas as gpd
from shapely import wkb

# Load facilities
facilities = gpd.read_parquet("river_data_facilities.geoparquet")

# Get a facility's upstream river geometry
facility = facilities[facilities['facilityName'].str.contains('PRECHEZA')].iloc[0]
upstream_line = wkb.loads(facility['upstream_line_wkb'])
downstream_line = wkb.loads(facility['downstream_line_wkb'])
```

## Visualization Scripts

- `visualize_single_facility.py` - Interactive map for a single facility
- `visualize_facilities_rivers.py` - Overview map with sampled facilities

## Source Data

- **Facilities**: [EEA Industrial Emissions Database](https://www.eea.europa.eu/data-and-maps/data/industrial-reporting-under-the-industrial-6)
- **Rivers**: [HydroRIVERS v1.0](https://www.hydrosheds.org/products/hydrorivers)
- **Water Polygons**: [EU-Hydro River Network Database](https://land.copernicus.eu/imagery-in-situ/eu-hydro)

## Parameters Used

- Max distance to river: 1,000m
- Upstream trace distance: 10km
- Downstream trace distance: 10km
- Polygon buffer: 600m

## License

The derived dataset follows the licenses of the source datasets. See original sources for details.
