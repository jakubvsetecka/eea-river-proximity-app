# Hydroscope — River Pollution Monitoring

> **11th CASSINI Hackathon — EU Space for Water (Vienna, 24–26 April 2026)**
> A satellite-based triage tool that flags likely industrial pollution sources on European rivers, cross-referenced against the EEA industrial emissions registry.

Hydroscope helps environmental inspectorates and river basin authorities answer one question: *"Which industrial facility should we inspect first this week?"* It joins Sentinel-2-derived water-quality time series to the ~160 k facilities in the EEA Industrial Emissions Directive (E-PRTR) registry, ranks anomalous segments, and lets a user explore the upstream/downstream river network around each candidate.

## Demo & Repos

- **App (this repo):** https://github.com/jakubvsetecka/eea-river-proximity-app
- **Research / exploration repo:** https://github.com/jakubvsetecka/EU-Space-for-Water
- **Hackathon page:** https://taikai.network/cassinihackathons/hackathons/space-for-water
- **Hugging Face org:** https://huggingface.co/cassini-team-todo

## What it does

- Loads ~160 k EEA-reported industrial facilities and ~28 k associated HydroRIVERS segments.
- Splits each facility's nearest river into an upstream (potential source water) and downstream (potentially affected) trace.
- Pulls Sentinel-2 statistics over the river surface around each facility and computes per-bin water-quality indices.
- Detects anomalies on the resulting time series and surfaces them as map markers, KPIs, and timeline histograms.
- Lets a user filter by country, sector, pollutant, or bbox and inspect a single facility's full upstream/downstream geometry and history.

Headline UI features: facility map with clustering, anomaly markers, timeline view with delta-distribution histograms, KPI panel with anomaly indicators.

## Repo layout

```
api/         FastAPI backend (routes: facilities, segments, stats, health)
frontend/    Single-page web client (DuckDB WASM querying parquet directly)
data/        Sentinel statistics cache (sharded JSON)
*.py         Standalone scripts: river_proximity matching + visualizations
```

## Datasets (Hugging Face — `cassini-team-todo`)

All datasets used or produced by the project are mirrored under the [`cassini-team-todo`](https://huggingface.co/cassini-team-todo) Hugging Face org.

| Dataset | What's in it |
|---|---|
| [`eea-river-proximity`](https://huggingface.co/datasets/cassini-team-todo/eea-river-proximity) | **Primary dataset for this app.** 160,576 facilities matched to nearby rivers with upstream/downstream traces, plus per-facility Sentinel-2 time series, anomaly events, and per-bin anomaly tables. |
| [`eea-industrial-emissions`](https://huggingface.co/datasets/cassini-team-todo/eea-industrial-emissions) | EEA Industrial Emissions Directive (IED / E-PRTR) — site map and facility clusters as GeoPackages plus the original tabular CSVs. |
| [`hydro-rivers-europe`](https://huggingface.co/datasets/cassini-team-todo/hydro-rivers-europe) | HydroRIVERS v1.0 Europe — original ESRI shapefile and a GeoParquet export used for routing. |
| [`eu-hydro-master-skeleton`](https://huggingface.co/datasets/cassini-team-todo/eu-hydro-master-skeleton) | Copernicus EU-Hydro v1.3 — per-basin GeoParquet shards: river centerlines, river-surface polygons, inland water polygons, river basins. |
| [`eea-waterbase`](https://huggingface.co/datasets/cassini-team-todo/eea-waterbase) | EEA Waterbase – Water Quality ICM (WISE-4) v2018.1 — station-level chemistry/biology measurements + monitoring-site registry. |
| [`eea-waterbase-cleaned`](https://huggingface.co/datasets/cassini-team-todo/eea-waterbase-cleaned) | Cleaned WISE-4 subset (rivers only, 2015-07–2017-12, valid coords) prepared for training Sentinel-2 water-quality models. |

## Source data & EU space assets

- **Copernicus Sentinel-2** (10 m multispectral) — water-quality indices (NDWI, NDVI, SABI, CGI, CDOM, DOC, Cyanobacteria, Turbidity). https://dataspace.copernicus.eu/
- **Copernicus EU-Hydro v1.3** — river network and water-surface polygons. https://land.copernicus.eu/imagery-in-situ/eu-hydro
- **EEA Industrial Emissions Database (E-PRTR / IED)** — facility registry and pollutant releases. https://www.eea.europa.eu/data-and-maps/data/industrial-reporting-under-the-industrial-6
- **EEA Waterbase WISE-4** — in-situ water-quality measurements. https://www.eea.europa.eu/data-and-maps/data/waterbase-water-quality-icm-2
- **HydroRIVERS v1.0** — global river network for upstream/downstream tracing. https://www.hydrosheds.org/products/hydrorivers

## Running locally

Backend:

```bash
pip install -r requirements.txt
uvicorn api.main:app --reload
```

The API expects `river_data_facilities.geoparquet` and `river_data_segments.geoparquet` (from the [`eea-river-proximity`](https://huggingface.co/datasets/cassini-team-todo/eea-river-proximity) dataset) at the repo root. Override paths with `EEA_*` env vars (see `api/config.py`).

Frontend: `frontend/index.html` is a static page that can also run **without the backend** — it queries parquet files directly from Hugging Face via DuckDB WASM.

## Scripts

- `river_proximity.py` — match facilities to rivers, compute upstream/downstream segment IDs and geometries.
- `visualize_single_facility.py` — interactive map for one facility.
- `visualize_facilities_rivers.py` — overview map across sampled facilities.

## Files in the river-proximity dataset

The [`eea-river-proximity`](https://huggingface.co/datasets/cassini-team-todo/eea-river-proximity) dataset (3.22 GB total) is what this app reads. Six artifacts, produced by the pipeline below:

| File | Size | What it is |
|---|---|---|
| `river_data_facilities.geoparquet` | 2.62 GB | 160,576 facilities matched to rivers, with upstream/downstream WKB geometries and Sentinel-visibility flags. One row per facility. |
| `river_data_segments.geoparquet` | 1.66 MB | 28,434 river segments (`HYRIV_ID`, `direction`, `ORD_STRA`, `DIS_AV_CMS`, `LENGTH_KM`, `geometry`). |
| `facility_timeseries.parquet` | 210 MB | Sentinel-2 NDCI / turbidity / NDWI per facility, per direction, in P10D bins, 2017-01 → 2023-12. 9.2 M rows · 23,158 facilities · 20,755 paired (both directions). |
| `facility_anomalies_per_bin.parquet` | 216 MB | Per-bin anomaly output for paired facilities. 4.26 M rows · 20,755 facilities · 255 dates. |
| `facility_anomalies_events.parquet` | 11.8 kB | Consolidated pollution events after deduplication and persistence filtering. **42 events · 36 unique upstream/downstream polygon pairs · 2017–2023.** |
| `facility_canonical_map.parquet` | 2.1 MB | Maps every `facility_idx` to the `canonical_facility_id` whose polygon-pair carries the bin data (deduplicates facilities sharing the same river polygons). |
| `sentinel_cache/` | — | Sharded raw responses from the Sentinel Hub Statistical API. |

### Pipeline

1. **River matching** (`river_proximity.py`) — for each facility, find the closest HydroRIVERS segment within 1 km, split it at the closest point, BFS upstream 10 km, walk downstream 10 km, clip EU-Hydro water-surface polygons to match.
2. **Sentinel visibility flag** — overlap each HydroRIVERS segment with the EU-Hydro `River_Net_p` polygon (± 30 m buffer); a segment is "Sentinel-visible" if ≥ 30 % of its length falls inside a water polygon. **59,568 / 160,576 facilities (37.1 %)** have at least one visible segment.
3. **Sentinel-2 retrieval** — for visible polygons, query Sentinel Hub Statistical API in 10-day bins over 2017–2023. Compute per-bin mean / std / valid-pixel count for NDCI, turbidity, NDWI.
4. **Anomaly detection** (`scripts/compute_facility_anomalies.py`):
   - Inner-join upstream + downstream on `(facility_id, date)`.
   - Raw delta = downstream − upstream.
   - **Spatial detrend**: subtract the cross-facility median delta per date (removes regional Sentinel-2 artifacts).
   - **Robust z-score** per `(facility_id, quarter)` using median + MAD × 1.4826.
   - **Persistence**: require previous-bin z > 1.5.
   - **High-confidence flag**: detrended delta > 0 ∧ z > 3 ∧ prev-z > 1.5.
5. **Event consolidation** — merge consecutive flagged bins, drop single-bin events, dedupe by polygon pair → **42 events**.

### Key columns

`river_data_facilities.geoparquet` adds Sentinel-visibility columns on top of the basic facility/river fields:

| Column | Description |
|--------|-------------|
| `facilityName`, `city`, `countryName`, `EPRTR_SectorCode/Name`, `Pollutant`, `Releases` | Facility identity, sector, emissions |
| `closest_river_id`, `distance_to_river_m`, `river_strahler`, `river_discharge` | Closest HydroRIVERS segment + attributes |
| `upstream_segment_ids`, `downstream_segment_ids`, `n_upstream`, `n_downstream` | Traced segment IDs |
| `upstream_line_wkb`, `downstream_line_wkb`, `upstream_poly_wkb`, `downstream_poly_wkb` | Geometries (WKB) |
| `closest_river_overlap_fraction` | Fraction of closest segment inside an EU-Hydro water polygon |
| `closest_river_is_sentinel_visible` | True if overlap ≥ 0.30 |
| `n_upstream_sentinel_visible`, `n_downstream_sentinel_visible` | Counts of visible traced segments |
| `has_sentinel_visible_river` | **Summary flag** — use to filter to facilities where Sentinel-2 retrieval is feasible |

`facility_anomalies_per_bin.parquet` carries the full anomaly pipeline output: paired NDCI/turbidity means, `delta_*_raw`, `date_median_*` (subtracted artifact), detrended `delta_*`, seasonal `baseline_med_*` / `baseline_mad_*`, robust `z_delta_*` and `z_delta_*_prev`, `high_confidence_ndci`/`_turb`, `any_anomaly`, and `event_key` linking back to the events table.

`facility_anomalies_events.parquet` collapses to one row per polygon-pair × time-window with `start_date`, `end_date`, `duration_bins`, `peak_z_*`, `mean_z_*`, `signal_type` (`"ndci"` / `"turb"` / `"both"`).

```python
import geopandas as gpd
import pandas as pd
from shapely import wkb

facilities = gpd.read_parquet("river_data_facilities.geoparquet")
visible = facilities[facilities["has_sentinel_visible_river"]]      # 59,568 rows

ts = pd.read_parquet("facility_timeseries.parquet")
events = pd.read_parquet("facility_anomalies_events.parquet")        # 42 events
```

### Sentinel visibility by Strahler order

| Strahler | Facilities | % Sentinel-visible |
|---|---:|---:|
| 1 | 43,699 | 22.5 % |
| 2 | 28,113 | 20.5 % |
| 3 | 31,616 | 23.9 % |
| 4 | 24,961 | 42.5 % |
| 5 | 15,709 | 67.8 % |
| 6 |  8,128 | 86.5 % |
| 7 |  8,131 | 97.5 % |
| 8 |    219 | 100.0 % |

### Pipeline parameters

| Parameter | Value |
|---|---|
| Max facility–river distance | 1,000 m |
| Upstream / downstream trace distance | 10 km each |
| Polygon buffer | 600 m |
| Sentinel visibility threshold | overlap ≥ 30 % with EU-Hydro `River_Net_p` (± 30 m) |
| Sentinel-2 time window | 2017-01-01 → 2023-12-16, 10-day bins |
| Indices | NDCI, turbidity proxy, NDWI |
| Anomaly thresholds | z > 3.0, prev-z > 1.5, ≥ 2 consecutive bins, detrended delta > 0, MAD baseline ≥ 8 bins/quarter |

## License

The app code is provided as-is for hackathon purposes. Derived datasets follow their source licenses (see each Hugging Face dataset card and the source links above).
