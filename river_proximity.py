"""
Match industrial facilities to nearby river segments with upstream/downstream split.

Optimized for batch processing:
1. Find closest rivers for all facilities (using spatial index)
2. Trace upstream/downstream segment IDs (fast lookups)
3. Batch build geometries at the end using lookup dicts
4. Batch clip polygons once at the end
"""

import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path
from shapely.ops import unary_union
from shapely.geometry import Point, LineString, MultiLineString
from shapely import STRtree
from collections import defaultdict
import sys

def log(msg):
    print(msg, flush=True)


def load_facilities(filepath: Path) -> gpd.GeoDataFrame:
    """Load water-releasing facilities and convert to GeoDataFrame."""
    log(f"Loading facilities from {filepath}...")
    df = pd.read_csv(filepath, low_memory=False)
    df = df.dropna(subset=["Longitude", "Latitude"])
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.Longitude, df.Latitude),
        crs="EPSG:4326"
    )
    log(f"Loaded {len(gdf):,} facilities with valid coordinates")
    return gdf


def load_rivers(filepath: Path, min_strahler_order: int = 1) -> gpd.GeoDataFrame:
    """Load HydroRIVERS data."""
    log(f"Loading HydroRIVERS from {filepath}...")
    gdf = gpd.read_parquet(filepath)
    if min_strahler_order > 1:
        original_count = len(gdf)
        gdf = gdf[gdf["ORD_STRA"] >= min_strahler_order].copy()
        log(f"Filtered rivers: {original_count:,} -> {len(gdf):,} (Strahler >= {min_strahler_order})")
    else:
        log(f"Loaded {len(gdf):,} river segments")
    return gdf


def load_river_polygons(polygon_dir: Path) -> gpd.GeoDataFrame:
    """Load all EU-Hydro river polygon shards."""
    log(f"Loading EU-Hydro river polygons from {polygon_dir}...")
    shards = sorted(polygon_dir.glob("*.geoparquet"))
    if not shards:
        raise FileNotFoundError(f"No geoparquet files found in {polygon_dir}")
    gdfs = [gpd.read_parquet(shard) for shard in shards]
    result = pd.concat(gdfs, ignore_index=True)
    result = gpd.GeoDataFrame(result, geometry="geometry", crs=gdfs[0].crs)
    log(f"Loaded {len(result):,} river polygons from {len(shards)} basin shards")
    return result


def build_river_network(rivers: gpd.GeoDataFrame) -> tuple[dict, dict, dict]:
    """Build lookup dictionaries for river network traversal."""
    log("Building river network graph...")
    id_to_downstream = dict(zip(rivers["HYRIV_ID"], rivers["NEXT_DOWN"]))
    id_to_length = dict(zip(rivers["HYRIV_ID"], rivers["LENGTH_KM"]))

    id_to_upstream = {}
    for hyriv_id, next_down in id_to_downstream.items():
        if next_down != 0:
            if next_down not in id_to_upstream:
                id_to_upstream[next_down] = []
            id_to_upstream[next_down].append(hyriv_id)

    log(f"  Network has {len(id_to_downstream):,} segments")
    return id_to_downstream, id_to_upstream, id_to_length


def build_river_lookups(rivers: gpd.GeoDataFrame, rivers_proj: gpd.GeoDataFrame):
    """Build lookup dictionaries for fast access."""
    log("Building river lookup dictionaries...")

    # Geometry lookup (projected for line splitting)
    id_to_geom_proj = dict(zip(rivers_proj["HYRIV_ID"], rivers_proj.geometry))

    # Metadata lookup
    id_to_strahler = dict(zip(rivers["HYRIV_ID"], rivers["ORD_STRA"]))
    id_to_discharge = dict(zip(rivers["HYRIV_ID"], rivers["DIS_AV_CMS"]))

    log(f"  Built lookups for {len(id_to_geom_proj):,} segments")
    return id_to_geom_proj, id_to_strahler, id_to_discharge


def split_line_at_point(line: LineString, point: Point) -> tuple[LineString, LineString]:
    """
    Split a LineString at the closest point to the given point.
    Returns (upstream_part, downstream_part).
    """
    distance_along = line.project(point)

    if distance_along <= 0:
        return None, line
    elif distance_along >= line.length:
        return line, None

    split_point = line.interpolate(distance_along)
    coords = list(line.coords)

    upstream_coords = [coords[0]]
    downstream_coords = []

    cumulative_dist = 0
    split_inserted = False

    for i in range(1, len(coords)):
        segment = LineString([coords[i-1], coords[i]])
        segment_length = segment.length

        if not split_inserted and cumulative_dist + segment_length >= distance_along:
            upstream_coords.append((split_point.x, split_point.y))
            downstream_coords.append((split_point.x, split_point.y))
            downstream_coords.append(coords[i])
            split_inserted = True
        elif not split_inserted:
            upstream_coords.append(coords[i])
        else:
            downstream_coords.append(coords[i])

        cumulative_dist += segment_length

    upstream_line = LineString(upstream_coords) if len(upstream_coords) >= 2 else None
    downstream_line = LineString(downstream_coords) if len(downstream_coords) >= 2 else None

    return upstream_line, downstream_line


def trace_downstream_segments(
    start_id: int,
    id_to_downstream: dict,
    id_to_length: dict,
    max_distance_km: float
) -> list:
    """Trace downstream and return list of segment IDs in order."""
    segments = []
    current_id = id_to_downstream.get(start_id, 0)
    distance = 0

    while current_id != 0 and distance < max_distance_km:
        segments.append(current_id)
        distance += id_to_length.get(current_id, 0)
        current_id = id_to_downstream.get(current_id, 0)

    return segments


def trace_upstream_segments(
    start_id: int,
    id_to_upstream: dict,
    id_to_length: dict,
    max_distance_km: float
) -> list:
    """Trace upstream using BFS and return list of segment IDs."""
    segments = []
    queue = [(uid, 0) for uid in id_to_upstream.get(start_id, [])]
    visited = set()

    while queue:
        current_id, dist = queue.pop(0)
        if current_id in visited or dist > max_distance_km:
            continue
        visited.add(current_id)
        segments.append(current_id)
        seg_len = id_to_length.get(current_id, 0)
        for uid in id_to_upstream.get(current_id, []):
            queue.append((uid, dist + seg_len))

    return segments


def process_facilities_fast(
    facilities: gpd.GeoDataFrame,
    facilities_proj: gpd.GeoDataFrame,
    rivers_proj: gpd.GeoDataFrame,
    id_to_downstream: dict,
    id_to_upstream: dict,
    id_to_length: dict,
    id_to_geom_proj: dict,
    id_to_strahler: dict,
    id_to_discharge: dict,
    max_distance_m: float,
    upstream_distance_km: float,
    downstream_distance_km: float,
):
    """Process all facilities in an optimized way."""

    # Build spatial index
    log("Building spatial index for rivers...")
    river_tree = STRtree(rivers_proj.geometry.values)
    river_indices = rivers_proj.index.values
    river_ids = rivers_proj["HYRIV_ID"].values
    log(f"  Spatial index built for {len(rivers_proj):,} rivers")

    log(f"\nProcessing {len(facilities):,} facilities...")

    results = []
    all_upstream_ids = set()
    all_downstream_ids = set()

    # Store split line parts for later geometry building
    facility_split_parts = {}  # facility_idx -> (upstream_part_proj, downstream_part_proj)

    for i, (idx, row) in enumerate(facilities.iterrows()):
        if i % 10000 == 0 and i > 0:
            log(f"  Processed {i:,}/{len(facilities):,}...")

        facility_point_proj = facilities_proj.loc[idx].geometry

        # Find closest river using spatial index
        buffer = facility_point_proj.buffer(max_distance_m)
        candidate_indices = river_tree.query(buffer)

        if len(candidate_indices) == 0:
            continue

        # Get candidates
        candidate_geoms = rivers_proj.geometry.iloc[candidate_indices]
        distances = candidate_geoms.distance(facility_point_proj)

        # Filter by max distance
        nearby_mask = distances <= max_distance_m
        if not nearby_mask.any():
            continue

        # Get closest
        closest_local_idx = distances[nearby_mask].idxmin()
        closest_distance = distances[closest_local_idx]
        closest_river_id = rivers_proj.loc[closest_local_idx, "HYRIV_ID"]

        # Get the geometry from lookup (faster than GeoDataFrame access)
        river_geom_proj = id_to_geom_proj.get(closest_river_id)
        if river_geom_proj is None:
            continue

        # Split the closest segment at the nearest point to facility
        upstream_part, downstream_part = split_line_at_point(river_geom_proj, facility_point_proj)

        # Trace further upstream/downstream (just IDs, very fast)
        upstream_segment_ids = trace_upstream_segments(closest_river_id, id_to_upstream, id_to_length, upstream_distance_km)
        downstream_segment_ids = trace_downstream_segments(closest_river_id, id_to_downstream, id_to_length, downstream_distance_km)

        # Collect all segment IDs
        all_upstream_ids.update(upstream_segment_ids)
        all_downstream_ids.update(downstream_segment_ids)
        all_upstream_ids.add(closest_river_id)
        all_downstream_ids.add(closest_river_id)

        # Store split parts for geometry building later
        facility_split_parts[idx] = (upstream_part, downstream_part)

        # Build result (without geometries for now)
        results.append({
            "facility_idx": idx,
            "facilityName": row.get("facilityName"),
            "city": row.get("city"),
            "countryName": row.get("countryName"),
            "EPRTR_SectorCode": row.get("EPRTR_SectorCode"),
            "EPRTR_SectorName": row.get("EPRTR_SectorName"),
            "Pollutant": row.get("Pollutant"),
            "Releases": row.get("Releases"),
            "facility_lon": row.get("Longitude"),
            "facility_lat": row.get("Latitude"),
            "closest_river_id": closest_river_id,
            "distance_to_river_m": closest_distance,
            "river_strahler": id_to_strahler.get(closest_river_id),
            "river_discharge": id_to_discharge.get(closest_river_id),
            "upstream_segment_ids": upstream_segment_ids,
            "downstream_segment_ids": downstream_segment_ids,
            "n_upstream": len(upstream_segment_ids) + (1 if upstream_part else 0),
            "n_downstream": len(downstream_segment_ids) + (1 if downstream_part else 0),
        })

    log(f"  Found {len(results):,} facilities with nearby rivers")
    log(f"  Total unique upstream segments: {len(all_upstream_ids):,}")
    log(f"  Total unique downstream segments: {len(all_downstream_ids):,}")

    return results, all_upstream_ids, all_downstream_ids, facility_split_parts


def build_geometries_and_clip_polygons(
    results: list,
    facility_split_parts: dict,
    id_to_geom_proj: dict,
    river_polygons: gpd.GeoDataFrame,
    buffer_meters: float,
):
    """Build line geometries and clip polygons for all facilities."""
    log("\nBuilding line geometries and clipping polygons...")

    # Ensure polygons in EPSG:3035
    if river_polygons.crs.to_epsg() != 3035:
        river_polygons_proj = river_polygons.to_crs("EPSG:3035")
    else:
        river_polygons_proj = river_polygons

    # Build spatial index for polygons
    polygon_sindex = river_polygons_proj.sindex

    for i, result in enumerate(results):
        if i % 5000 == 0 and i > 0:
            log(f"  Processed {i:,}/{len(results):,}...")

        facility_idx = result["facility_idx"]
        upstream_part, downstream_part = facility_split_parts.get(facility_idx, (None, None))

        # Build upstream line geometry (in projected CRS)
        upstream_geoms = []
        if upstream_part is not None and not upstream_part.is_empty:
            upstream_geoms.append(upstream_part)
        for seg_id in result["upstream_segment_ids"]:
            geom = id_to_geom_proj.get(seg_id)
            if geom is not None:
                upstream_geoms.append(geom)

        # Build downstream line geometry (in projected CRS)
        downstream_geoms = []
        if downstream_part is not None and not downstream_part.is_empty:
            downstream_geoms.append(downstream_part)
        for seg_id in result["downstream_segment_ids"]:
            geom = id_to_geom_proj.get(seg_id)
            if geom is not None:
                downstream_geoms.append(geom)

        # Merge line geometries (still in EPSG:3035)
        upstream_line_proj = unary_union(upstream_geoms) if upstream_geoms else None
        downstream_line_proj = unary_union(downstream_geoms) if downstream_geoms else None

        # Clip polygons based on line geometries
        upstream_poly_proj = None
        downstream_poly_proj = None

        if upstream_line_proj is not None:
            upstream_buffer = upstream_line_proj.buffer(buffer_meters)
            candidates_idx = list(polygon_sindex.intersection(upstream_buffer.bounds))
            if candidates_idx:
                candidates = river_polygons_proj.iloc[candidates_idx]
                intersecting = candidates[candidates.intersects(upstream_buffer)]
                if len(intersecting) > 0:
                    clipped = [row.geometry.intersection(upstream_buffer) for _, row in intersecting.iterrows()]
                    clipped = [p for p in clipped if not p.is_empty]
                    if clipped:
                        upstream_poly_proj = unary_union(clipped)

        if downstream_line_proj is not None:
            downstream_buffer = downstream_line_proj.buffer(buffer_meters)
            candidates_idx = list(polygon_sindex.intersection(downstream_buffer.bounds))
            if candidates_idx:
                candidates = river_polygons_proj.iloc[candidates_idx]
                intersecting = candidates[candidates.intersects(downstream_buffer)]
                if len(intersecting) > 0:
                    clipped = [row.geometry.intersection(downstream_buffer) for _, row in intersecting.iterrows()]
                    clipped = [p for p in clipped if not p.is_empty]
                    if clipped:
                        downstream_poly_proj = unary_union(clipped)

        # Convert everything to WGS84
        upstream_line = None
        downstream_line = None
        upstream_poly = None
        downstream_poly = None

        if upstream_line_proj:
            upstream_line = gpd.GeoSeries([upstream_line_proj], crs="EPSG:3035").to_crs("EPSG:4326").iloc[0]
        if downstream_line_proj:
            downstream_line = gpd.GeoSeries([downstream_line_proj], crs="EPSG:3035").to_crs("EPSG:4326").iloc[0]
        if upstream_poly_proj:
            upstream_poly = gpd.GeoSeries([upstream_poly_proj], crs="EPSG:3035").to_crs("EPSG:4326").iloc[0]
        if downstream_poly_proj:
            downstream_poly = gpd.GeoSeries([downstream_poly_proj], crs="EPSG:3035").to_crs("EPSG:4326").iloc[0]

        result["upstream_line_geom"] = upstream_line
        result["downstream_line_geom"] = downstream_line
        result["upstream_poly_geom"] = upstream_poly
        result["downstream_poly_geom"] = downstream_poly

    log(f"  Processed {len(results):,} facilities")
    return results


def batch_clip_polygons(
    all_segment_ids: set,
    direction: str,
    rivers_proj: gpd.GeoDataFrame,
    river_polygons: gpd.GeoDataFrame,
    buffer_meters: float,
) -> gpd.GeoDataFrame:
    """Batch clip polygons for all segments at once."""
    log(f"  Clipping {direction} polygons for {len(all_segment_ids):,} segments...")

    if not all_segment_ids:
        return gpd.GeoDataFrame(columns=["geometry", "direction", "HYRIV_ID", "matched_river_ids", "source_basin"])

    # Get all segment geometries
    segments_gdf = rivers_proj[rivers_proj["HYRIV_ID"].isin(all_segment_ids)].copy()
    if len(segments_gdf) == 0:
        return gpd.GeoDataFrame(columns=["geometry", "direction", "HYRIV_ID", "matched_river_ids", "source_basin"])

    # Create buffer for each segment
    segments_gdf["buffer"] = segments_gdf.geometry.buffer(buffer_meters)

    # Ensure polygons in EPSG:3035
    if river_polygons.crs.to_epsg() != 3035:
        river_polygons_proj = river_polygons.to_crs("EPSG:3035")
    else:
        river_polygons_proj = river_polygons

    # Build spatial index for polygons
    polygon_sindex = river_polygons_proj.sindex

    clipped_results = []
    processed = 0

    for idx, seg_row in segments_gdf.iterrows():
        seg_id = seg_row["HYRIV_ID"]
        buffer_geom = seg_row["buffer"]

        # Find candidate polygons
        candidates_idx = list(polygon_sindex.intersection(buffer_geom.bounds))
        if not candidates_idx:
            continue

        candidates = river_polygons_proj.iloc[candidates_idx]
        intersecting = candidates[candidates.intersects(buffer_geom)]

        for poly_idx, poly_row in intersecting.iterrows():
            clipped = poly_row.geometry.intersection(buffer_geom)
            if not clipped.is_empty:
                clipped_results.append({
                    "geometry": clipped,
                    "direction": direction,
                    "HYRIV_ID": seg_id,
                    "matched_river_ids": [seg_id],
                    "source_basin": poly_row.get("source_basin"),
                    "OBJECT_ID": poly_row.get("OBJECT_ID"),
                })

        processed += 1
        if processed % 10000 == 0:
            log(f"    Processed {processed:,}/{len(segments_gdf):,} segments...")

    if not clipped_results:
        return gpd.GeoDataFrame(columns=["geometry", "direction", "HYRIV_ID", "matched_river_ids", "source_basin"])

    result = gpd.GeoDataFrame(clipped_results, crs="EPSG:3035")
    log(f"    Clipped {len(result):,} polygons")
    return result.to_crs("EPSG:4326")


def main(
    facilities_path: Path,
    rivers_path: Path,
    polygons_path: Path,
    output_facilities_path: Path,
    output_segments_path: Path,
    max_distance_m: float = 1000,
    upstream_distance_km: float = 10,
    downstream_distance_km: float = 10,
    polygon_buffer_m: float = 600,
    min_strahler_order: int = 1,
    limit: int = None,
):
    """Main pipeline - polygons are stored per-facility as WKB."""

    # Load data
    facilities = load_facilities(facilities_path)
    rivers = load_rivers(rivers_path, min_strahler_order)
    river_polygons = load_river_polygons(polygons_path)

    # Keep only needed columns
    river_columns = ["HYRIV_ID", "NEXT_DOWN", "ORD_STRA", "DIS_AV_CMS", "LENGTH_KM", "UPLAND_SKM", "DIST_DN_KM"]
    rivers = rivers[["geometry"] + river_columns]

    # Build network
    id_to_downstream, id_to_upstream, id_to_length = build_river_network(rivers)

    # Project for distance calculations
    log("Projecting data to EPSG:3035...")
    facilities_proj = facilities.to_crs("EPSG:3035")
    rivers_proj = rivers.to_crs("EPSG:3035")

    # Build lookups
    id_to_geom_proj, id_to_strahler, id_to_discharge = build_river_lookups(rivers, rivers_proj)

    # Process facilities
    if limit:
        facilities = facilities.head(limit)
        facilities_proj = facilities_proj.head(limit)

    # Phase 1: Process all facilities (fast - just IDs)
    results, all_upstream_ids, all_downstream_ids, facility_split_parts = process_facilities_fast(
        facilities, facilities_proj, rivers_proj,
        id_to_downstream, id_to_upstream, id_to_length,
        id_to_geom_proj, id_to_strahler, id_to_discharge,
        max_distance_m, upstream_distance_km, downstream_distance_km
    )

    if not results:
        log("No facilities found near rivers!")
        return None, None, None

    # Phase 2: Build line geometries and clip polygons per facility
    results = build_geometries_and_clip_polygons(
        results, facility_split_parts, id_to_geom_proj,
        river_polygons, polygon_buffer_m
    )

    # Create facilities GeoDataFrame
    result_df = pd.DataFrame(results)

    # Convert line and polygon geometries to WKB for parquet storage
    from shapely import wkb
    result_df["upstream_line_wkb"] = result_df["upstream_line_geom"].apply(
        lambda g: g.wkb if g is not None else None
    )
    result_df["downstream_line_wkb"] = result_df["downstream_line_geom"].apply(
        lambda g: g.wkb if g is not None else None
    )
    result_df["upstream_poly_wkb"] = result_df["upstream_poly_geom"].apply(
        lambda g: g.wkb if g is not None else None
    )
    result_df["downstream_poly_wkb"] = result_df["downstream_poly_geom"].apply(
        lambda g: g.wkb if g is not None else None
    )
    result_df = result_df.drop(columns=["upstream_line_geom", "downstream_line_geom", "upstream_poly_geom", "downstream_poly_geom"])

    facilities_gdf = gpd.GeoDataFrame(
        result_df,
        geometry=gpd.points_from_xy(result_df["facility_lon"], result_df["facility_lat"]),
        crs="EPSG:4326"
    )

    # Phase 3: Build segments dataset
    log(f"\nBuilding segments dataset...")

    all_segment_ids = all_upstream_ids | all_downstream_ids
    segments_data = []

    for seg_id in all_segment_ids:
        seg_row = rivers[rivers["HYRIV_ID"] == seg_id]
        if len(seg_row) == 0:
            continue
        seg_row = seg_row.iloc[0]

        direction = []
        if seg_id in all_upstream_ids:
            direction.append("upstream")
        if seg_id in all_downstream_ids:
            direction.append("downstream")

        for d in direction:
            segments_data.append({
                "HYRIV_ID": seg_id,
                "direction": d,
                "geometry": seg_row.geometry,
                "ORD_STRA": seg_row["ORD_STRA"],
                "DIS_AV_CMS": seg_row["DIS_AV_CMS"],
                "LENGTH_KM": seg_row["LENGTH_KM"],
            })

    segments_gdf = gpd.GeoDataFrame(segments_data, crs=rivers.crs)
    log(f"  Created {len(segments_gdf):,} segment entries")

    # Save outputs (polygons are now stored per-facility in facilities file as WKB)
    log(f"\nSaving outputs...")
    log(f"  Facilities: {output_facilities_path}")
    facilities_gdf.to_parquet(output_facilities_path)

    log(f"  Segments: {output_segments_path}")
    segments_gdf.to_parquet(output_segments_path)

    # Note: Polygons are now stored per-facility as upstream_poly_wkb and downstream_poly_wkb
    # in the facilities file, so we don't create a separate polygons file

    log("\nDone!")
    log(f"  Facilities with rivers: {len(facilities_gdf):,}")
    if len(facilities_gdf) > 0:
        log(f"  Avg upstream parts: {facilities_gdf['n_upstream'].mean():.1f}")
        log(f"  Avg downstream parts: {facilities_gdf['n_downstream'].mean():.1f}")

    return facilities_gdf, segments_gdf


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent

    facilities_path = base_dir / "tabular" / "F2_4_Water_Releases_Facilities.csv"
    rivers_path = base_dir.parent / "hydro-rivers-europe" / "HydroRIVERS_v10_eu.geoparquet"
    polygons_path = base_dir.parent / "eu-hydro-master-skeleton" / "eu_hydro_master_skeleton_geoparquet" / "river_polygons"

    output_facilities_path = base_dir / "river_data_facilities.geoparquet"
    output_segments_path = base_dir / "river_data_segments.geoparquet"

    result = main(
        facilities_path=facilities_path,
        rivers_path=rivers_path,
        polygons_path=polygons_path,
        output_facilities_path=output_facilities_path,
        output_segments_path=output_segments_path,
        max_distance_m=1000,
        upstream_distance_km=10,
        downstream_distance_km=10,
        polygon_buffer_m=600,
        min_strahler_order=1,
        limit=None,
    )
