"""
Visualize facilities with upstream/downstream river segments and polygons.
Uses polygon WKB stored per facility for correct clipping.
"""

import geopandas as gpd
import folium
from folium.plugins import AntPath, MarkerCluster
from pathlib import Path
from shapely import wkb


def load_facilities(filepath: Path) -> gpd.GeoDataFrame:
    """Load facility data with upstream/downstream geometries."""
    print(f"Loading facilities from {filepath}...")
    gdf = gpd.read_parquet(filepath)
    print(f"Loaded {len(gdf):,} facilities")
    return gdf


def load_segments(filepath: Path) -> gpd.GeoDataFrame:
    """Load river segments with direction info."""
    print(f"Loading segments from {filepath}...")
    gdf = gpd.read_parquet(filepath)
    print(f"Loaded {len(gdf):,} segments ({gdf['direction'].value_counts().to_dict()})")
    return gdf


def get_facility_color(sector_code: int) -> str:
    """Color based on industry sector."""
    colors = {
        1: "#e41a1c",  # Energy - red
        2: "#377eb8",  # Metals - blue
        3: "#4daf4a",  # Minerals - green
        4: "#984ea3",  # Chemical - purple
        5: "#ff7f00",  # Waste/wastewater - orange
        6: "#a65628",  # Paper - brown
        7: "#f781bf",  # Livestock - pink
        8: "#999999",  # Food - gray
        9: "#66c2a5",  # Other - teal
    }
    return colors.get(sector_code, "#999999")


# Upstream = green/teal tones (coming from nature)
# Downstream = red/orange tones (affected by pollution)
UPSTREAM_LINE_COLOR = "#2ca02c"  # Green
DOWNSTREAM_LINE_COLOR = "#d62728"  # Red
UPSTREAM_POLY_COLOR = "#98df8a"  # Light green
DOWNSTREAM_POLY_COLOR = "#ff9896"  # Light red


def create_map(
    facilities: gpd.GeoDataFrame,
    segments: gpd.GeoDataFrame,
    output_path: Path,
    sample_facilities: int = 2000
) -> None:
    """Create interactive map with upstream/downstream visualization."""
    print("Creating map...")

    # Sample facilities for performance
    if len(facilities) > sample_facilities:
        print(f"Sampling {sample_facilities} facilities for performance...")
        facilities = facilities.sample(sample_facilities, random_state=42)

    # Get segment IDs for sampled facilities
    upstream_ids = set()
    downstream_ids = set()
    for _, row in facilities.iterrows():
        upstream_ids.update(row.get("upstream_segment_ids", []))
        downstream_ids.update(row.get("downstream_segment_ids", []))

    # Filter segments
    upstream_segments = segments[(segments["direction"] == "upstream") & (segments["HYRIV_ID"].isin(upstream_ids))]
    downstream_segments = segments[(segments["direction"] == "downstream") & (segments["HYRIV_ID"].isin(downstream_ids))]

    print(f"  Upstream segments: {len(upstream_segments):,}")
    print(f"  Downstream segments: {len(downstream_segments):,}")

    # Calculate center
    center_lat = facilities.geometry.y.mean()
    center_lon = facilities.geometry.x.mean()

    # Create map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=5,
        tiles="CartoDB positron"
    )

    # Feature groups
    upstream_poly_layer = folium.FeatureGroup(name="Upstream Polygons", show=True)
    downstream_poly_layer = folium.FeatureGroup(name="Downstream Polygons", show=True)
    upstream_line_layer = folium.FeatureGroup(name="Upstream Flow", show=True)
    downstream_line_layer = folium.FeatureGroup(name="Downstream Flow", show=True)

    # Add polygons from facility WKB
    print("Adding polygons from facility data...")
    poly_count_up = 0
    poly_count_down = 0

    for _, row in facilities.iterrows():
        # Upstream polygon
        if row.get("upstream_poly_wkb") is not None:
            try:
                geom = wkb.loads(row["upstream_poly_wkb"])
                if geom is not None and not geom.is_empty:
                    if geom.geom_type == "Polygon":
                        polys = [geom]
                    elif geom.geom_type == "MultiPolygon":
                        polys = list(geom.geoms)
                    else:
                        polys = []

                    for poly in polys:
                        coords = [[c[1], c[0]] for c in poly.exterior.coords]
                        folium.Polygon(
                            locations=coords,
                            color=UPSTREAM_LINE_COLOR,
                            weight=1,
                            fill=True,
                            fill_color=UPSTREAM_POLY_COLOR,
                            fill_opacity=0.5,
                        ).add_to(upstream_poly_layer)
                        poly_count_up += 1
            except:
                pass

        # Downstream polygon
        if row.get("downstream_poly_wkb") is not None:
            try:
                geom = wkb.loads(row["downstream_poly_wkb"])
                if geom is not None and not geom.is_empty:
                    if geom.geom_type == "Polygon":
                        polys = [geom]
                    elif geom.geom_type == "MultiPolygon":
                        polys = list(geom.geoms)
                    else:
                        polys = []

                    for poly in polys:
                        coords = [[c[1], c[0]] for c in poly.exterior.coords]
                        folium.Polygon(
                            locations=coords,
                            color=DOWNSTREAM_LINE_COLOR,
                            weight=1,
                            fill=True,
                            fill_color=DOWNSTREAM_POLY_COLOR,
                            fill_opacity=0.5,
                        ).add_to(downstream_poly_layer)
                        poly_count_down += 1
            except:
                pass

    print(f"  Added {poly_count_up:,} upstream polygons, {poly_count_down:,} downstream polygons")

    # Add upstream segments with flow animation (reverse direction - going upstream)
    print("Adding upstream segments...")
    for idx, row in upstream_segments.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue

        if geom.geom_type == "LineString":
            coords = list(geom.coords)
        elif geom.geom_type == "MultiLineString":
            coords = []
            for part in geom.geoms:
                coords.extend(list(part.coords))
        else:
            continue

        if not coords:
            continue

        # Reverse coords for upstream (animation goes against flow)
        coords_latlon = [[c[1], c[0]] for c in reversed(coords)]
        weight = max(1, row["ORD_STRA"] - 2)

        popup_html = f"""
        <b>Upstream Segment</b><br>
        <b>ID:</b> {row['HYRIV_ID']}<br>
        <b>Strahler:</b> {row['ORD_STRA']}<br>
        <b>Discharge:</b> {row['DIS_AV_CMS']:.1f} m³/s
        """

        AntPath(
            locations=coords_latlon,
            weight=weight,
            color=UPSTREAM_LINE_COLOR,
            pulse_color="#ffffff",
            delay=800,
            dash_array=[10, 20],
            popup=folium.Popup(popup_html, max_width=250)
        ).add_to(upstream_line_layer)

    # Add downstream segments with flow animation
    print("Adding downstream segments...")
    for idx, row in downstream_segments.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue

        if geom.geom_type == "LineString":
            coords = list(geom.coords)
        elif geom.geom_type == "MultiLineString":
            coords = []
            for part in geom.geoms:
                coords.extend(list(part.coords))
        else:
            continue

        if not coords:
            continue

        coords_latlon = [[c[1], c[0]] for c in coords]
        weight = max(1, row["ORD_STRA"] - 2)

        popup_html = f"""
        <b>Downstream Segment</b><br>
        <b>ID:</b> {row['HYRIV_ID']}<br>
        <b>Strahler:</b> {row['ORD_STRA']}<br>
        <b>Discharge:</b> {row['DIS_AV_CMS']:.1f} m³/s
        """

        AntPath(
            locations=coords_latlon,
            weight=weight,
            color=DOWNSTREAM_LINE_COLOR,
            pulse_color="#ffffff",
            delay=800,
            dash_array=[10, 20],
            popup=folium.Popup(popup_html, max_width=250)
        ).add_to(downstream_line_layer)

    # Add facilities
    print("Adding facilities...")
    marker_cluster = MarkerCluster(name="Facilities").add_to(m)

    for _, row in facilities.iterrows():
        color = get_facility_color(row.get("EPRTR_SectorCode", 0))

        popup_html = f"""
        <b>{row.get('facilityName', 'Unknown')}</b><br>
        <b>City:</b> {row.get('city', 'N/A')}<br>
        <b>Sector:</b> {row.get('EPRTR_SectorName', 'N/A')}<br>
        <b>Pollutant:</b> {row.get('Pollutant', 'N/A')}<br>
        <b>Upstream segments:</b> {row.get('n_upstream', 0)}<br>
        <b>Downstream segments:</b> {row.get('n_downstream', 0)}
        """

        folium.CircleMarker(
            location=[row.geometry.y, row.geometry.x],
            radius=6,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7,
            popup=folium.Popup(popup_html, max_width=300)
        ).add_to(marker_cluster)

    # Add layers
    upstream_poly_layer.add_to(m)
    downstream_poly_layer.add_to(m)
    upstream_line_layer.add_to(m)
    downstream_line_layer.add_to(m)
    folium.LayerControl().add_to(m)

    # Legend
    legend_html = """
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000;
                background-color: white; padding: 15px; border-radius: 8px;
                border: 2px solid #333; font-size: 12px; max-width: 200px;
                box-shadow: 0 2px 6px rgba(0,0,0,0.3);">
        <b>River Flow Analysis</b><br><br>
        <b style="color: #2ca02c;">━━━ Upstream</b><br>
        Source water (green)<br><br>
        <b style="color: #d62728;">━━━ Downstream</b><br>
        Potentially affected (red)<br><br>
        <b>Facilities:</b><br>
        Colored by industry sector<br>
        <small>Click for details</small>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    # Save
    print(f"Saving map to {output_path}...")
    m.save(str(output_path))
    print(f"Done!")


def main():
    base_dir = Path(__file__).resolve().parent

    # Paths
    facilities_path = base_dir / "river_data_facilities.geoparquet"
    segments_path = base_dir / "river_data_segments.geoparquet"
    output_path = base_dir / "facilities_rivers_map.html"

    # Load data
    facilities = load_facilities(facilities_path)
    segments = load_segments(segments_path)

    # Create map
    create_map(facilities, segments, output_path, sample_facilities=2000)


if __name__ == "__main__":
    main()
