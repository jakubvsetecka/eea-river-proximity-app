"""
Visualize a single facility with its upstream/downstream river split.
Uses pre-generated river_data files with polygon WKB stored per facility.
"""

import geopandas as gpd
import folium
from folium.plugins import AntPath
from pathlib import Path
import sys
from shapely import wkb

UPSTREAM_COLOR = "#2ca02c"  # Green
DOWNSTREAM_COLOR = "#d62728"  # Red
UPSTREAM_POLY_COLOR = "#98df8a"
DOWNSTREAM_POLY_COLOR = "#ff9896"


def create_facility_map(facility_name: str, output_path: Path):
    """Create a map focused on a single facility showing upstream/downstream split."""

    base_dir = Path(__file__).resolve().parent

    # Load pre-generated facility data
    print(f"Loading data...")
    facilities_path = base_dir / "river_data_facilities.geoparquet"
    facilities = gpd.read_parquet(facilities_path)
    print(f"Loaded {len(facilities):,} facilities")

    # Find facility
    facility_matches = facilities[facilities['facilityName'].str.contains(facility_name, case=False, na=False)]
    if len(facility_matches) == 0:
        print(f"Facility '{facility_name}' not found!")
        return

    facility = facility_matches.iloc[0]
    print(f"Found: {facility['facilityName']} in {facility['city']}")
    print(f"  Closest river: {facility['closest_river_id']} ({facility['distance_to_river_m']:.0f}m away)")
    print(f"  Upstream: {facility['n_upstream']} parts")
    print(f"  Downstream: {facility['n_downstream']} parts")

    # Parse geometries from WKB
    upstream_line_geom = None
    downstream_line_geom = None
    upstream_poly_geom = None
    downstream_poly_geom = None

    if 'upstream_line_wkb' in facility.index and facility['upstream_line_wkb'] is not None:
        upstream_line_geom = wkb.loads(facility['upstream_line_wkb'])
    if 'downstream_line_wkb' in facility.index and facility['downstream_line_wkb'] is not None:
        downstream_line_geom = wkb.loads(facility['downstream_line_wkb'])
    if 'upstream_poly_wkb' in facility.index and facility['upstream_poly_wkb'] is not None:
        upstream_poly_geom = wkb.loads(facility['upstream_poly_wkb'])
    if 'downstream_poly_wkb' in facility.index and facility['downstream_poly_wkb'] is not None:
        downstream_poly_geom = wkb.loads(facility['downstream_poly_wkb'])

    # Create map
    m = folium.Map(
        location=[facility.geometry.y, facility.geometry.x],
        zoom_start=13,
        tiles="CartoDB positron"
    )

    # Add upstream polygons
    if upstream_poly_geom is not None:
        if upstream_poly_geom.geom_type == "Polygon":
            polys = [upstream_poly_geom]
        elif upstream_poly_geom.geom_type == "MultiPolygon":
            polys = list(upstream_poly_geom.geoms)
        else:
            polys = []

        for poly in polys:
            coords = [[c[1], c[0]] for c in poly.exterior.coords]
            folium.Polygon(
                locations=coords,
                color=UPSTREAM_COLOR,
                weight=1,
                fill=True,
                fill_color=UPSTREAM_POLY_COLOR,
                fill_opacity=0.4,
                popup="Upstream water surface"
            ).add_to(m)

    # Add downstream polygons
    if downstream_poly_geom is not None:
        if downstream_poly_geom.geom_type == "Polygon":
            polys = [downstream_poly_geom]
        elif downstream_poly_geom.geom_type == "MultiPolygon":
            polys = list(downstream_poly_geom.geoms)
        else:
            polys = []

        for poly in polys:
            coords = [[c[1], c[0]] for c in poly.exterior.coords]
            folium.Polygon(
                locations=coords,
                color=DOWNSTREAM_COLOR,
                weight=1,
                fill=True,
                fill_color=DOWNSTREAM_POLY_COLOR,
                fill_opacity=0.4,
                popup="Downstream water surface"
            ).add_to(m)

    # Add upstream line (animated going upstream = reversed coords)
    if upstream_line_geom is not None:
        if upstream_line_geom.geom_type == "LineString":
            lines = [upstream_line_geom]
        elif upstream_line_geom.geom_type == "MultiLineString":
            lines = list(upstream_line_geom.geoms)
        else:
            lines = []

        for line in lines:
            coords = [[c[1], c[0]] for c in reversed(list(line.coords))]
            AntPath(
                locations=coords,
                weight=4,
                color=UPSTREAM_COLOR,
                pulse_color="#ffffff",
                delay=600,
                dash_array=[10, 20],
                popup="Upstream (source water)"
            ).add_to(m)

    # Add downstream line (animated going downstream)
    if downstream_line_geom is not None:
        if downstream_line_geom.geom_type == "LineString":
            lines = [downstream_line_geom]
        elif downstream_line_geom.geom_type == "MultiLineString":
            lines = list(downstream_line_geom.geoms)
        else:
            lines = []

        for line in lines:
            coords = [[c[1], c[0]] for c in line.coords]
            AntPath(
                locations=coords,
                weight=4,
                color=DOWNSTREAM_COLOR,
                pulse_color="#ffffff",
                delay=600,
                dash_array=[10, 20],
                popup="Downstream (affected area)"
            ).add_to(m)

    # Add facility marker
    folium.Marker(
        location=[facility.geometry.y, facility.geometry.x],
        popup=f"""
        <b>{facility['facilityName']}</b><br>
        {facility.get('city', '')}, {facility.get('countryName', '')}<br>
        <hr>
        <b>Closest river:</b> {facility['closest_river_id']}<br>
        <b>Distance:</b> {facility['distance_to_river_m']:.0f}m<br>
        <b>River order:</b> {facility['river_strahler']}<br>
        <b>Discharge:</b> {facility['river_discharge']:.1f} m³/s<br>
        <hr>
        <b style="color:{UPSTREAM_COLOR}">Upstream:</b> {facility['n_upstream']} parts<br>
        <b style="color:{DOWNSTREAM_COLOR}">Downstream:</b> {facility['n_downstream']} parts
        """,
        icon=folium.Icon(color='orange', icon='industry', prefix='fa')
    ).add_to(m)

    # Legend
    legend_html = f"""
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000;
                background-color: white; padding: 15px; border-radius: 8px;
                border: 2px solid #333; font-size: 12px; max-width: 220px;">
        <b>{facility['facilityName']}</b><br>
        <small>{facility.get('city', '')}</small><br><br>
        <b style="color: {UPSTREAM_COLOR};">━━━ Upstream ({facility['n_upstream']})</b><br>
        Source water flowing toward facility<br><br>
        <b style="color: {DOWNSTREAM_COLOR};">━━━ Downstream ({facility['n_downstream']})</b><br>
        Water flowing away (potentially affected)<br><br>
        <small>River split at closest point to facility</small>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    # Save
    print(f"Saving to {output_path}...")
    m.save(str(output_path))
    print("Done!")


if __name__ == "__main__":
    facility_name = sys.argv[1] if len(sys.argv) > 1 else "PRECHEZA"
    output_path = Path(__file__).resolve().parent / "facility_map.html"
    create_facility_map(facility_name, output_path)
