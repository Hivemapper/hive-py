# Buffer projection note: convert_to_geojson_poly() uses EPSG:3857. For Turf.js-identical buffers, use AEQD projection.

import json
import warnings
import shapely
from shapely.validation import make_valid
from shapely.ops import orient
from area import area as geodesic_area

MAX_AREA = 5_000_000  # 5 km² — max area for imagery/mapfeatures
MIN_AREA = 10  # 10 m²  — min area
MAX_PROBE_AREA = 1_000_000  # 1 km² — max area for probe endpoint
MAX_VERTICES = 500  # max vertices per polygon
MAX_POLYGONS = 50  # max polygons in a MultiPolygon


def validate_geometry(geojson_dict):
    """Convert GeoJSON dict to valid Shapely geometry."""
    if not geojson_dict:
        raise ValueError("geojson_dict is None or empty")
    geometry = shapely.from_geojson(json.dumps(geojson_dict))
    if not geometry.is_valid:
        geometry = make_valid(geometry)
    return geometry


def enforce_winding_order(geometry):
    """Apply RFC 7946 winding: CCW exterior, CW holes."""
    return orient(geometry, sign=1.0)


def check_area_bounds(geojson_dict, max_area=MAX_AREA, min_area=MIN_AREA):
    """Return True if geodesic area is within [min_area, max_area] m²."""
    computed = abs(geodesic_area(geojson_dict))
    return min_area <= computed <= max_area


def count_vertices(geometry):
    """Return max vertex count for Polygon/MultiPolygon."""
    if geometry.geom_type == "Polygon":
        return len(geometry.exterior.coords)
    if geometry.geom_type == "MultiPolygon":
        return max(len(p.exterior.coords) for p in geometry.geoms)
    return 0


def count_polygons(geometry):
    """Return polygon count."""
    if geometry.geom_type == "MultiPolygon":
        return len(list(geometry.geoms))
    if geometry.geom_type == "Polygon":
        return 1
    return 0


def simplify_if_needed(geometry, max_vertices=MAX_VERTICES):
    """Iteratively simplify geometry until vertex count <= max_vertices."""
    if count_vertices(geometry) <= max_vertices:
        return geometry
    tolerance = 0.00001
    for _ in range(20):
        simplified = geometry.simplify(tolerance, preserve_topology=True)
        if not simplified.is_valid:
            simplified = make_valid(simplified)
        if count_vertices(simplified) <= max_vertices:
            return simplified
        tolerance *= 2
    return simplified


def to_convex_hull(geometry):
    """Fallback: return convex hull of geometry."""
    return geometry.convex_hull


def preprocess_geometry(
    geojson_dict,
    max_area=MAX_AREA,
    min_area=MIN_AREA,
    max_vertices=MAX_VERTICES,
    max_polygons=MAX_POLYGONS,
):
    """Main pipeline: validate, orient, check area, simplify, return GeoJSON dict."""
    shapely_geom = validate_geometry(geojson_dict)
    shapely_geom = enforce_winding_order(shapely_geom)
    if not check_area_bounds(geojson_dict, max_area, min_area):
        warnings.warn(f"Geometry area outside bounds [{min_area}, {max_area}] m²")
    shapely_geom = simplify_if_needed(shapely_geom, max_vertices)
    if count_polygons(shapely_geom) > max_polygons:
        warnings.warn(
            f"Geometry has {count_polygons(shapely_geom)} polygons, exceeds max {max_polygons}"
        )
    return json.loads(shapely.to_geojson(shapely_geom))
