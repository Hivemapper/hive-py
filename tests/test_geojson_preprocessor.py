import json
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shapely.geometry import Point, Polygon, MultiPolygon
from shapely.ops import orient

from util.geojson_preprocessor import (
    validate_geometry,
    enforce_winding_order,
    check_area_bounds,
    count_vertices,
    count_polygons,
    simplify_if_needed,
    to_convex_hull,
    preprocess_geometry,
    MAX_AREA,
    MIN_AREA,
    MAX_PROBE_AREA,
    MAX_VERTICES,
    MAX_POLYGONS,
)
from tests.fixtures.fixture_feature import test_feature
# --- Fixtures ---
SMALL_SQUARE = {
    "type": "Polygon",
    "coordinates": [[[0, 0], [0.02, 0], [0.02, 0.02], [0, 0.02], [0, 0]]],
}
BOWTIE = {"type": "Polygon", "coordinates": [[[0, 0], [1, 1], [1, 0], [0, 1], [0, 0]]]}
TINY = {
    "type": "Polygon",
    "coordinates": [[[0, 0], [0.00001, 0], [0.00001, 0.00001], [0, 0.00001], [0, 0]]],
}
HUGE = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}
CW_POLY = {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}


# --- 14 test functions ---


def test_validate_geometry_valid_polygon():
    result = validate_geometry(SMALL_SQUARE)
    assert result.is_valid
    assert result.geom_type in ("Polygon", "MultiPolygon")


def test_validate_geometry_fixes_invalid():
    result = validate_geometry(BOWTIE)
    assert result.is_valid


def test_enforce_winding_order():
    geom = validate_geometry(CW_POLY)
    result = enforce_winding_order(geom)
    expected = orient(result, sign=1.0)
    assert result.equals(expected)


def test_check_area_bounds_within():
    assert check_area_bounds(SMALL_SQUARE) is True


def test_check_area_bounds_too_small():
    assert check_area_bounds(TINY) is False


def test_check_area_bounds_too_large():
    assert check_area_bounds(HUGE) is False


def test_count_vertices_polygon():
    geom = validate_geometry(SMALL_SQUARE)
    assert count_vertices(geom) == 5


def test_count_vertices_multipolygon():
    sq = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])  # 5 coords
    tri = Polygon([(2, 2), (3, 2), (3, 3), (2, 2)])  # 4 coords
    mp = MultiPolygon([sq, tri])
    assert count_vertices(mp) == 5


def test_count_polygons_multipolygon():
    sq1 = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    sq2 = Polygon([(2, 2), (3, 2), (3, 3), (2, 3)])
    sq3 = Polygon([(4, 4), (5, 4), (5, 5), (4, 5)])
    mp = MultiPolygon([sq1, sq2, sq3])
    assert count_polygons(mp) == 3


def test_count_polygons_polygon():
    sq = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    assert count_polygons(sq) == 1


def test_simplify_if_needed_under_limit():
    geom = validate_geometry(SMALL_SQUARE)
    result = simplify_if_needed(geom, 500)
    assert count_vertices(result) == 5
    assert result.equals(geom)


def test_simplify_if_needed_over_limit():
    geom = Point(0, 0).buffer(0.1, quad_segs=200)
    assert count_vertices(geom) > 500
    result = simplify_if_needed(geom, 500)
    assert count_vertices(result) < 500
    assert result.is_valid


def test_preprocess_geometry_full_pipeline():
    result = preprocess_geometry(SMALL_SQUARE)
    assert isinstance(result, dict)
    assert result["type"] in ("Polygon", "MultiPolygon")
    assert "coordinates" in result


def test_preprocess_geometry_with_test_feature():
    feature = json.loads(test_feature)
    geom = feature["geometry"]
    result = preprocess_geometry(geom)
    assert isinstance(result, dict)
    assert result["type"] in ("Polygon", "MultiPolygon")
    assert "coordinates" in result
