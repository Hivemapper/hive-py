import argparse
import csv
import datetime
import geopy.distance
import json
import math
import shapefile
import shapely

from area import area
from collections import Counter
from geographiclib.geodesic import Geodesic
from pyproj import Transformer
from shapely import affinity
from shapely.geometry import box, Polygon, LineString, MultiPolygon, GeometryCollection, Point, MultiPoint
from shapely.ops import split, snap, unary_union
from shapely.validation import make_valid
from tqdm import tqdm

AREA_LIMIT = 4000000
MAX_MULTIPOLYGON_CARDINALITY = 8
MIN_SUBTRAHEND_AREA = 2500 # ~100m of 25m width road
DEFAULT_WIDTH = 25
MERCATOR_TO_WGS = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
WGS_TO_MERCATOR = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

def flat_list(features):
  new_features = []
  for feature in features:
    if type(feature) is list:
      for f in feature:
        new_features.append(f)
    else:
      new_features.append(feature)
  return new_features

def explode_multipolygon(mp):
  coords = mp.get('geometry', mp).get('coordinates')
  n = len(coords)
  c = MAX_MULTIPOLYGON_CARDINALITY
  m = math.ceil(n / c)
  mps = []
  for i in range(m):
    mps.append({
      'type': 'MultiPolygon',
      'coordinates': coords[i * c : (i + 1) * c]
      })
  return mps

# from https://github.com/shapely/shapely/issues/1068#issuecomment-770296614
def complex_split(geom: LineString, splitter):
    """Split a complex linestring by another geometry without splitting at
    self-intersection points.

    Parameters
    ----------
    geom : LineString
        An optionally complex LineString.
    splitter : Geometry
        A geometry to split by.

    Warnings
    --------
    A known vulnerability is where the splitter intersects the complex
    linestring at one of the self-intersecting points of the linestring.
    In this case, only one the first path through the self-intersection
    will be split.

    Examples
    --------
    >>> complex_line_string = LineString([(0, 0), (1, 1), (1, 0), (0, 1)])
    >>> splitter = LineString([(0, 0.5), (0.5, 1)])
    >>> complex_split(complex_line_string, splitter).wkt
    'GEOMETRYCOLLECTION (LINESTRING (0 0, 1 1, 1 0, 0.25 0.75), LINESTRING (0.25 0.75, 0 1))'

    Return
    ------
    GeometryCollection
        A collection of the geometries resulting from the split.
    """
    if geom.is_simple:
        return split(geom, splitter)

    if isinstance(splitter, Polygon):
        splitter = splitter.exterior

    # Ensure that intersection exists and is zero dimensional.
    relate_str = geom.relate(splitter)
    if relate_str[0] == '1':
        raise ValueError('Cannot split LineString by a geometry which intersects a '
                         'continuous portion of the LineString.')
    if not (relate_str[0] == '0' or relate_str[1] == '0'):
        return GeometryCollection((geom,))

    intersection_points = geom.intersection(splitter)
    # This only inserts the point at the first pass of a self-intersection if
    # the point falls on a self-intersection.
    snapped_geom = snap(geom, intersection_points, tolerance=1.0e-12)  # may want to make tolerance a parameter.
    # A solution to the warning in the docstring is to roll your own split method here.
    # The current one in shapely returns early when a point is found to be part of a segment.
    # But if the point was at a self-intersection it could be part of multiple segments.
    return split(snapped_geom, intersection_points)

# https://snorfalorpagus.net/blog/2016/03/13/splitting-large-polygons-for-faster-intersections/
def katana(geometry, threshold, count=0):
    """Split a Polygon into two parts across it's shortest dimension"""
    bounds = geometry.bounds
    width = bounds[2] - bounds[0]
    height = bounds[3] - bounds[1]
    if max(width, height) <= threshold or count == 250:
        # either the polygon is smaller than the threshold, or the maximum
        # number of recursions has been reached
        return [geometry]
    if height >= width:
        # split left to right
        a = box(bounds[0], bounds[1], bounds[2], bounds[1]+height/2)
        b = box(bounds[0], bounds[1]+height/2, bounds[2], bounds[3])
    else:
        # split top to bottom
        a = box(bounds[0], bounds[1], bounds[0]+width/2, bounds[3])
        b = box(bounds[0]+width/2, bounds[1], bounds[2], bounds[3])
    result = []
    for d in (a, b,):
        c = geometry.intersection(d)
        if not isinstance(c, GeometryCollection):
            c = [c]
        else:
            c = c.geoms
        for e in c:
            if isinstance(e, (Polygon, MultiPolygon)):
                result.extend(katana(e, threshold, count+1))
    if count > 0:
        return result
    # convert multipart into singlepart
    final_result = []
    for g in result:
        if isinstance(g, MultiPolygon):
            final_result.extend(g.geoms)
        else:
            final_result.append(g)
    return final_result

def abs_angular_delta(a, b):
  delta = abs(a - b)
  return delta if delta <= 180 else 360 - delta

def angle_between_segments(p0, p1, p2):
  p0lon, p0lat = p0
  p1lon, p1lat = p1
  p2lon, p2lat = p2

  azi_a = Geodesic.WGS84.Inverse(p0lat, p0lon, p1lat, p1lon).get('azi2')
  azi_b = Geodesic.WGS84.Inverse(p1lat, p1lon, p2lat, p2lon).get('azi2')
  return abs_angular_delta(azi_a, azi_b)

def get_coords(feature):
  geo = feature.get('geometry', feature)
  return geo.get('coordinates')

def combine_polys(polys):
  return {
    "type": "Feature",
    "properties": {},
    "geometry": {
      "type": "MultiPolygon",
      "coordinates": [
        poly.get('geometry', poly).get('coordinates') for poly in polys
      ],
    },
  }

def filter_small_segments(linestring, min_length):
  coords = get_coords(linestring)

  if len(coords) < 2:
    return coords

  new_coords = [coords[0]]

  prev = coords[0]

  for i in range(1, len(coords)):
    cur = coords[i]
    d = geopy.distance.distance(
      reversed(cur),
      reversed(prev),
    ).meters

    if d < min_length:
      continue

    new_coords.append(cur)
    prev = cur

  return new_coords

def point_to_square(coord, width):
  half_width = float(width) / 2.0
  new_coords = []

  cx, cy = WGS_TO_MERCATOR.transform(*coord[0:2])

  new_coords.append(MERCATOR_TO_WGS.transform(cx - half_width, cy - half_width))
  new_coords.append(MERCATOR_TO_WGS.transform(cx + half_width, cy - half_width))
  new_coords.append(MERCATOR_TO_WGS.transform(cx + half_width, cy + half_width))
  new_coords.append(MERCATOR_TO_WGS.transform(cx - half_width, cy + half_width))
  new_coords.append(new_coords[0])

  properties = {}
  if len(coord) > 2 and coord[2] is not None:
    properties['id'] = coord[2]
  if len(coord) > 3 and coord[3] is not None:
    min_date = datetime.datetime.fromisoformat(coord[3])
    properties['min_date'] = min_date.strftime('%Y-%m-%d')

  return {
    "type": "Feature",
    "properties": properties,
    "geometry": {
      "type": "Polygon",
      "coordinates": [new_coords],
    },
  }

def explode_sharp_angles(coords, threshold = 45):
  if len(coords) < 3:
    return [coords]

  lines = []
  cur_line = [coords[0], coords[1]]

  for i in range(2, len(coords)):
    theta = angle_between_segments(coords[i - 2], coords[i - 1], coords[i])
    if theta <= threshold:
      cur_line.append(coords[i])
    else:
      lines.append(cur_line)
      # also add the one-point segment because we don't
      # support a miter/join solution for continuous solution
      lines.append([coords[i - 1]])
      cur_line = [coords[i - 1], coords[i]]

  lines.append(cur_line)

  return lines

def geojson_point_to_poly(
  point,
  width = DEFAULT_WIDTH,
):
  coord = get_coords(point)
  return point_to_square(coord, width)

def geojson_linestring_to_poly(
  linestring,
  width = DEFAULT_WIDTH,
):
  filtered_coords = filter_small_segments(linestring, width)
  half_width = float(width) / 2.0

  n = 2 * len(filtered_coords)

  new_coords = [None] * (n + 1)

  # if we're left with 1 point, convert to a box
  if len(filtered_coords) < 2:
    return point_to_square(filtered_coords[0], width)

  lines = explode_sharp_angles(filtered_coords)

  if len(lines) > 1:
    linestrings = [{
      "type": "LineString",
      "coordinates": line,
    } for line in lines]

    polys = [geojson_linestring_to_poly(line) for line in linestrings]
    return combine_polys(polys)
  else:
    p0x, p0y = WGS_TO_MERCATOR.transform(*filtered_coords[0])
    p1x, p1y = WGS_TO_MERCATOR.transform(*filtered_coords[1])

    dx, dy = (p1x - p0x, p1y - p0y)
    mag = math.sqrt(dx ** 2 + dy ** 2)
    dx /= mag
    dy /= mag
    nx, ny = (-dy * half_width, dx * half_width)

    new_coords[0] = MERCATOR_TO_WGS.transform(p0x + nx, p0y + ny)
    new_coords[n - 1] = MERCATOR_TO_WGS.transform(p0x - nx, p0y - ny)
    new_coords[n] = new_coords[0]

    for i in range(1, len(filtered_coords)):
      p0x, p0y = WGS_TO_MERCATOR.transform(*filtered_coords[i - 1])
      p1x, p1y = WGS_TO_MERCATOR.transform(*filtered_coords[i])

      dx, dy = (p1x - p0x, p1y - p0y)
      mag = math.sqrt(dx ** 2 + dy ** 2)
      dx /= mag
      dy /= mag
      nx, ny = (-dy * half_width, dx * half_width)

      new_coords[i] = MERCATOR_TO_WGS.transform(p1x + nx, p1y + ny)
      new_coords[n - i - 1] = MERCATOR_TO_WGS.transform(p1x - nx, p1y - ny)

    return {
      "type": "Feature",
      "properties": {},
      "geometry": {
        "type": "Polygon",
        "coordinates": [new_coords],
      },
    }

def chunk_by_area(feature, limit = AREA_LIMIT, verbose = False):
  try:
    geom = feature.get('geometry', feature)
    if area(geom) < AREA_LIMIT:
      return feature

    shapely_poly = shapely.from_geojson(json.dumps(feature))
    geoms = [
      json.loads(elt) for elt in shapely.to_geojson(
        katana(shapely_poly, 0.01)
      ).tolist()
    ]

    return [{
      "type": "Feature",
      "properties": {},
      "geometry": geometry,
    } for geometry in geoms]
  except Exception as e:
    if verbose:
      print(f'Geometry: {geom}')  
    raise

# from https://gis.stackexchange.com/questions/435879/python-shapely-split-a-complex-line-at-self-intersections
def find_self_intersection(line):
    intersection = None
    if not line.is_simple:
        intersection = unary_union(line)
        seg_coordinates = []
        for seg in intersection.geoms:
            seg_coordinates.extend(list(seg.coords))
        intersection = [Point(p) for p, c in Counter(seg_coordinates).items() if c > 1]
        intersection = MultiPoint(intersection)
    return intersection

def convert_to_geojson_poly(feature, width = DEFAULT_WIDTH, verbose = False, max_area = AREA_LIMIT):
  geom = feature.get('geometry', feature)
  t = geom['type']
  if t == 'LineString':
    try:
      s = shapely.from_geojson(json.dumps(geom))
      if not s.is_simple:
        x = find_self_intersection(s)
        d = s.difference(x)
        sp = complex_split(d, x)
        ls = [json.loads(shapely.to_geojson(g)) for g in sp.geoms]
        polys = [geojson_linestring_to_poly(l, width) for l in ls]
        spolys = [shapely.from_geojson(json.dumps(p)) for p in polys]
        mpoly = unary_union(spolys)
        return json.loads(shapely.to_geojson(mpoly))
    except:
      return None
    return geojson_linestring_to_poly(geom, width)
    # s = shapely.from_geojson(json.dumps(gj))
    # return json.loads(shapely.to_geojson(unary_union(s)))
  elif t == 'Point':
    return geojson_point_to_poly(geom, width)
  elif t == 'Polygon' or t == 'MultiPolygon':
    return chunk_by_area(feature, max_area, verbose)
  elif t == 'GeometryCollection':
    polys = [convert_to_geojson_poly(p, width, verbose) for p in geom.get('geometries', [])]
    polys = flat_list(polys)
    spolys = [shapely.from_geojson(json.dumps(p)) for p in polys]
    mpoly = unary_union(spolys)
    if not mpoly.is_valid:
      mpoly = make_valid(mpoly)
    return convert_to_geojson_poly(json.loads(shapely.to_geojson(mpoly)), width, verbose)
  else:
    raise Exception(f'Unsupported type: {t}')

def transform_shapefile_to_geojson_polygons(file_path, out_path = None, width = DEFAULT_WIDTH, verbose = False):
  geojson = {}

  if verbose:
    print(f'reading {file_path} as geojson...')
  with shapefile.Reader(file_path) as shp:
    geojson = shp.__geo_interface__

  features = geojson.get('features')
  if verbose:
    print(f'converting {len(features)} features to polygons...')

  if verbose:
    polygons = []
    for f in tqdm(features):
      polygons.append(convert_to_geojson_poly(f, width, verbose))
  else:
    polygons = [convert_to_geojson_poly(f, width, verbose) for f in features]

  polygons = [polygon for polygon in polygons if polygon is not None]
  polygons = flat_list(polygons)
  polygons = [shapely.from_geojson(json.dumps(f)) for f in polygons]
  for i, p in enumerate(polygons):
    if not p.is_valid:
      polygons[i] = make_valid(p)
  polygons = unary_union(polygons)
  if not polygons.is_valid:
    polygons = make_valid(polygons)
  polygons = json.loads(shapely.to_geojson(polygons))
  polygons = [convert_to_geojson_poly(polygons, width, verbose)]
  polygons = [p for p in polygons if p is not None]
  polygons = flat_list(polygons)

  if out_path:
    if verbose:
      print(f'writing to {out_path}...')

    with open(out_path, 'w') as f:
      json.dump({
        'type': 'FeatureCollection',
        'features': polygons,
        }, f)

  return polygons

def transform_csv_to_geojson_polygons(
  file_path,
  out_path = None,
  width = DEFAULT_WIDTH,
  custom_id_field = None,
  custom_min_date_field = None,
  custom_date_formatting=None,
  verbose = False
):
  mp_lim = min(
    MAX_MULTIPOLYGON_CARDINALITY,
    max(2, AREA_LIMIT // (width ** 2)) - 1,
  )

  geojson = {}

  if verbose:
    print(f'reading {file_path} as geojson...')
  coords = []
  with open(file_path, newline='') as csvfile:
    reader = csv.reader(csvfile)
    lon_idx = -1
    lat_idx = -1
    custom_id_idx = -1
    custom_min_date_idx = -1
    for row in reader:
      # figure out the coordinates indices
      needs_id_field = custom_id_idx == -1 and custom_id_field is not None
      needs_min_date_field = custom_min_date_field == -1 and custom_min_date_field is not None
      if lon_idx == -1 or lat_idx == -1 or needs_id_field or needs_min_date_field:
        for i, col in enumerate(row):
          if col.lower() == 'latitude' or col.lower() == 'lat':
            lat_idx = i
          elif col.lower() == 'longitude' or col.lower() == 'lon':
            lon_idx = i
          elif custom_id_field is not None and col == custom_id_field:
            custom_id_idx = i
          elif custom_min_date_field is not None and col == custom_min_date_field:
            custom_min_date_idx = i
        continue

      lon = row[lon_idx]
      lat = row[lat_idx]
      custom_id = None if custom_id_idx == -1 else row[custom_id_idx]
      custom_min_date = None if custom_min_date_field is None else row[custom_min_date_idx]
      if custom_min_date is not None and custom_date_formatting is not None:
        custom_min_date = datetime.datetime.strptime(
          custom_min_date,
          custom_date_formatting
        ).strftime('%Y-%m-%d')
      coords.append((lon, lat, custom_id, custom_min_date))

  if verbose:
    print(f'converting {len(coords)} coords to polygons...')
    polygons = []
    for p in tqdm(coords):
      polygons.append(point_to_square(p, width))
  else:
    polygons = [point_to_square(p, width) for p in coords]

  has_custom_id_field = custom_id_idx > -1 and custom_id_field is not None
  has_custom_min_date_field = custom_min_date_idx > -1 and custom_min_date_field is not None
  if has_custom_id_field or has_custom_min_date_field:
    multi_polys = polygons
  else:
    multi_polys = []
    for i in range(math.ceil(len(polygons) / mp_lim)):
      multi_polys.append(
        combine_polys(
          polygons[i * mp_lim : (i + 1) * mp_lim]
        )
      )

  if out_path:
    if verbose:
      print(f'writing to {out_path}...')

    with open(out_path, 'w') as f:
      json.dump({
        'type': 'FeatureCollection',
        'features': multi_polys,
        }, f)

  return polygons

def union_each_feature_inplace(features):
  for i, feature in enumerate(features):
    f = shapely.from_geojson(json.dumps(feature))
    if not f.is_valid:
      f = make_valid(f)
    try:
      features[i] = shapely.unary_union(f)
    except shapely.errors.GEOSException as e:
      features[i] = None

def to_shapely(features):
  return [shapely.from_geojson(json.dumps(f)) for f in features]

def union_features(features):
  try:
    return shapely.unary_union(features)
  except shapely.errors.GEOSException as e:
    return shapely.unary_union([f for f in features if f.is_valid])

def subtract_geojson(
  minuend_in,
  subtrahend_in,
  delta_out,
  width=DEFAULT_WIDTH,
  verbose=False,
):
  minuend_features = []
  subtrahend_features = []

  with open(minuend_in, 'r') as f:
    fc = json.load(f)
    minuend_features += fc.get('features', [fc])

  with open(subtrahend_in, 'r') as f:
    fc = json.load(f)
    subtrahend_features += fc.get('features', [fc])

  minuend_features = [convert_to_geojson_poly(f, width) for f in minuend_features]
  minuend_features = [feature for feature in minuend_features if feature is not None]
  minuend_features = flat_list(minuend_features)
  subtrahend_features = [convert_to_geojson_poly(f, width * 1.25) for f in subtrahend_features]
  subtrahend_features = [feature.get('geometry', feature) for feature in subtrahend_features if feature is not None]
  subtrahend_features = [f for f in subtrahend_features if area(f) >= MIN_SUBTRAHEND_AREA]
  subtrahend_features = flat_list(subtrahend_features)

  delta = None
  if subtrahend_features:
    delta = shapely.difference(
      union_features(to_shapely(minuend_features)),
      union_features(to_shapely(subtrahend_features)))
  else:
    delta = to_shapely(minuend_features)

  if type(delta) != list:
    delta = [delta]

  for i, p in enumerate(delta):
    if not p.is_valid:
      delta[i] = make_valid(p)

  delta = unary_union(delta)
  if not delta.is_valid:
    delta = make_valid(delta)

  delta = json.loads(shapely.to_geojson(delta))
  delta = [convert_to_geojson_poly(delta, width)]
  delta = [p for p in delta if p is not None]
  delta = flat_list(delta)
  delta = [{
    "type": "Feature",
    "geometry": g.get('feature', g).get('geometry', g),
    "properties": {},
  } for g in delta]

  if verbose:
    print(f'writing delta geojson to {delta_out}...')

  with open(delta_out, 'w') as f:
    json.dump({
      'type': 'FeatureCollection',
      'features': delta,
      }, f)

  return delta

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('-s', '--shapefile', type=str, required=False)
  parser.add_argument('-c', '--csvfile', type=str, required=False)
  parser.add_argument('-o', '--output_json', type=str, required=True)
  parser.add_argument('-w', '--width', type=int, default=DEFAULT_WIDTH)
  parser.add_argument('-I', '--custom_id_field', type=str)
  parser.add_argument('-S', '--custom_min_date_field', type=str)
  parser.add_argument('-q', '--quiet', action='store_true')
  args = parser.parse_args()

  if not args.shapefile and not args.csvfile:
    print('No input shp or csv')
  elif args.shapefile and args.csvfile:
    print('Too many inputs (shp and csv)')
  elif args.shapefile:
    transform_shapefile_to_geojson_polygons(
      args.shapefile,
      args.output_json,
      args.width,
      not args.quiet,
    )
  elif args.csv:
    transform_csv_to_geojson_polygons(
      args.csvfile,
      args.output_json,
      args.width,
      args.custom_id_field,
      args.custom_min_date_field,
      not args.quiet,
    )
