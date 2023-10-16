import argparse
import csv
import geopy.distance
import json
import math
import shapefile
import shapely

from area import area
from geographiclib.geodesic import Geodesic
from pyproj import Transformer
from shapely.geometry import box, Polygon, MultiPolygon, GeometryCollection
from tqdm import tqdm

AREA_LIMIT = 1000000
MAX_MULTIPOLYGON_CARDINALITY = 8
DEFAULT_WIDTH = 25
MERCATOR_TO_WGS = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
WGS_TO_MERCATOR = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

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
        for e in c:
            if isinstance(e, (Polygon, MultiPolygon)):
                result.extend(katana(e, threshold, count+1))
    if count > 0:
        return result
    # convert multipart into singlepart
    final_result = []
    for g in result:
        if isinstance(g, MultiPolygon):
            final_result.extend(g)
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

def chunk_by_area(feature, limit = AREA_LIMIT):
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

def convert_to_geojson_poly(feature, width = DEFAULT_WIDTH):
  geom = feature.get('geometry', feature)
  t = geom['type']

  if t == 'LineString':
    return geojson_linestring_to_poly(geom, width)
  elif t == 'Point':
    return geojson_point_to_poly(geom, width)
  elif t == 'Polygon' or t == 'MultiPolygon':
    return chunk_by_area(feature)
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
      polygons.append(convert_to_geojson_poly(f, width))
  else:
    polygons = [convert_to_geojson_poly(f, width) for f in features]

  new_polygons = []
  for maybe_polys in polygons:
    if type(maybe_polys) is list:
      for poly in maybe_polys:
          new_polygons.append(poly)
    else:
      new_polygons.append(maybe_polys)

  if out_path:
    if verbose:
      print(f'writing to {out_path}...')

    with open(out_path, 'w') as f:
      json.dump({
        'type': 'FeatureCollection',
        'features': new_polygons,
        }, f)

  return new_polygons

def transform_csv_to_geojson_polygons(
  file_path,
  out_path = None,
  width = DEFAULT_WIDTH,
  custom_id_field = None,
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
    for row in reader:
      # figure out the coordinates indices
      if lon_idx == -1 or lat_idx == -1 or (custom_id_idx == -1 and custom_id_field is not None):
        for i, col in enumerate(row):
          if col.lower() == 'latitude' or col.lower() == 'lat':
            lat_idx = i
          elif col.lower() == 'longitude' or col.lower() == 'lon':
            lon_idx = i
          elif custom_id_field is not None and col.lower() == custom_id_field:
            custom_id_idx = i
        continue

      lon = row[lon_idx]
      lat = row[lat_idx]
      custom_id = None if custom_id_idx == -1 else row[custom_id_idx]
      coords.append((lon, lat, custom_id))

  if verbose:
    print(f'converting {len(coords)} coords to polygons...')
    polygons = []
    for p in tqdm(coords):
      polygons.append(point_to_square(p, width))
  else:
    polygons = [point_to_square(p, width) for p in coords]

  if custom_id_idx > -1 and custom_id_field is not None:
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

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('-s', '--shapefile', type=str, required=False)
  parser.add_argument('-c', '--csvfile', type=str, required=False)
  parser.add_argument('-o', '--output_json', type=str, required=True)
  parser.add_argument('-w', '--width', type=int, default=DEFAULT_WIDTH)
  parser.add_argument('-I', '--custom_id_field', type=str)
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
      not args.quiet,
    )
