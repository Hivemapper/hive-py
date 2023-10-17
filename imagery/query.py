import argparse
import concurrent.futures
import geopy.distance
import json
import os
import requests
import shutil
import uuid

from area import area
from datetime import datetime, timedelta
from itertools import repeat
from geographiclib.geodesic import Geodesic
from tqdm import tqdm
from util import geo

DEFAULT_STITCH_MAX_DISTANCE = 20
DEFAULT_STITCH_MAX_LAG = 300
DEFAULT_STITCH_MAX_ANGLE = 30
DEFAULT_THREADS = 20
DEFAULT_WIDTH = 25
IMAGERY_API_URL = 'https://hivemapper.com/api/developer/imagery/poly'
LATEST_IMAGERY_API_URL = 'https://hivemapper.com/api/developer/latest/poly'
MAX_AREA = 1000 * 1000 # 1km^2

def valid_date(s):
  try:
    return datetime.strptime(s, "%Y-%m-%d")
  except ValueError:
    msg = "not a valid date: {0!r}".format(s)
    raise argparse.ArgumentTypeError(msg)

def download_file(url, local_path, verbose=False):
  os.makedirs(os.path.dirname(local_path), exist_ok=True)
  clean = url.split('?')[0].split('.com/')[1]
  if verbose:
    print("GET {} => {}".format(clean, local_path))

  with requests.get(url, stream=True) as r:
    r.raise_for_status()
    with open(local_path, 'wb') as f:
      shutil.copyfileobj(r.raw, f)

def download_files(
  frames,
  local_dir,
  preserve_dirs=True,
  merge_metadata=False,
  num_threads=DEFAULT_THREADS,
  verbose=False,
):
  urls = [frame.get('url') for frame in frames]
  if preserve_dirs:
    img_paths = [url.split('.com/')[1].split('?')[0] for url in urls]
    meta_paths = [
      url.split('.com/')[1].split('?')[0]
        .replace('keyframes', 'metadata')
        .replace('.jpg', '.json')
      for url in urls
    ]
  else:
    img_paths = ["{}.jpg".format(i) for i in range(len(frames))]
    meta_paths = ["{}.json".format(i) for i in range(len(frames))]
  local_img_paths = [os.path.join(local_dir, path) for path in img_paths]
  local_meta_paths = [os.path.join(local_dir, path) for path in meta_paths]

  if len(frames) == 0:
    return

  if merge_metadata:
    local_meta_path = os.path.join(local_dir, 'meta.json')
    os.makedirs(os.path.dirname(local_meta_path), exist_ok=True)
    meta = {
      img_path:
        { key: frame[key] for key in frame if key != 'url' }
        for frame in frames
      for img_path in img_paths
    }
    with open(local_meta_path, 'w') as f:
      json.dump(meta, f, indent=4)
  else:
    for frame, meta_path in zip(frames, local_meta_paths):
      os.makedirs(os.path.dirname(meta_path), exist_ok=True)
      with open(meta_path, 'w') as f:
        meta = {key : frame[key] for key in frame if key != 'url'}
        json.dump(meta, f, indent=4)

  with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
    executor.map(download_file, urls, local_img_paths, repeat(verbose))

def query_imagery(features, weeks, custom_ids, authorization, local_dir, verbose=False):
  headers = {
    "content-type": "application/json",
    "authorization": f'Basic {authorization}',
  }
  frames = []

  itr = features if not verbose else tqdm(features)
  for feature, custom_id in zip(itr, custom_ids):
    data = feature.get('geometry', feature)
    assert(area(data) <= MAX_AREA)

    for week in weeks:
      url = f'{IMAGERY_API_URL}?week={week}'
      if verbose:
        print(url)

      with requests.post(url, data=json.dumps(data), headers=headers) as r:
        r.raise_for_status()
        resp = r.json()
        results = resp.get('frames', [])
        if custom_id is not None:
          for result in results:
            result['id'] = custom_id
        frames += results

  return frames

def query_latest_imagery(features, custom_ids, min_days, authorization, local_dir, verbose=False):
  headers = {
    "content-type": "application/json",
    "authorization": f'Basic {authorization}',
  }
  frames = []

  itr = features if not verbose else tqdm(features)
  for feature, custom_id, min_day in zip(itr, custom_ids, min_days):
    data = feature.get('geometry', feature)
    assert(area(data) <= MAX_AREA)

    url = LATEST_IMAGERY_API_URL
    if min_day:
      url += f'?min_week={min_day}'
    if verbose:
      print(url)

    with requests.post(url, data=json.dumps(data), headers=headers) as r:
      r.raise_for_status()
      resp = r.json()
      results = resp.get('frames', [])
      if custom_id is not None:
        for result in results:
          result['id'] = custom_id
      frames += results

  return frames

def query_frames(geojson_file, start_day, end_day, output_dir, authorization, verbose = False):
  assert(start_day <= end_day)

  features = []
  with open(geojson_file, 'r') as f:
    fc = json.load(f)
    features += fc.get('features', [fc])

  custom_ids = []
  for feature in features:
    properties = feature.get('properties', {})
    custom_ids.append(properties.get('id', None))

  features = [geo.convert_to_geojson_poly(f) for f in features]
  new_features = []
  for feature in features:
    if type(feature) is list:
      for f in feature:
        new_features.append(f)
    else:
      new_features.append(feature)
  features = new_features

  assert(len(features))

  s = start_day
  weeks = [s.strftime("%Y-%m-%d"), end_day.strftime("%Y-%m-%d")]
  while s < end_day - timedelta(days=7):
    s += timedelta(days=7)
    weeks.append(s.strftime("%Y-%m-%d"))
  weeks = list(set(weeks))

  assert(len(weeks))

  if verbose:
    print(f'Querying {len(features)} features for imagery across {len(weeks)} weeks...')
  frames = query_imagery(features, weeks, custom_ids, authorization, output_dir, verbose)
  filtered_frames = [frame for frame in frames if frame_within_day_bounds(frame, start_day, end_day)]

  return filtered_frames

def query_latest_frames(geojson_file, output_dir, authorization, verbose = False):
  features = []
  with open(geojson_file, 'r') as f:
    fc = json.load(f)
    features += fc.get('features', [fc])

  custom_ids = []
  min_dates = []
  for feature in features:
    properties = feature.get('properties', {})
    custom_ids.append(properties.get('id', None))
    min_dates.append(properties.get('min_date', None))

  features = [geo.convert_to_geojson_poly(f) for f in features]
  new_features = []
  for feature in features:
    if type(feature) is list:
      for f in feature:
        new_features.append(f)
    else:
      new_features.append(feature)
  features = new_features

  assert(len(features))

  if verbose:
    print(f'Querying {len(features)} features for imagery across for latest...')
  frames = query_latest_imagery(features, custom_ids, min_dates, authorization, output_dir, verbose)

  return frames


def frame_within_day_bounds(frame, start_day, end_day):
  d = datetime.fromisoformat(frame.get('timestamp').split('.')[0])
  return d >= start_day and d <= end_day + timedelta(days=1)

def json_iso_str_to_date(s):
  return datetime.fromisoformat(s.replace('Z', ''))

def boundaries_intersect(a, b):
  a0 = json_iso_str_to_date(a[0].get('timestamp'))
  a1 = json_iso_str_to_date(a[-1].get('timestamp'))
  b0 = json_iso_str_to_date(b[0].get('timestamp'))
  b1 = json_iso_str_to_date(b[-1].get('timestamp'))

  latest_start = max(a0, b0)
  earliest_end = min(a1, b1)
  return latest_start < earliest_end  

def stitch(
  frames,
  max_dist = DEFAULT_STITCH_MAX_DISTANCE,
  max_lag = DEFAULT_STITCH_MAX_LAG,
  max_azimuth_delta = DEFAULT_STITCH_MAX_ANGLE,
  verbose = False
):
  if verbose:
    print(f'Stitching {len(frames)} frames...')
  by_sequence = {}
  for frame in frames:
    sequence = frame.get('sequence')
    by_sequence.setdefault(sequence, [])
    by_sequence[sequence].append(frame)

  seqs = []
  for seq in by_sequence.values():
    if len(seq) > 1:
      seqs.append(sorted(seq, key=lambda f: f.get('idx')))
  seqs = sorted(seqs, key=lambda s: s[0].get('timestamp'))

  colls = [[seqs.pop(0)]]

  if len(frames) == 0:
    return colls

  cur_coll = colls[-1]
  remaining = []

  while seqs:
    seq = seqs.pop(0)

    if boundaries_intersect(cur_coll[-1], seq):
      remaining.append(seq)

      if len(seqs) == 0:
        colls.append([remaining.pop(0)])
        cur_coll = colls[-1]
        seqs = remaining
        remaining = []

      continue

    t0 = json_iso_str_to_date(cur_coll[-1][-1].get('timestamp'))
    t1 = json_iso_str_to_date(seq[0].get('timestamp'))

    if (t1 - t0).seconds > max_lag:
      remaining.append(seq)
  
      if len(seqs) == 0:
        colls.append([remaining.pop(0)])
        cur_coll = colls[-1]
        seqs = remaining
        remaining = []

      continue

    lat_a0 = cur_coll[-1][-2].get('position').get('lat')
    lon_a0 = cur_coll[-1][-2].get('position').get('lon')
    lat_a1 = cur_coll[-1][-1].get('position').get('lat')
    lon_a1 = cur_coll[-1][-1].get('position').get('lon')
    lat_b0 = seq[0].get('position').get('lat')
    lon_b0 = seq[0].get('position').get('lon')
    lat_b1 = seq[1].get('position').get('lat')
    lon_b1 = seq[1].get('position').get('lon')

    d = geopy.distance.distance((lat_a1, lon_a1), (lat_b0, lon_b0)).meters

    if d > max_dist:
      remaining.append(seq)

      if len(seqs) == 0:
        colls.append([remaining.pop(0)])
        cur_coll = colls[-1]
        seqs = remaining
        remaining = []

      continue

    azi_a = Geodesic.WGS84.Inverse(lat_a0, lon_a0, lat_a1, lon_a1).get('azi2')
    azi_b = Geodesic.WGS84.Inverse(lat_b0, lon_b0, lat_b1, lon_b1).get('azi2')
    delta_azi = geo.abs_angular_delta(azi_a, azi_b)

    if delta_azi > max_azimuth_delta:
      remaining.append(seq)

      if len(seqs) == 0:
        colls.append([remaining.pop(0)])
        cur_coll = colls[-1]
        seqs = remaining
        remaining = []

      continue      

    cur_coll.append(seq)

    if len(seqs) == 0 and remaining:
      colls.append([remaining.pop(0)])
      cur_coll = colls[-1]
      seqs = remaining
      remaining = []

  stitched = [[f for seq in coll for f in seq] for coll in colls]
  if verbose:
    print(f'Stitched {len(stitched)} paths!')

  return stitched

def frames_to_linestring(frames, ident):
  return {
    "type": "Feature",
    "properties": {
      "id": ident,
    },
    "geometry": {
      "type": "LineString",
      "coordinates": [
        [
          f.get('position').get('lon'),
          f.get('position').get('lat'),
        ] for f in frames
      ]
    }
  }

def frames_to_points(frames):
  return [{
    "type": "Feature",
    "properties": {
      "sequence": f.get('sequence'),
      "idx": f.get('idx'),
    },
    "geometry": {
      "type": "Point",
      "coordinates": [
        f.get('position').get('lon'),
        f.get('position').get('lat'),
      ]
    }
  } for f in frames]

def write_geojson(frame_lists, output_dir, points = False, verbose = False):
  if points:
    features = frames_to_points(frame_lists[0])
  else:
    features = [frames_to_linestring(frames, i) for i, frames in enumerate(frame_lists)]
  geojson = {
    "type": "FeatureCollection",
    "features": features,
  }
  geojson_path = os.path.join(output_dir, 'frames.geojson')
  with open(geojson_path, 'w') as f:
    json.dump(geojson, f)

  if verbose:
    print(f'Wrote geojson to {geojson_path}')

def query(
  file_path,
  start_day,
  end_day,
  output_dir,
  authorization,
  latest=False,
  export_geojson=False,
  should_stitch=False,
  max_dist=DEFAULT_STITCH_MAX_DISTANCE,
  max_lag=DEFAULT_STITCH_MAX_LAG,
  max_angle=DEFAULT_STITCH_MAX_ANGLE,
  width=DEFAULT_WIDTH,
  merge_metadata=False,
  custom_id_field=None,
  custom_min_date_field=None,
  num_threads=DEFAULT_THREADS,
  verbose=False,
):
  if file_path.endswith('.shp'):
    geojson_file = f'{file_path[0 : len(file_path) - 4]}.geojson_{str(uuid.uuid4())}'
    geo.transform_shapefile_to_geojson_polygons(file_path, geojson_file, width, verbose)
  elif file_path.endswith('.csv'):
    geojson_file = f'{file_path[0 : len(file_path) - 4]}.geojson_{str(uuid.uuid4())}'
    geo.transform_csv_to_geojson_polygons(
      file_path,
      geojson_file,
      width,
      custom_id_field,
      custom_min_date_field,
      verbose,
    )
  else:
    geojson_file = file_path

  if latest:
    frames = query_latest_frames(geojson_file, output_dir, authorization, verbose)
  else:
    frames = query_frames(geojson_file, start_day, end_day, output_dir, authorization, verbose)
  print(f'Found {len(frames)} images!')

  if frames:
    if verbose:
      print(f'Downloading with {num_threads} threads...')

    if should_stitch:
      stitched = stitch(frames, max_dist, max_lag, max_angle, verbose)
      for i, frame_set in enumerate(stitched):
        folder = f'{str(uuid.uuid4())}-{str(i)}'
        local_dir = os.path.join(output_dir, folder)
        download_files(frame_set, local_dir, False, merge_metadata, num_threads, verbose)
      if export_geojson:
        write_geojson(stitched, output_dir, False, verbose)
    else:
      download_files(frames, output_dir, True, merge_metadata, num_threads, verbose)
      if export_geojson:
        write_geojson([frames], output_dir, True, verbose)
    
    print(f'{len(frames)} frames saved to {output_dir}!')

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('-i', '--input_file', type=str, required=True)
  parser.add_argument('-s', '--start_day', type=valid_date)
  parser.add_argument('-e', '--end_day', type=valid_date)
  parser.add_argument('-L', '--latest', action='store_true')
  parser.add_argument('-x', '--stitch', action='store_true')
  parser.add_argument('-d', '--max_dist', type=float, default=DEFAULT_STITCH_MAX_DISTANCE)
  parser.add_argument('-l', '--max_lag', type=float, default=DEFAULT_STITCH_MAX_ANGLE)
  parser.add_argument('-z', '--max_angle', type=float, default=DEFAULT_STITCH_MAX_LAG)
  parser.add_argument('-o', '--output_dir', type=str, required=True)
  parser.add_argument('-g', '--export_geojson', action='store_true')
  parser.add_argument('-w', '--width', type=int, default=DEFAULT_WIDTH)
  parser.add_argument('-M', '--merge_metadata', action='store_true')
  parser.add_argument('-I', '--custom_id_field', type=str)
  parser.add_argument('-S', '--custom_min_date_field', type=str)
  parser.add_argument('-a', '--authorization', type=str, required=True)
  parser.add_argument('-c', '--num_threads', type=int, default=DEFAULT_THREADS)
  parser.add_argument('-v', '--verbose', action='store_true')
  args = parser.parse_args()

  query(
    args.input_file,
    args.start_day,
    args.end_day,
    args.output_dir,
    args.authorization,
    args.latest,
    args.export_geojson,
    args.stitch,
    args.max_dist,
    args.max_lag,
    args.max_angle,
    args.width,
    args.merge_metadata,
    args.custom_id_field,
    args.custom_min_date_field,
    args.num_threads,
    args.verbose,
  )
