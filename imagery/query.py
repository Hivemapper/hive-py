"""
  python -m imagery.query --geojson mygeo.json\
    --start_day 2023-06-01 --end_day 2023-06-08\
    --output_dir hm_out --authorization xxx
"""
import argparse
import concurrent.futures
import geopy.distance
import json
import os
import requests
import shutil

from area import area
from datetime import datetime, timedelta
from itertools import repeat
from geographiclib.geodesic import Geodesic

DEFAULT_STITCH_MAX_DISTANCE = 20
DEFAULT_STITCH_MAX_LAG = 300
DEFAULT_STITCH_MAX_ANGLE = 30
DEFAULT_THREADS = 20
IMAGERY_API_URL = 'https://hivemapper.com/api/developer/imagery/poly'
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

def download_files(frames, local_dir, preserve_dirs = True, num_threads=DEFAULT_THREADS, verbose=False):
  urls = [frame.get('url') for frame in frames]
  if preserve_dirs:
    img_paths = [url.split('.com/')[1].split('?')[0] for url in urls]
    meta_paths = [url.split('.com/')[1].split('?')[0].replace('keyframes', 'metadata') for url in urls]
  else:
    img_paths = ["{}.jpg".format(i) for i in range(len(frames))]
    meta_paths = ["{}.json".format(i) for i in range(len(frames))]
  local_img_paths = [os.path.join(local_dir, path) for path in img_paths]
  local_meta_paths = [os.path.join(local_dir, path) for path in meta_paths]
  for frame, meta_path in zip(frames, local_meta_paths):
    os.makedirs(os.path.dirname(meta_path), exist_ok=True)
    with open(meta_path, 'w') as f:
      meta = {key : frame[key] for key in frame if key != 'url'}
      json.dump(meta, f, indent=4)

  with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
    executor.map(download_file, urls, local_img_paths, repeat(verbose))

def query_imagery(features, weeks, authorization, local_dir, verbose=False):
  headers = {
    "content-type": "application/json",
    "authorization": f'Basic {authorization}',
  }
  frames = []

  for feature in features:
    data = feature.get('geometry', feature)
    assert(area(data) <= MAX_AREA)

    for week in weeks:
      url = f'{IMAGERY_API_URL}?week={week}'
      if verbose:
        print(url)

      with requests.post(url, data=json.dumps(data), headers=headers) as r:
        r.raise_for_status()
        resp = r.json()
        frames += resp.get('frames', [])

  return frames

def query_frames(geojson_file, start_day, end_day, output_dir, authorization, verbose = False):
  assert(start_day <= end_day)

  features = []
  with open(geojson_file, 'r') as f:
    fc = json.load(f)
    features += fc.get('features', [fc])

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

  frames = query_imagery(features, weeks, authorization, output_dir, verbose)
  filtered_frames = [frame for frame in frames if frame_within_day_bounds(frame, start_day, end_day)]

  return filtered_frames

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

def abs_angular_delta(a, b):
  delta = abs(a - b)
  return delta if delta <= 180 else 360 - delta

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
    delta_azi = abs_angular_delta(azi_a, azi_b)

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
  geojson_file,
  start_day,
  end_day,
  output_dir,
  authorization,
  export_geojson=False,
  should_stitch=False,
  max_dist=DEFAULT_STITCH_MAX_DISTANCE,
  max_lag=DEFAULT_STITCH_MAX_LAG,
  max_angle=DEFAULT_STITCH_MAX_ANGLE,
  num_threads=DEFAULT_THREADS,
  verbose=False,
):
  frames = query_frames(geojson_file, start_day, end_day, output_dir, authorization, verbose)
  print(f'Found {len(frames)} images!')

  if frames:
    if verbose:
      print(f'Downloading with {num_threads} threads...')

    if should_stitch:
      stitched = stitch(frames, max_dist, max_lag, max_angle, verbose)
      for i, frame_set in enumerate(stitched):
        local_dir = os.path.join(output_dir, str(i))
        download_files(frame_set, local_dir, False, num_threads, verbose)
      if export_geojson:
        write_geojson(stitched, output_dir, False, verbose)
    else:
      download_files(frames, output_dir, True, num_threads, verbose)
      if export_geojson:
        write_geojson([frames], output_dir, True, verbose)
    
    print(f'{len(frames)} frames saved to {output_dir}!')

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('-i', '--geojson', type=str, required=True)
  parser.add_argument('-s', '--start_day', type=valid_date, required=True)
  parser.add_argument('-e', '--end_day', type=valid_date, required=True)
  parser.add_argument('-x', '--stitch', action='store_true')
  parser.add_argument('-d', '--max_dist', type=float, default=DEFAULT_STITCH_MAX_DISTANCE)
  parser.add_argument('-l', '--max_lag', type=float, default=DEFAULT_STITCH_MAX_ANGLE)
  parser.add_argument('-z', '--max_angle', type=float, default=DEFAULT_STITCH_MAX_LAG)
  parser.add_argument('-o', '--output_dir', type=str, required=True)
  parser.add_argument('-g', '--export_geojson', action='store_true')
  parser.add_argument('-a', '--authorization', type=str, required=True)
  parser.add_argument('-c', '--num_threads', type=int, default=DEFAULT_THREADS)
  parser.add_argument('-v', '--verbose', action='store_true')
  args = parser.parse_args()

  query(
    args.geojson,
    args.start_day,
    args.end_day,
    args.output_dir,
    args.authorization,
    args.export_geojson,
    args.stitch,
    args.max_dist,
    args.max_lag,
    args.max_angle,
    args.num_threads,
    args.verbose,
  )
