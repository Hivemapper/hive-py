import argparse
import concurrent.futures
import geopy.distance
import hashlib
import json
import os
import requests
import shutil
import uuid

from area import area
from datetime import datetime, timedelta
from exiftool import ExifToolHelper
from itertools import repeat
from geographiclib.geodesic import Geodesic
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm
from urllib.parse import quote
from util import geo
from imagery.processing import clahe_smart_clip

BATCH_SIZE = 10000
CACHE_DIR = '.hivepy_cache'
DEFAULT_BACKOFF = 1.0
DEFAULT_RETRIES = 10
DEFAULT_STITCH_MAX_DISTANCE = 20
DEFAULT_STITCH_MAX_LAG = 300
DEFAULT_STITCH_MAX_ANGLE = 30
DEFAULT_THREADS = 20
DEFAULT_WIDTH = 25
IMAGERY_API_URL = 'https://hivemapper.com/api/developer/imagery/poly'
LATEST_IMAGERY_API_URL = 'https://hivemapper.com/api/developer/latest/poly'
RENEW_ASSET_URL = 'https://hivemapper.com/api/developer/renew/';
MAX_API_THREADS = 8
MAX_AREA = 1000 * 1000 # 1km^2
STATUS_FORCELIST = [429, 500, 502, 503, 504]
VALID_POST_PROCESSING_OPTS = ['clahe-smart-clip']

request_session = requests.Session()
retries = Retry(
  total=DEFAULT_RETRIES,
  backoff_factor=DEFAULT_BACKOFF,
  status_forcelist=STATUS_FORCELIST,
  raise_on_status=True,
  allowed_methods=['GET', 'POST'],
)
request_session.mount('http://', HTTPAdapter(max_retries=retries))
request_session.mount('https://', HTTPAdapter(max_retries=retries))

# to be lazy loaded
CAMERA_INFO = {}

def setup_cache(verbose = True):
  if verbose:
    print(f'Making cache dir: {CACHE_DIR}')
  os.makedirs(CACHE_DIR, exist_ok=True)  

def clear_cache(verbose = True):
  if verbose:
    print(f'Deleting cache dir: {CACHE_DIR}')
  shutil.rmtree(CACHE_DIR)

def post_cached(url, data, headers, verbose=True, use_cache=True, pbar=None):
  loc = None
  if use_cache:
    str_data = json.dumps({ 'url': url, 'data': data }).encode('utf-8')
    h = hashlib.md5(str_data).hexdigest()
    loc = os.path.join(CACHE_DIR, h)

    if os.path.isfile(loc):
      if verbose:
        print('Using cached data...')
        if pbar is not None:
          pbar.update(1)
      with open(loc, 'r') as f:
        try:
          return json.load(f)
        except:
          pass

  with request_session.post(url, data=json.dumps(data), headers=headers) as r:
    r.raise_for_status()
    resp = r.json()
    frames = resp.get('frames', [])

    if loc is not None:
      with open(loc, 'w') as f:
        json.dump(frames, f)

    if pbar is not None:
      pbar.update(1)

    return frames

def make_week(d):
    year = d.year
    week = d.isocalendar()[1]
    if week == 0 or (week == 52 and d.month < 12):
        year -= 1
        week = 52

    return datetime.strptime(
        "{}-W{}-1".format(year, week), '%Y-W%W-%w'
      ).strftime('%Y-%m-%d')

def valid_date(s):
  try:
    return datetime.strptime(s, "%Y-%m-%d")
  except ValueError:
    msg = "not a valid date: {0!r}".format(s)
    raise argparse.ArgumentTypeError(msg)

def fetch_camera_info(device):
  if len(CAMERA_INFO) == 0:
    url = 'https://hivemapper.com/api/developer/devices'
    with request_session.get(url, stream=True) as r:
      r.raise_for_status()
      resp = r.json()
      CAMERA_INFO['hdc'] = resp['hdc']
      CAMERA_INFO['hdc-s'] = resp['hdc-s']

  return CAMERA_INFO.get(device)

def update_exif(local_path, metadata, verbose=False):
  tags = {}

  if 'camera' in metadata:
    cam = metadata.get('camera', {})
    focal = cam.get('focal', 0.0)
    k1 = cam.get('k1', 0.0)
    k2 = cam.get('k2', 0.0)
    tags['FocalLength'] = focal
    tags['Lens'] = f'{k1} {k2}'

  if verbose:
    print(f'Writing {len(tags)} tags to {local_path}:')

  if len(tags) == 0:
    return

  with ExifToolHelper() as et:
    et.set_tags(
      [local_path],
      tags=tags,
      params=['-overwrite_original']
    )

def renew_asset(asset, authorization):
  headers = {
    "content-type": "application/json",
    "authorization": f'Basic {authorization}',
  }

  encoded_asset = quote(asset, safe='')
  url = f'{RENEW_ASSET_URL}{encoded_asset}'
  with request_session.post(url, headers=headers) as r:
    r.raise_for_status()
    resp = r.json()
    return resp.get('url', asset)

def download_file(
  url,
  local_path,
  metadata,
  authorization,
  encode_exif=False,
  verbose=True,
  overwrite=False,
  pbar=None,
  is_retry=False
):
  if not overwrite and os.path.isfile(local_path):
    if verbose:
      print(f'{local_path} exists, skipping download...')
    return local_path

  os.makedirs(os.path.dirname(local_path), exist_ok=True)
  clean = url.split('?')[0].split('.com/')[1]
  if verbose:
    print("GET {} => {}".format(clean, local_path))

  with request_session.get(url, stream=True) as r:
    try:
      r.raise_for_status()
    except Exception as e:
      if is_retry:
        raise e

      if verbose:
        print(f'Renewing asset {url.split("?")[0]}...')

      new_url = renew_asset(url, authorization)
      return download_file(
        new_url,
        local_path,
        metadata,
        authorization,
        encode_exif,
        verbose,
        overwrite,
        pbar,
        True)

    with open(local_path, 'wb') as f:
      shutil.copyfileobj(r.raw, f)

    if verbose:
      print(f'Downloaded {local_path}')

  if encode_exif:
    update_exif(local_path, metadata, verbose)

  if pbar:
    pbar.update(1)

  return local_path

def download_files(
  frames,
  local_dir,
  authorization,
  preserve_dirs=True,
  merge_metadata=False,
  camera_intrinsics=False,
  encode_exif=False,
  num_threads=DEFAULT_THREADS,
  verbose=False,
  use_cache=True,
  pbar=None,
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

  if camera_intrinsics:
    for frame in frames:
      device = frame.get('device', 'hdc')
      width = float(frame.get('width', 2028))
      camera_info = fetch_camera_info(device)
      frame['camera'] = {
        'focal': camera_info.get('focal', 0.0) * width,
        'k1': camera_info.get('k1', 0.0),
        'k2': camera_info.get('k2', 0.0),
      }

  if merge_metadata:
    local_meta_path = os.path.join(local_dir, 'meta.json')
    os.makedirs(os.path.dirname(local_meta_path), exist_ok=True)
    meta = {
      img_path:
        { key: frame[key] for key in frame if key != 'url' }
      for frame, img_path in zip(frames, img_paths)
    }
    with open(local_meta_path, 'w') as f:
      json.dump(meta, f, indent=4)
  else:
    for frame, meta_path in zip(frames, local_meta_paths):
      os.makedirs(os.path.dirname(meta_path), exist_ok=True)
      with open(meta_path, 'w') as f:
        meta = {key : frame[key] for key in frame if key != 'url'}
        json.dump(meta, f, indent=4)

  executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_threads)
  futures = []

  for url, local_img_path, frame in zip(urls, local_img_paths, frames):
    future = executor.submit(
      download_file,
      url,
      local_img_path,
      frame,
      authorization,
      encode_exif,
      verbose,
      not use_cache,
      pbar,
    )
    futures.append(future)

  for future in concurrent.futures.as_completed(futures):
    try:
      results = future.result()
    except Exception as e:
      print(e)

  return local_img_paths

def query_imagery(
  features,
  weeks,
  custom_ids,
  authorization,
  local_dir,
  num_threads=DEFAULT_RETRIES,
  verbose=False,
  use_cache=True,
):
  headers = {
    "content-type": "application/json",
    "authorization": f'Basic {authorization}',
  }
  frames = []
  pbar = None

  if verbose:
    pbar = tqdm(total=len(features) * len(weeks))

  threads = min(MAX_API_THREADS, num_threads)
  executor = concurrent.futures.ThreadPoolExecutor(max_workers=threads)
  futures = []

  for feature, custom_id in zip(features, custom_ids):
    data = feature.get('geometry', feature)
    assert(area(data) <= MAX_AREA)

    for week in weeks:
      url = f'{IMAGERY_API_URL}?week={week}'
      if verbose:
        print(url)

      future = executor.submit(post_cached, url, data, headers, verbose, use_cache, pbar)
      futures.append(future)

  for future in concurrent.futures.as_completed(futures):
    results = future.result()

    if custom_id is not None:
      for result in results:
        result['id'] = custom_id

    frames += results

  if pbar is not None:
    pbar.close()

  return frames

def query_latest_imagery(
  features,
  custom_ids,
  min_days,
  authorization,
  local_dir,
  num_threads=DEFAULT_THREADS,
  verbose=False,
  use_cache=True
):
  headers = {
    "content-type": "application/json",
    "authorization": f'Basic {authorization}',
  }
  frames = []
  pbar = None

  if verbose:
    pbar = tqdm(total=len(features))

  threads = min(MAX_API_THREADS, num_threads)
  executor = concurrent.futures.ThreadPoolExecutor(max_workers=threads)
  futures = []

  for feature, custom_id, min_day in zip(features, custom_ids, min_days):
    data = feature.get('geometry', feature)
    assert(area(data) <= MAX_AREA)

    url = LATEST_IMAGERY_API_URL
    if min_day:
      url += f'?min_week={min_day}'
    if verbose:
      print(url)

    future = executor.submit(post_cached, url, data, headers, verbose, use_cache, pbar)
    futures.append(future)

  for future in concurrent.futures.as_completed(futures):
    results = future.result()

    if custom_id is not None:
      for result in results:
        result['id'] = custom_id

    frames += results

  if pbar is not None:
    pbar.close()

  return frames

def query_frames(
  features,
  custom_ids,
  start_day,
  end_day,
  output_dir,
  authorization,
  num_threads = DEFAULT_THREADS,
  verbose = False,
  use_cache = True
):
  assert(start_day <= end_day)

  s = start_day
  weeks = [s.strftime("%Y-%m-%d"), end_day.strftime("%Y-%m-%d")]
  while s < end_day - timedelta(days=7):
    s += timedelta(days=7)
    weeks.append(s.strftime("%Y-%m-%d"))
  weeks = [make_week(datetime.strptime(week, "%Y-%m-%d")) for week in weeks]
  weeks = list(set(weeks))

  assert(len(weeks))

  if verbose:
    print(f'Querying {len(features)} features for imagery across {len(weeks)} weeks...')
  frames = query_imagery(
    features, weeks,
    custom_ids,
    authorization,
    output_dir,
    num_threads,
    verbose,
    use_cache
  )
  filtered_frames = [frame for frame in frames if frame_within_day_bounds(frame, start_day, end_day)]

  return filtered_frames

def load_features(geojson_file):
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
  features = [feature for feature in features if feature is not None]
  new_features = []
  for feature in features:
    if type(feature) is list:
      for f in feature:
        new_features.append(f)
    else:
      new_features.append(feature)
  features = new_features

  assert(len(features))

  return features, custom_ids, min_dates

def query_latest_frames(
  features,
  custom_ids,
  min_dates,
  output_dir,
  authorization,
  num_threads = DEFAULT_THREADS,
  verbose = False,
  use_cache = True
):
  if verbose:
    print(f'Querying {len(features)} features for imagery across for latest...')
  frames = query_latest_imagery(
    features,
    custom_ids,
    min_dates,
    authorization,
    output_dir,
    num_threads,
    verbose,
    use_cache
  )

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
  skip_stitching = []
  for seq in by_sequence.values():
    if len(seq) > 1:
      seqs.append(sorted(seq, key=lambda f: f.get('idx')))
    else:
      skip_stitching.append([seq])
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
  skipped = [[ f for seq in coll for f in seq] for coll in skip_stitching]
  if verbose:
    print(f'Stitched {len(stitched)} paths!')
    print(f'Skipped {len(skipped)} paths.')

  return stitched + skipped

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
    features = []
    for i, frames in enumerate(frame_lists):
      if len(frames) > 1:
        features.append(frames_to_linestring(frames, i))
      else:
        frames_to_points(frames)
  geojson = {
    "type": "FeatureCollection",
    "features": features,
  }
  geojson_path = os.path.join(output_dir, 'frames.geojson')
  with open(geojson_path, 'w') as f:
    json.dump(geojson, f)

  if verbose:
    print(f'Wrote geojson to {geojson_path}')

def _query(
  features,
  custom_ids,
  min_dates,
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
  camera_intrinsics=False,
  update_exif=False,
  custom_id_field=None,
  custom_min_date_field=None,
  skip_geo_file=None,
  num_threads=DEFAULT_THREADS,
  verbose=False,
  use_cache=True,
):
  if latest:
    frames = query_latest_frames(features, custom_ids, min_dates, output_dir, authorization, num_threads, verbose, use_cache)
  else:
    frames = query_frames(features, custom_ids, start_day, end_day, output_dir, authorization, num_threads, verbose, use_cache)
  print(f'Found {len(frames)} images!')

  img_paths = []
  pbar = None

  if frames:
    if verbose:
      print(f'Downloading with {num_threads} threads...')
      pbar = tqdm(total=len(frames))

    if should_stitch:
      stitched = stitch(frames, max_dist, max_lag, max_angle, verbose)
      for i, frame_set in enumerate(stitched):
        folder = f'{str(uuid.uuid4())}-{str(i)}'
        local_dir = os.path.join(output_dir, folder)
        img_paths += download_files(
          frame_set,
          local_dir,
          authorization,
          False,
          merge_metadata,
          camera_intrinsics,
          update_exif,
          num_threads,
          verbose,
          use_cache,
          pbar,
        )
      if export_geojson:
        write_geojson(stitched, output_dir, False, verbose)
    else:
      img_paths += download_files(
        frames,
        output_dir,
        authorization,
        True,
        merge_metadata,
        camera_intrinsics,
        update_exif,
        num_threads,
        verbose,
        use_cache,
        pbar,
      )
      if export_geojson:
        write_geojson([frames], output_dir, True, verbose)
    
    print(f'{len(frames)} frames saved to {output_dir}!')

  if pbar is not None:
    pbar.close()

  return img_paths

def transform_input(
  file_path,
  width=DEFAULT_WIDTH,
  custom_id_field=None,
  custom_min_date_field=None,
  skip_geo_file=None,
  verbose=False,
  use_cache=True,
):
  geojson_file = None
  loc = None
  if use_cache:
    loc = f'transformed_{file_path}'
    loc = loc.replace('/', '_')
    loc = loc.replace('\\', '_')
    loc = os.path.join(CACHE_DIR, loc)
    if os.path.isfile(loc):
      with open(loc, 'r') as f:
        geojson_file = f.read()

      if verbose:
        print(f'Using cached geometry: {geojson_file}')

  if geojson_file is None and file_path.endswith('.shp'):
    geojson_file = f'{file_path[0 : len(file_path) - 4]}.geojson_{str(uuid.uuid4())}'
    geo.transform_shapefile_to_geojson_polygons(file_path, geojson_file, width, verbose)
  elif geojson_file is None and file_path.endswith('.csv'):
    geojson_file = f'{file_path[0 : len(file_path) - 4]}.geojson_{str(uuid.uuid4())}'
    geo.transform_csv_to_geojson_polygons(
      file_path,
      geojson_file,
      width,
      custom_id_field,
      custom_min_date_field,
      verbose,
    )
  elif geojson_file is None:
    geojson_file = file_path

  if use_cache:
    with open(loc, 'w') as f:
      f.write(geojson_file)

  if skip_geo_file:
    skips = skip_geo_file.split(',')
    geojson_file2 = None

    if use_cache:
      loc += '_'.join(skips).replace('/','_').replace('\\', '_')
      print(loc)
      if os.path.isfile(loc):
        with open(loc, 'r') as f:
          geojson_file = f.read()
        if verbose:
          print(f'Using cached subtracted geometry: {geojson_file}')
          return geojson_file

    for skip_f in skips:
      geojson_file2 = geojson_file.replace('.json', '_delta.json')
      geo.subtract_geojson(geojson_file, skip_f, geojson_file2, width, verbose)
      geojson_file = geojson_file2

  if use_cache:
    with open(loc, 'w') as f:
      f.write(geojson_file)

  return geojson_file

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
  camera_intrinsics=False,
  update_exif=False,
  custom_id_field=None,
  custom_min_date_field=None,
  skip_geo_file=None,
  num_threads=DEFAULT_THREADS,
  verbose=False,
  use_cache=True,
  use_batches=False,
):
  geojson_file = transform_input(
    file_path,
    width,
    custom_id_field,
    custom_min_date_field,
    skip_geo_file,
    verbose,
    use_cache,
  )

  features, custom_ids, min_dates = load_features(geojson_file)

  if not use_batches:
    _query(
      features,
      custom_ids,
      min_dates,
      start_day,
      end_day,
      output_dir,
      authorization,
      latest,
      export_geojson,
      should_stitch,
      max_dist,
      max_lag,
      max_angle,
      width,
      merge_metadata,
      camera_intrinsics,
      update_exif,
      custom_id_field,
      custom_min_date_field,
      skip_geo_file,
      num_threads,
      verbose,
      use_cache,
    )
    return

  for i in range(0, len(features), BATCH_SIZE):
    if verbose:
      print(f'processing {i} to {i + BATCH_SIZE} features...')

    loc = None
    if use_cache:
      with open(geojson_file, 'rb') as f:
        h = hashlib.md5(f.read()).hexdigest()
        loc = os.path.join(CACHE_DIR, f'batch_{start_day}_{end_day}_{latest}_{should_stitch}_{h}_{i}')

      if os.path.isfile(loc):
        if verbose:
          print('Cache hit -- skipping batch.')
        continue
   
    _query(
      features[i:i + BATCH_SIZE],
      custom_ids[i:i + BATCH_SIZE],
      min_dates[i:i + BATCH_SIZE],
      start_day,
      end_day,
      output_dir,
      authorization,
      latest,
      export_geojson,
      should_stitch,
      max_dist,
      max_lag,
      max_angle,
      width,
      merge_metadata,
      camera_intrinsics,
      update_exif,
      custom_id_field,
      custom_min_date_field,
      skip_geo_file,
      num_threads,
      verbose,
      use_cache,
    )

    if loc is not None:
      with open(loc, 'w') as f:
        f.write(datetime.now().isoformat())

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
  parser.add_argument('-k', '--camera_intrinsics', action='store_true')
  parser.add_argument('-E', '--update_exif', action='store_true')
  parser.add_argument('-K', '--skip_geo_file', type=str)
  parser.add_argument('-P', '--image_post_processing', type=str)
  parser.add_argument('-a', '--authorization', type=str, required=True)
  parser.add_argument('-c', '--num_threads', type=int, default=DEFAULT_THREADS)
  parser.add_argument('-v', '--verbose', action='store_true')
  parser.add_argument('-C', '--cache', action='store_true')
  parser.add_argument('-b', '--use_batches', action='store_true')
  args = parser.parse_args()

  if args.cache:
    setup_cache(args.verbose)

  if args.image_post_processing:
    assert(args.image_post_processing in VALID_POST_PROCESSING_OPTS)
    assert(not args.cache)

  img_paths = query(
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
    args.camera_intrinsics,
    args.update_exif,
    args.custom_id_field,
    args.custom_min_date_field,
    args.skip_geo_file,
    args.num_threads,
    args.verbose,
    args.cache,
    args.use_batches,
  )

  if args.image_post_processing:
    if args.verbose:
      print(f'post processing {len(img_paths)} with {args.image_post_processing}...')

    def post_process(img_path, image_post_processing, verbose):
      if image_post_processing == 'clahe-smart-clip':
        clahe_smart_clip(img_path, img_path, verbose)

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.num_threads) as executor:
      executor.map(post_process, img_paths, repeat(args.image_post_processing), repeat(args.verbose))

  # if args.cache:
  #   clear_cache(args.verbose)
