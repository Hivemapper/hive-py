import argparse
import concurrent.futures
import hashlib
import json
import os
import requests
import shutil
import uuid

from area import area
from datetime import datetime, timedelta
from exiftool import ExifToolHelper
from itertools import repeat, zip_longest
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm
from urllib.parse import quote, urlparse, urlencode
from util import geo, replace_dirs_with_zips, stitching, write_csv_from_csv
from imagery.processing import clahe_smart_clip, undistort_via_merged_json
import copy

BATCH_SIZE = 10000
CACHE_DIR = '.hivepy_cache'
DEFAULT_BACKOFF = 1.0
DEFAULT_RETRIES = 10
DEFAULT_STITCH_MAX_DISTANCE = 30
DEFAULT_STITCH_MAX_LAG = 360
DEFAULT_STITCH_MAX_ANGLE = 100
DEFAULT_THREADS = 20
DEFAULT_WIDTH = 25
IMAGERY_API_URL = 'https://hivemapper.com/api/developer/imagery/poly'
LATEST_IMAGERY_API_URL = 'https://hivemapper.com/api/developer/latest/poly'
PROBE_API_URL = 'https://hivemapper.com/api/developer/probe'
RENEW_ASSET_URL = 'https://hivemapper.com/api/developer/renew/'
MAP_MATCH_API_URL = 'https://hivemapper.com/api/developer/imagery/mapmatch'
MAX_API_THREADS = 16
MAX_MAP_MATCH_AREA = 500 * 500
MAX_AREA = 1000 * 1000 * 4 # 4km^2
MAX_PROBE_AREA = 1000 * 1000 # 1km^2
STATUS_FORCELIST = [429, 502, 503, 504, 524]
VALID_POST_PROCESSING_OPTS = ['clahe-smart-clip', 'undistort']

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

def post_cached(
  url,
  data,
  headers,
  verbose=True,
  use_cache=True,
  skip_cached_frames=False,
  pbar=None,
  custom_id=None,
):
  loc = None
  if use_cache:
    str_data = json.dumps({ 'url': url, 'data': data }).encode('utf-8')
    h = hashlib.md5(str_data).hexdigest()
    loc = os.path.join(CACHE_DIR, h)

    if os.path.isfile(loc):
      if verbose:
        if pbar is not None:
          pbar.update(1)

      if skip_cached_frames:
        return []

      with open(loc, 'r') as f:
        try:
          return json.load(f)
        except:
          pass

  with request_session.post(url, data=json.dumps(data), headers=headers) as r:
    try:
      try:
        if "error" in r.json():
          http_json_error_msg = r.json()["error"]
          print (http_json_error_msg)
      except json.JSONDecodeError:
        pass
      r.raise_for_status()
    except requests.exceptions.HTTPError as e:
      if e.response.status_code == 500:
        if verbose:
          print('Encountered a server error, skipping:')
          print(e)
        if pbar:
          pbar.update(1)
        return []
      else:
        raise e
    except requests.exceptions.RetryError as e:
      if verbose:
        print('Encountered a server error, skipping:')
        print(e)
      if pbar:
        pbar.update(1)
      return []

    resp = r.json()
    frames = resp.get('frames', [])

    if custom_id is not None:
      for frame in frames:
        frame['id'] = custom_id

    if loc is not None:
      with open(loc, 'w') as f:
        json.dump(frames, f)

    if pbar is not None:
      pbar.update(1)

    return frames

def make_week(d):
    year = d.year
    week = d.isocalendar()[1] - 1
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
      CAMERA_INFO['bee'] = resp['bee']

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
  is_retry=False,
):
  if not overwrite and os.path.isfile(local_path):
    if verbose:
      print(f'{local_path} exists, skipping download...')
    if pbar:
      pbar.update(1)
    return local_path

  k = url.split('.com/')[1].split('?')[0]
  loc = None
  if not overwrite:
    loc = os.path.join(CACHE_DIR, k.replace('/', '_'))
    if os.path.isfile(loc):
      if pbar:
        pbar.update(1)
      return

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

    if not overwrite:
      with open(loc, 'w') as f:
        f.write(local_path)

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
    return []

  if camera_intrinsics:
    for frame in frames:
      device = frame.get('device', 'hdc')
      width = float(frame.get('width', 2028))
      camera_info = fetch_camera_info(device)
      frame['camera'] = copy.deepcopy(camera_info)
      frame['camera']['focal'] = camera_info.get('focal', 0.0) * width,

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
  mount=None,
  azi_filter=None,
  num_threads=DEFAULT_RETRIES,
  verbose=False,
  use_cache=True,
  skip_cached_frames=False,
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

  for feature, custom_id in zip_longest(features, custom_ids):
    data = feature.get('geometry', feature)
    assert(area(data) <= MAX_AREA)

    for week in weeks:
      url = f'{IMAGERY_API_URL}?week={week}'
      if mount:
        url += f'&mount={mount}'
      if azi_filter:
        url += f'&azimuth={azi_filter[0]}&tolerance={azi_filter[1]}'

      future = executor.submit(
        post_cached,
        url,
        data,
        headers,
        verbose,
        use_cache,
        skip_cached_frames,
        pbar,
        custom_id,
      )
      futures.append(future)

  for future in concurrent.futures.as_completed(futures):
    results = future.result()

    frames += results

  if pbar is not None:
    pbar.close()

  return frames

def query_latest_imagery(
  features,
  custom_ids,
  min_days,
  crossjoin,
  azi_filter,
  global_min_date,
  authorization,
  local_dir,
  mount=None,
  map_match=False,
  num_threads=DEFAULT_THREADS,
  verbose=False,
  use_cache=True,
  skip_cached_frames=False,
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

  for feature, custom_id, min_day in zip_longest(features, custom_ids, min_days):
    data = feature.get('geometry', feature)
    assert(area(data) <= MAX_AREA)

    url = LATEST_IMAGERY_API_URL if not map_match else MAP_MATCH_API_URL
    params_added = False
    if min_day:
      url += f'?min_week={min_day}'
      params_added = True
    elif global_min_date:
      url += f'?min_week={global_min_date.strftime("%Y-%m-%d")}'
      params_added = True      

    if mount:
      pchar = '&' if params_added else '?'
      url += f'{pchar}mount={mount}'
      params_added = True
    if crossjoin:
      pchar = '&' if params_added else '?'
      url += f'{pchar}crossjoin=true'
      params_added = True
    if azi_filter:
      pchar = '&' if params_added else '?'
      url += f'{pchar}azimuth={azi_filter[0]}&tolerance={azi_filter[1]}'
      params_added = True

    future = executor.submit(
      post_cached,
      url,
      data,
      headers,
      verbose,
      use_cache,
      skip_cached_frames,
      pbar,
      custom_id,
    )
    futures.append(future)

  for future in concurrent.futures.as_completed(futures):
    results = future.result()
    frames += results

  if pbar is not None:
    pbar.close()

  return frames

def query_imagery_with_segment_ids(
  segment_ids,
  weeks,
  custom_ids,
  authorization,
  local_dir,
  mount=None,
  azi_filter=None,
  num_threads=DEFAULT_RETRIES,
  verbose=False,
  use_cache=True,
  skip_cached_frames=False,
):
  headers = {
    "content-type": "application/json",
    "authorization": f'Basic {authorization}',
  }
  frames = []
  pbar = None

  if verbose:
    pbar = tqdm(total=len(segment_ids) * len(weeks))

  threads = min(MAX_API_THREADS, num_threads)
  executor = concurrent.futures.ThreadPoolExecutor(max_workers=threads)
  futures = []
  assert(len(segment_ids) <= MAX_API_THREADS)

  for segment_id, custom_id in zip_longest(segment_ids, custom_ids):
    data = {'segmentId': segment_id}
    for week in weeks:
      url = f'{IMAGERY_API_URL}?week={week}'
      if mount:
        url += f'&mount={mount}'
      if azi_filter:
        url += f'&azimuth={azi_filter[0]}&tolerance={azi_filter[1]}'
      future = executor.submit(
        post_cached,
        url,
        data,
        headers,
        verbose,
        use_cache,
        skip_cached_frames,
        pbar,
        custom_id,
      )
      futures.append(future)

  for future in concurrent.futures.as_completed(futures):
    results = future.result()

    frames += results

  if pbar is not None:
    pbar.close()

  return frames
    
def query_latest_imagery_with_segment_ids(
  segment_ids,
  custom_ids,
  min_days,
  authorization,
  local_dir,
  mount=None,
  num_threads=DEFAULT_THREADS,
  verbose=False,
  use_cache=True,
  skip_cached_frames=False,
):
  headers = {
    "content-type": "application/json",
    "authorization": f'Basic {authorization}',
  }
  frames = []
  pbar = None

  if verbose:
    pbar = tqdm(total=len(segment_ids))

  threads = min(MAX_API_THREADS, num_threads)
  executor = concurrent.futures.ThreadPoolExecutor(max_workers=threads)
  futures = []
  assert(len(segment_ids) <= MAX_API_THREADS)

  for segment_id, custom_id, min_day in zip_longest(segment_ids, custom_ids, min_days):
    data = segment_id
    url = LATEST_IMAGERY_API_URL
    params = {}

    if min_day:
        params['min_week'] = min_day
    if mount:
        params['mount'] = mount

    if params:
        url += f'?{urlencode(params)}'

    future = executor.submit(
      post_cached,
      url,
      data,
      headers,
      verbose,
      use_cache,
      skip_cached_frames,
      pbar,
      custom_id,
    )
    futures.append(future)

  for future in concurrent.futures.as_completed(futures):
    results = future.result()
    frames += results

  if pbar is not None:
    pbar.close()

  return frames

def query_frames_with_segment_ids(  
  segment_ids,
  custom_ids,
  start_day,
  end_day,
  output_dir,
  authorization,
  mount = None,
  azi_filter = None,
  num_threads = DEFAULT_THREADS,
  verbose = False,
  use_cache = True,
  skip_cached_frames = False,
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
    print(f'Querying {len(segment_ids)} segments for imagery across {len(weeks)} weeks...')
  frames = query_imagery_with_segment_ids(
    segment_ids,
    weeks,
    custom_ids,
    authorization,
    output_dir,
    mount,
    azi_filter,
    num_threads,
    verbose,
    use_cache,
    skip_cached_frames
  )
  filtered_frames = [frame for frame in frames if frame_within_day_bounds(frame, start_day, end_day)]

  return filtered_frames
    
def query_frames(
  features,
  custom_ids,
  start_day,
  end_day,
  output_dir,
  authorization,
  mount = None,
  azi_filter = None,
  num_threads = DEFAULT_THREADS,
  verbose = False,
  use_cache = True,
  skip_cached_frames = False,
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
    features,
    weeks,
    custom_ids,
    authorization,
    output_dir,
    mount,
    azi_filter,
    num_threads,
    verbose,
    use_cache,
    skip_cached_frames
  )
  filtered_frames = [frame for frame in frames if frame_within_day_bounds(frame, start_day, end_day)]

  return filtered_frames

def load_features(geojson_file, verbose = False, map_match = False):
  features = []
  with open(geojson_file, 'r') as f:
    fc = json.load(f)
    features += fc.get('features', [fc])

  for i in range(len(features)):
    if features[i].get('geometry', features[i]).get('type') == 'MultiPolygon':
      features[i] = geo.explode_multipolygon(features[i])
  features = geo.flat_list(features)

  custom_ids = []
  min_dates = []
  for feature in features:
    properties = feature.get('properties', {})
    custom_ids.append(properties.get('id', None))
    min_dates.append(properties.get('min_date', None))

  max_area = MAX_AREA if not map_match else MAX_MAP_MATCH_AREA
  features = [geo.convert_to_geojson_poly(f, DEFAULT_WIDTH, verbose, max_area) for f in features]
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
  crossjoin,
  azi_filter,
  global_min_date,
  output_dir,
  authorization,
  mount = None,
  map_match = False,
  num_threads = DEFAULT_THREADS,
  verbose = False,
  use_cache = True,
  skip_cached_frames = False
):
  if verbose:
    print(f'Querying {len(features)} features for imagery across for latest...')
  frames = query_latest_imagery(
    features,
    custom_ids,
    min_dates,
    crossjoin,
    azi_filter,
    global_min_date,
    authorization,
    output_dir,
    mount,
    map_match,
    num_threads,
    verbose,
    use_cache,
    skip_cached_frames
  )

  return frames

def handle_download_and_export_from_raw_frames(  
  frames_raw,  
  output_dir,
  authorization,
  export_geojson,
  should_stitch,
  max_dist,
  max_lag,
  max_angle,
  merge_metadata,
  camera_intrinsics,
  update_exif,
  tracked_by_id,
  num_threads,
  verbose,
  use_cache,
  ):
  frames = []
  seen = set()
  for frame in frames_raw:
    url = frame.get('url')
    path = urlparse(url).path  # Extract the path part of the URL
    k = path.lstrip('/')  # Remove leading slash if necessary
    if k in seen:
        continue
    seen.add(k)
    frames.append(frame)

  print(f'Found {len(frames)} images!')

  img_paths = []
  pbar = None

  if frames:
    if verbose:
      print(f'Downloading with {num_threads} threads...')
      pbar = tqdm(total=len(frames))

    if tracked_by_id is not None:
      by_id = {}
      for frame in frames:
        custom_id = frame['id']
        by_id.setdefault(custom_id, [])
        by_id[custom_id].append(frame)
        tracked_by_id.setdefault(custom_id, frame.get('timestamp'))
        tracked_by_id[custom_id] = max(tracked_by_id[custom_id], frame.get('timestamp'))
      for custom_id, frame_set in by_id.items():
        local_dir = os.path.join(output_dir, custom_id)
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
        write_geojson([frames], output_dir, True, verbose)
    elif should_stitch:
      stitched = stitching.stitch(frames, max_dist, max_lag, max_angle, verbose)
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

def _query_segment_imagery(      
  segment_ids, 
  start_day,
  end_day,
  output_dir,
  authorization,
  mount,
  azi_filter,
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
  tracked_by_id,
  skip_geo_file,
  num_threads,
  verbose,
  use_cache,
  skip_cached_frames,
  custom_ids = [],
  min_dates = [], 
):
  if latest:
    frames_raw = query_latest_imagery_with_segment_ids(
      segment_ids,
      custom_ids,
      min_dates,
      output_dir,
      authorization,
      mount,
      azi_filter,
      num_threads,
      verbose,
      use_cache,
      skip_cached_frames,
    )
  else:
    frames_raw = query_frames_with_segment_ids(
      segment_ids,
      custom_ids,
      start_day,
      end_day,
      output_dir,
      authorization,
      mount,
      azi_filter,
      num_threads,
      verbose,
      use_cache,
      skip_cached_frames,
    )
  return handle_download_and_export_from_raw_frames(
    frames_raw,
    output_dir,
    authorization,
    export_geojson,
    should_stitch,
    max_dist,
    max_lag,
    max_angle,
    merge_metadata,
    camera_intrinsics,
    update_exif,
    tracked_by_id,
    num_threads,
    verbose,
    use_cache,
  )


def frame_within_day_bounds(frame, start_day, end_day):
  d = datetime.fromisoformat(frame.get('timestamp').split('.')[0])
  return d >= start_day and d <= end_day + timedelta(days=1)

def frames_to_linestring(frames, ident):
  return {
    "type": "Feature",
    "properties": {
      "id": ident,
      "sequence": frames[0].get('sequence'),
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

  if os.path.isfile(geojson_path):
    try:
      with open(geojson_path, 'r') as f:
        fc = json.load(f)
        features = fc.get('features', [])
        geojson['features'] += features
    except Exception as e:
      print(e)

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
  mount=None,
  latest=False,
  crossjoin=False,
  azi_filter=None,
  global_min_date=None,
  export_geojson=False,
  should_stitch=False,
  max_dist=DEFAULT_STITCH_MAX_DISTANCE,
  max_lag=DEFAULT_STITCH_MAX_LAG,
  max_angle=DEFAULT_STITCH_MAX_ANGLE,
  width=DEFAULT_WIDTH,
  merge_metadata=False,
  camera_intrinsics=False,
  update_exif=False,
  map_match=False,
  custom_id_field=None,
  custom_min_date_field=None,
  tracked_by_id=None,
  skip_geo_file=None,
  num_threads=DEFAULT_THREADS,
  verbose=False,
  use_cache=True,
  skip_cached_frames=False,
):
  if latest:
    frames_raw = query_latest_frames(
      features,
      custom_ids,
      min_dates,
      crossjoin,
      azi_filter,
      global_min_date,
      output_dir,
      authorization,
      mount,
      map_match,
      num_threads,
      verbose,
      use_cache,
      skip_cached_frames,
    )
  else:
    frames_raw = query_frames(
      features,
      custom_ids,
      start_day,
      end_day,
      output_dir,
      authorization,
      mount,
      azi_filter,
      num_threads,
      verbose,
      use_cache,
      skip_cached_frames,
    )

  return handle_download_and_export_from_raw_frames(
    frames_raw,
    output_dir,
    authorization,
    export_geojson,
    should_stitch,
    max_dist,
    max_lag,
    max_angle,
    merge_metadata,
    camera_intrinsics,
    update_exif,
    tracked_by_id,
    num_threads,
    verbose,
    use_cache,
  )

def transform_input(
  file_path,
  width=DEFAULT_WIDTH,
  custom_id_field=None,
  custom_min_date_field=None,
  custom_date_formatting=None,
  skip_geo_file=None,
  verbose=False,
  use_cache=True,
):
  geojson_file = None
  loc = None
  if use_cache:
    loc = f'transformed_3_{file_path}'
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
      custom_date_formatting,
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
      loc += '4' + '_'.join(['s' for s in skips]).replace('/','_').replace('\\', '_')
      if os.path.isfile(loc):
        with open(loc, 'r') as f:
          geojson_file = f.read()
        if verbose:
          print(f'Using cached subtracted geometry: {geojson_file}')
        return geojson_file

    for skip_f in skips:
      geojson_file2 = geojson_file.replace('json', 'd_').replace('d_', 'd_s')
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
  mount=None,
  latest=False,
  crossjoin=False,
  azi_filter=None,
  global_min_date=None,
  export_geojson=False,
  should_stitch=False,
  max_dist=DEFAULT_STITCH_MAX_DISTANCE,
  max_lag=DEFAULT_STITCH_MAX_LAG,
  max_angle=DEFAULT_STITCH_MAX_ANGLE,
  width=DEFAULT_WIDTH,
  merge_metadata=False,
  camera_intrinsics=False,
  update_exif=False,
  map_match=False,
  custom_id_field=None,
  custom_min_date_field=None,
  custom_date_formatting=None,
  tracked_by_id=None,
  skip_geo_file=None,
  num_threads=DEFAULT_THREADS,
  verbose=False,
  use_cache=True,
  skip_cached_frames=False,
  use_batches=False,
  segment_ids=None,
):
  if(segment_ids):
    #handle segment id endpoint, no transformation needed
    image_path = _query_segment_imagery(
      segment_ids,
      start_day,
      end_day,
      output_dir,
      authorization,
      mount,
      azi_filter,
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
      tracked_by_id,
      skip_geo_file,
      num_threads,
      verbose,
      use_cache,
      skip_cached_frames,
      )
    return image_path

  geojson_file = transform_input(
    file_path,
    width,
    custom_id_field,
    custom_min_date_field,
    custom_date_formatting,
    skip_geo_file,
    verbose,
    use_cache,
  )

  features, custom_ids, min_dates = load_features(geojson_file, verbose)

  if not use_batches:
    img_paths = _query(
      features,
      custom_ids,
      min_dates,
      start_day,
      end_day,
      output_dir,
      authorization,
      mount,
      latest,
      crossjoin,
      azi_filter,
      global_min_date,
      export_geojson,
      should_stitch,
      max_dist,
      max_lag,
      max_angle,
      width,
      merge_metadata,
      camera_intrinsics,
      update_exif,
      map_match,
      custom_id_field,
      custom_min_date_field,
      tracked_by_id,
      skip_geo_file,
      num_threads,
      verbose,
      use_cache,
      skip_cached_frames,
    )
    return img_paths

  img_paths = []

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
   
    img_paths += _query(
      features[i:i + BATCH_SIZE],
      custom_ids[i:i + BATCH_SIZE],
      min_dates[i:i + BATCH_SIZE],
      start_day,
      end_day,
      output_dir,
      authorization,
      mount,
      latest,
      crossjoin,
      azi_filter,
      global_min_date,
      export_geojson,
      should_stitch,
      max_dist,
      max_lag,
      max_angle,
      width,
      merge_metadata,
      camera_intrinsics,
      update_exif,
      map_match,
      custom_id_field,
      custom_min_date_field,
      tracked_by_id,
      skip_geo_file,
      num_threads,
      verbose,
      use_cache,
    )

    if loc is not None:
      with open(loc, 'w') as f:
        f.write(datetime.now().isoformat())

  return img_paths

def probe(
  input_file,
  output_dir,
  authorization,
  mount,
  start_day=None,
  width=DEFAULT_WIDTH,
  verbose=False,
):
  features, _, _ = load_features(input_file, verbose)

  if len(features) > 1:
    raise ValueError(f'Can only support a single GeoJSON feature')

  data = features[0].get('geometry', features[0])
  assert(area(data) <= MAX_PROBE_AREA)

  headers = {
    "content-type": "application/json",
    "authorization": f'Basic {authorization}',
  }

  url = PROBE_API_URL
  if start_day:
    url += f'?min_week={start_day.strftime("%Y-%m-%d")}'
  if mount:
    url += f'&mount={mount}'

  probe_data = None

  with request_session.post(url, data=json.dumps(data), headers=headers) as r:
    r.raise_for_status()
    probe_data = r.json()

  if verbose:
    print(probe_data)

  os.makedirs(output_dir, exist_ok=True)
  now = datetime.now().isoformat()
  local_path = os.path.join(output_dir, f'probe_{now}.json')
  with open(local_path, 'w') as f:
    json.dump(probe_data, f)

  print(f'Saved probe data to {local_path}')

def validate_max_args(value_list, max_args, arg_name = ''):
    if len(value_list) > max_args:
        raise argparse.ArgumentTypeError(f"Maximum {max_args} {arg_name} arguments allowed, but {len(value_list)} provided.")
    return value_list

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  # require at least one of input_file or segment_id
  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument('-i', '--input_file', type=str, help='Input file')
  group.add_argument('-sg', '--segment_ids', nargs='+', help='Segment IDs')
  parser.add_argument('-s', '--start_day', type=valid_date)
  parser.add_argument('-e', '--end_day', type=valid_date)
  parser.add_argument('-W', '--week', type=valid_date)
  parser.add_argument('-L', '--latest', action='store_true')
  parser.add_argument('-j', '--crossjoin', action='store_true')
  parser.add_argument('-A', '--azimuth_filter_angle', type=int)
  parser.add_argument('-T', '--azimuth_filter_tolerance', type=int)
  parser.add_argument('-G', '--global_min_date', type=valid_date)
  parser.add_argument('-x', '--stitch', action='store_true')
  parser.add_argument('-d', '--max_dist', type=float, default=DEFAULT_STITCH_MAX_DISTANCE)
  parser.add_argument('-l', '--max_lag', type=float, default=DEFAULT_STITCH_MAX_ANGLE)
  parser.add_argument('-z', '--max_angle', type=float, default=DEFAULT_STITCH_MAX_LAG)
  parser.add_argument('-o', '--output_dir', type=str, required=True)
  parser.add_argument('-Z', '--zip_dirs', action='store_true')
  parser.add_argument('-Zio', '--zip_images_only', action='store_true')
  parser.add_argument('-g', '--export_geojson', action='store_true')
  parser.add_argument('-w', '--width', type=int, default=DEFAULT_WIDTH)
  parser.add_argument('-m', '--mount', type=str)
  parser.add_argument('-M', '--merge_metadata', action='store_true', default=True)
  parser.add_argument('-I', '--custom_id_field', type=str)
  parser.add_argument('-S', '--custom_min_date_field', type=str)
  parser.add_argument('-SF', '--custom_date_formatting', type=str)
  parser.add_argument('-Io', '--custom_output_dir_field', type=str)
  parser.add_argument('-Ib', '--custom_output_success_field', type=str)
  parser.add_argument('-Is', '--custom_output_date_field', type=str)
  parser.add_argument('-tI', '--track_by_custom_id', action='store_true')
  parser.add_argument('-p', '--passthrough_csv_output', action='store_true')
  parser.add_argument('-k', '--camera_intrinsics', action='store_true')
  parser.add_argument('-E', '--update_exif', action='store_true')
  parser.add_argument('-K', '--skip_geo_file', type=str)
  parser.add_argument('-P', '--image_post_processing', type=str)
  parser.add_argument('-a', '--authorization', type=str, required=True)
  parser.add_argument('-c', '--num_threads', type=int, default=DEFAULT_THREADS)
  parser.add_argument('-v', '--verbose', action='store_true')
  parser.add_argument('-C', '--cache', action='store_true')
  parser.add_argument('-b', '--use_batches', action='store_true')
  parser.add_argument('-N', '--skip_cached_frames', action='store_true')
  parser.add_argument('-q', '--probe', action='store_true')
  parser.add_argument('-U', '--map_match', action='store_true')
  args = parser.parse_args()

  # require either
  if (args.input_file == None) and (args.segment_ids== None):
    # throw error that requires either one
    print('Please provide either an input geojson file or segment ids')
    exit()

  # specify start_date or week
  if (args.week is not None):
    args.start_day = datetime.strptime(make_week(args.week), '%Y-%m-%d')
    args.end_day = args.start_day + timedelta(days = 6)
  if args.segment_ids:
    args.segment_ids = validate_max_args(args.segment_ids, MAX_API_THREADS, 'segment_ids')

  if args.input_file and args.probe:
    probe(
      args.input_file,
      args.output_dir,
      args.authorization,
      args.mount,
      args.start_day,
      args.width,
      args.verbose,
    )
    exit()

  if args.cache:
    setup_cache(args.verbose)

  if args.image_post_processing:
    assert(args.image_post_processing in VALID_POST_PROCESSING_OPTS)
    assert(not args.skip_cached_frames)

  tracked_by_id = ({}
                  if args.track_by_custom_id and args.input_file.endswith('.csv')
                  else None)

  azi_filter = None
  if args.azimuth_filter_angle is not None and args.azimuth_filter_tolerance is not None:
    azi_filter = (args.azimuth_filter_angle, args.azimuth_filter_tolerance)

  img_paths = query(
    args.input_file,
    args.start_day,
    args.end_day,
    args.output_dir,
    args.authorization,
    args.mount,
    args.latest,
    args.crossjoin,
    azi_filter,
    args.global_min_date,
    args.export_geojson,
    args.stitch,
    args.max_dist,
    args.max_lag,
    args.max_angle,
    args.width,
    args.merge_metadata,
    args.camera_intrinsics,
    args.update_exif,
    args.map_match,
    args.custom_id_field,
    args.custom_min_date_field,
    args.custom_date_formatting,
    tracked_by_id,
    args.skip_geo_file,
    args.num_threads,
    args.verbose,
    args.cache,
    args.skip_cached_frames,
    args.use_batches,
    args.segment_ids
  )

  if args.image_post_processing:
    if args.verbose:
      print(f'post processing {len(img_paths)} with {args.image_post_processing}...')

    def post_process(img_path, image_post_processing, verbose):
      cache_dir = CACHE_DIR if args.cache else None
      if args.image_post_processing == 'clahe-smart-clip':
        clahe_smart_clip(img_path, img_path, verbose, cache_dir)
      elif args.image_post_processing == 'undistort':
        assert(args.camera_intrinsics)
        assert(args.update_exif)
        undistort_via_merged_json(img_path, img_path, verbose, cache_dir)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=args.num_threads)
    futures = []

    for img_path in img_paths:
      future = executor.submit(
        post_process,
        img_path,
        img_path,
        args.verbose,
      )
      futures.append(future)

    for future in concurrent.futures.as_completed(futures):
      try:
        results = future.result()
      except Exception as e:
        print(e)

  if tracked_by_id is not None and args.passthrough_csv_output:
    output_path = os.path.join(args.output_dir, 'results.csv')
    if args.verbose:
      print(f'writing {output_path}')
    write_csv_from_csv(
      args.input_file,
      output_path,
      args.custom_id_field,
      tracked_by_id,
      args.custom_output_dir_field,
      args.custom_date_formatting,
      args.custom_output_date_field,
      args.custom_output_success_field,
    )

  if args.zip_dirs:
    replace_dirs_with_zips(args.output_dir, args.zip_images_only, args.verbose)

  # if args.cache:
  #   clear_cache(args.verbose)
