import argparse
import concurrent.futures
import json
import os
import requests
import shutil
import uuid

from area import area
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm
from util import geo, replace_dirs_with_zips

DEFAULT_BACKOFF = 1.0
DEFAULT_RETRIES = 10
DEFAULT_THREADS = 20
DEFAULT_WIDTH = 25
MAP_FEATURE_API_URL = 'https://hivemapper.com/api/developer/mapFeatures/poly'
MAX_API_THREADS = 16
MAX_AREA = 1000 * 1000 * 4 # 4km^2
STATUS_FORCELIST = [429, 502, 503, 504, 524]

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

def post_cached(
  url,
  data,
  headers,
  verbose=True,
  pbar=None,
):
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
    mapfeatures = resp.get('features', [])

    if pbar is not None:
      pbar.update(1)

    return mapfeatures

def query_mapfeatures(
  features,
  authorization,
  num_threads=4,
  verbose=False,
):
  headers = {
    "content-type": "application/json",
    "authorization": f'Basic {authorization}',
  }
  mapfeatures = []
  pbar = None

  if verbose:
    pbar = tqdm(total=len(features))

  threads = min(MAX_API_THREADS, num_threads)
  executor = concurrent.futures.ThreadPoolExecutor(max_workers=threads)
  futures = []

  for feature in features:
    data = feature.get('geometry', feature)
    assert(area(data) <= MAX_AREA)

    url = MAP_FEATURE_API_URL
    future = executor.submit(
      post_cached,
      url,
      data,
      headers,
      verbose,
      pbar,
    )
    futures.append(future)

  for future in concurrent.futures.as_completed(futures):
    results = future.result()

    mapfeatures += results

  if pbar is not None:
    pbar.close()

  return mapfeatures

def transform_input(
  file_path,
  width=DEFAULT_WIDTH,
  verbose=False,
):
  geojson_file = None

  if geojson_file is None and file_path.endswith('.shp'):
    geojson_file = f'{file_path[0 : len(file_path) - 4]}.geojson_{str(uuid.uuid4())}'
    geo.transform_shapefile_to_geojson_polygons(file_path, geojson_file, width, verbose)
  elif geojson_file is None and file_path.endswith('.csv'):
    geojson_file = f'{file_path[0 : len(file_path) - 4]}.geojson_{str(uuid.uuid4())}'
    geo.transform_csv_to_geojson_polygons(
      file_path,
      geojson_file,
      width,
      None,
      None,
      None,
      verbose,
    )
  elif geojson_file is None:
    geojson_file = file_path

  return geojson_file

def load_features(geojson_file, verbose = False):
  features = []
  with open(geojson_file, 'r') as f:
    fc = json.load(f)
    features += fc.get('features', [fc])

  for i in range(len(features)):
    if features[i].get('geometry', features[i]).get('type') == 'MultiPolygon':
      features[i] = geo.explode_multipolygon(features[i])
  features = geo.flat_list(features)

  for feature in features:
    properties = feature.get('properties', {})

  features = [geo.convert_to_geojson_poly(f, DEFAULT_WIDTH, verbose) for f in features]
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

  return features

def query(
  file_path,
  authorization,
  width=DEFAULT_WIDTH,
  num_threads=DEFAULT_THREADS,
  verbose=False,    
):
  geojson_file = transform_input(
    file_path,
    width,
    verbose,
  )

  features = load_features(geojson_file, verbose)
  mapfeatures = query_mapfeatures(
    features,
    authorization,
    num_threads,
    verbose,
  )

  return mapfeatures

def download_file(
  url,
  local_path,
  authorization,
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
        authorization,
        verbose,
        False,
        pbar,
        True)

    with open(local_path, 'wb') as f:
      shutil.copyfileobj(r.raw, f)

    if verbose:
      print(f'Downloaded {local_path}')

  if pbar:
    pbar.update(1)

  return local_path

def download_imagery(
  mapfeatures,
  output_dir,
  authorization,
  num_threads=DEFAULT_THREADS,
  verbose=False,
):
  urls = []
  for mf in mapfeatures:
    url = mf.get('url', None)
    if url is not None:
      urls.append(url)

  executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_threads)
  futures = []
  pbar = None

  if verbose:
    print(f'Downloading with {num_threads} threads...')
    pbar = tqdm(total=len(mapfeatures))

  for mf in mapfeatures:
    url = mf.get('url', None)
    if url is None:
      if pbar is not None:
        pbar.update(1)
      continue

    local_img_path = os.path.join(output_dir, 'images', f'{mf.get("id")}.jpg')
    mf['image_path'] = local_img_path
    future = executor.submit(
      download_file,
      url,
      local_img_path,
      authorization,
      None,
      verbose,
      pbar,
    )
    futures.append(future)

  for future in concurrent.futures.as_completed(futures):
    try:
      results = future.result()
    except Exception as e:
      print(e)

  if pbar is not None:
    pbar.close()

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('-i', '--input_file', type=str, help='Input file', required=True)
  parser.add_argument('-o', '--output_dir', type=str, required=True)
  parser.add_argument('-w', '--width', type=int, default=DEFAULT_WIDTH)
  parser.add_argument('-a', '--authorization', type=str, required=True)
  parser.add_argument('-c', '--num_threads', type=int, default=DEFAULT_THREADS)
  parser.add_argument('-v', '--verbose', action='store_true')
  parser.add_argument('-z', '--zip_images', action='store_true')
  args = parser.parse_args()

  os.makedirs(args.output_dir, exist_ok=True)

  mapfeatures = query(
    args.input_file,
    args.authorization,
    args.width,
    args.num_threads,
    args.verbose,
  )

  download_imagery(
    mapfeatures,
    args.output_dir,
    args.authorization,
    args.num_threads,
    args.verbose,
  )

  loc = os.path.join(args.output_dir, 'mapfeatures.json')
  with open(loc, 'w') as f:
    json.dump(mapfeatures, f, indent=2)

  if args.zip_images:
    replace_dirs_with_zips(args.output_dir, False, args.verbose)
