"""
  python -m imagery.query --geojson mygeo.json\
    --start_day 2023-06-01 --end_day 2023-06-08\
    --output_dir hm_out --authorization xxx
"""
import argparse
import concurrent.futures
import json
import os
import requests
import shutil

from area import area
from datetime import datetime, timedelta
from itertools import repeat

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

def download_files(frames, local_dir, num_threads=DEFAULT_THREADS, verbose=False):
  urls = [frame.get('url') for frame in frames]
  local_img_paths = [os.path.join(local_dir, url.split('.com/')[1].split('?')[0]) for url in urls]
  local_meta_paths = [p.replace("keyframes/", "metadata/").split('.')[0] + '.json' for p in local_img_paths]
  for frame, meta_path in zip(frames, local_meta_paths):
    os.makedirs(os.path.dirname(meta_path), exist_ok=True)
    with open(meta_path, 'w') as f:
      meta = {key : frame[key] for key in frame if key != 'url'}
      json.dump(meta, f, indent=4)

  with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
    print(f'Downloading with {num_threads} threads...')
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

def query(geojson_file, start_day, end_day, output_dir, authorization, num_threads=DEFAULT_THREADS, verbose=False):
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

  print(f'Querying {len(features)} features for imagery across {len(weeks)} weeks...')

  frames = query_imagery(features, weeks, authorization, output_dir, verbose)
  print(f'Found {len(frames)} images!')

  if frames:
    download_files(frames, output_dir, num_threads, verbose)

  print(f'{len(frames)} frames saved to {output_dir}!')

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('-i', '--geojson', type=str, required=True)
  parser.add_argument('-s', '--start_day', type=valid_date, required=True)
  parser.add_argument('-e', '--end_day', type=valid_date, required=True)
  parser.add_argument('-o', '--output_dir', type=str, required=True)
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
    args.num_threads,
    args.verbose,
  )
