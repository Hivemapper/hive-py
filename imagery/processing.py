import cv2 as cv
import hashlib
import numpy as np
import os
import subprocess

from datetime import datetime
from exiftool import ExifToolHelper

TAGS_TO_KEEP = [
  'EXIF:DateTimeOriginal',
  'EXIF:Orientation',
  'EXIF:FocalLength',
  'EXIF:SubSecTimeOriginal',
  'EXIF:GPSVersionID',
  'EXIF:GPSLatitudeRef',
  'EXIF:GPSLatitude',
  'EXIF:GPSLongitudeRef',
  'EXIF:GPSLongitude',
  'EXIF:GPSAltitudeRef',
  'EXIF:GPSAltitude',
  'EXIF:GPSDOP',
  'XMP:XMPToolkit',
  'XMP:Lens',
]

def clahe(
  img_path,
  out_path,
  x_pct = 15,
  y_pct = 30,
  bins = 512,
  clip = 1,
  verbose = False,
):
  tokens = [
    'convert',
    img_path,
    '-clahe',
    f'{x_pct}x{y_pct}%+{bins}+{clip}',
    out_path
  ]

  if verbose:
    print(' '.join(tokens))
    subprocess.run(tokens)
  else:
    subprocess.run(tokens, stdout=subprocess.DEVNULL)

def brightness_stats(img_path, verbose = False):
  tokens = [
    'magick',
    img_path,
    '-colorspace',
    'gray',
    '-verbose',
    'info:'
  ]

  if verbose:
    print(' '.join(tokens))
    result = subprocess.run(tokens, capture_output=True, text=True)
  else:
    result = subprocess.run(tokens, stdout=subprocess.DEVNULL, capture_output=True, text=True)

  vals = {
    'min': None,
    'max': None,
    'mean': None,
    'median': None,
  }

  for line in result.stdout.split('\n'):
    parts = line.strip().split(' ')
    key = parts[0].replace(':', '')
    if key in vals:
      vals[key] = float(parts[1])

  return vals

def is_processed(img_path, process_name, cache_dir):
  loc = None
  with open(img_path, 'rb') as f:
    h = hashlib.md5(f.read()).hexdigest()
    loc = os.path.join(cache_dir, f'{process_name}:{h}')

  return os.path.isfile(loc)

def cache_processed_status(img_path, process_name, cache_dir):
  h = None
  with open(img_path, 'rb') as f:
    h = hashlib.md5(f.read()).hexdigest()

  loc = os.path.join(cache_dir, f'{process_name}:{h}')

  with open(loc, 'w') as f:
    f.write(datetime.now().isoformat())

def clahe_smart_clip(
  img_path,
  out_path,
  verbose = False,
  cache_dir = None,
  x_pct = 15,
  y_pct = 30,
  bins = 512,
):
  """
  from "A Generic Image Processing Pipeline for Enhancing Accuracy
    and Robustness of Visual Odometry"
  """
  if cache_dir:
    was_processed = is_processed(img_path, 'clahe', cache_dir)
    if was_processed:
      if verbose:
        print(f'Using cached version...')
      return

  vals = brightness_stats(img_path, verbose)
  _min = vals['min']
  _max = vals['max']
  _mean = vals['mean']
  _median = vals['median']

  clip = ((_max - _min) / _median) if _median > 0 else _mean

  clahe(img_path, out_path, x_pct, y_pct, bins, clip, verbose)

  if cache_dir:
    cache_processed_status(img_path, 'clahe', cache_dir)

def undistort_via_exif(
  img_path,
  out_path,
  verbose = False,
  cache_dir = None
):
  if cache_dir:
    was_processed = is_processed(img_path, 'undistort', cache_dir)
    if was_processed:
      if verbose:
        print(f'Using cached version...')
      return True

  if verbose:
    print(img_path)
    print(f'Undistorting {img_path}...')

  f = 0.0
  k1 = 0.0
  k2 = 0.0
  k3 = 0.0
  k4 = 0.0
  tags = {}

  with ExifToolHelper() as et:
    tags = et.get_tags(img_path, [])[0]
    f = float(tags.get('EXIF:FocalLength'))
    k1, k2 = [float(x) for x in tags.get('XMP:Lens').split(' ')]

  img = cv.imread(img_path)
  h, w= img.shape[:2]

  mtx = np.array([
    [f, 0.0, w / 2.0],
    [0.0, f, h / 2.0],
    [0.0, 0.0, 1.0],
  ], dtype=np.float64)

  dist = np.array([k1, k2, k3, k4], np.float64)

  newcameramtx, roi = cv.getOptimalNewCameraMatrix(mtx, dist, (w,h), 1, (w,h))

  R = np.eye(3, dtype=np.float64)

  mapx, mapy = cv.initUndistortRectifyMap(mtx, dist, R, newcameramtx, (w,h), cv.CV_32FC1)
  dst = cv.remap(img, mapx, mapy, interpolation=cv.INTER_LINEAR)

  if verbose:
    print(f'Writing {out_path}...')
  cv.imwrite(out_path, dst)

  with ExifToolHelper() as et:
    tags = { k: tags[k] for k in TAGS_TO_KEEP if k in tags }
    if verbose:
      print(f'Encoding exif tags from {img_path} to {out_path}...')
    et.set_tags(
      [out_path],
      tags=tags,
      params=['-overwrite_original']
    )

  if cache_dir:
    cache_processed_status(img_path, 'undistort', cache_dir)

  return True
