import argparse
import json
import geopy.distance
import numpy as np
import os
import uuid

from datetime import datetime
from dateutil import tz
from geographiclib.geodesic import Geodesic
from pyproj import Transformer
from scipy.spatial import KDTree
from timezonefinder import TimezoneFinder
from tqdm import tqdm
from util import geo

DEFAULT_STITCH_MAX_DISTANCE = 30 # 30 m
DEFAULT_STITCH_MAX_LAG = 360 # 6 min
DEFAULT_STITCH_MAX_ANGLE = 100 # right angle turn with margin

WGS_TO_MERCATOR = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

def get_timezone(frame):
  lon = frame.get('position').get('lon')
  lat = frame.get('position').get('lat')
  tf = TimezoneFinder()
  tz_name = tf.timezone_at(lng=lon, lat=lat)

  return tz.gettz(tz_name)

def json_iso_str_to_date(s, new_tz=None):
  ts = datetime.fromisoformat(s.replace('Z', ''))
  ts = ts.replace(tzinfo=tz.tzutc())
  if new_tz:
    ts = ts.astimezone(new_tz)

  return ts

def frame_mercator(frame):
  pos = frame.get('position')
  coord = [pos.get('lon'), pos.get('lat')]
  x, y = WGS_TO_MERCATOR.transform(*coord[0:2])

  return x, y

def build_kdtree(seqs):
  data = np.zeros((len(seqs), 2))
  for i, seq in enumerate(seqs):
    pos_a = seq[0].get('position')
    coord_a = [pos_a.get('lon'), pos_a.get('lat')]
    x_a, y_a = WGS_TO_MERCATOR.transform(*coord_a[0:2])
    x, y = frame_mercator(seq[0])
    data[i, 0] = x
    data[i, 1] = y

  return KDTree(data, compact_nodes=True)

def seqs_lag(seq_a, seq_b):
  t0 = json_iso_str_to_date(seq_a[-1].get('timestamp'))
  t1 = json_iso_str_to_date(seq_b[0].get('timestamp'))

  return (t1 - t0).seconds

def frame_azi(a, b):
  a0 = a.get('position')
  b0 = b.get('position')

  a0_lon = a0.get('lon')
  a0_lat = a0.get('lat')
  b0_lon = b0.get('lon')
  b0_lat = b0.get('lat')

  azi = Geodesic.WGS84.Inverse(a0_lat, a0_lon, b0_lat, b0_lon).get('azi2')
  return azi

def seqs_azi_delta(seq_a, seq_b):
  a0 = seq_a[-2].get('position')
  a1 = seq_a[-1].get('position')

  b0 = seq_b[0].get('position')
  b1 = seq_b[1].get('position')

  a0_lon = a0.get('lon')
  a0_lat = a0.get('lat')
  a1_lon = a1.get('lon')
  a1_lat = a1.get('lat')

  b0_lon = b0.get('lon')
  b0_lat = b0.get('lat')
  b1_lon = b1.get('lon')
  b1_lat = b1.get('lat')

  azi_a = Geodesic.WGS84.Inverse(a0_lat, a0_lon, a1_lat, a1_lon).get('azi2')
  azi_b = Geodesic.WGS84.Inverse(b0_lat, b0_lon, b1_lat, b1_lon).get('azi2')

  return geo.abs_angular_delta(azi_a, azi_b)

def cluster_seqs(
  seqs,
  max_dist = DEFAULT_STITCH_MAX_DISTANCE,
  max_lag = DEFAULT_STITCH_MAX_LAG,
  max_azimuth_delta = DEFAULT_STITCH_MAX_ANGLE,
  verbose=False,
):
  if len(seqs) == 1:
    return [seqs]

  tree = build_kdtree(seqs)
  clusters = []

  remaining = set([i for i in range(len(seqs))])
  remaining.remove(0)

  cluster = [seqs[0]]
  cur_seq = 0
  last_pos = frame_mercator(seqs[0][-1])

  cluster_done = False

  while remaining:
    if cluster_done:
      clusters.append(cluster)
      cluster_done = False
      cur_seq = remaining.pop()
      cluster = [seqs[cur_seq]]
      last_pos = frame_mercator(seqs[cur_seq][-1])

    candidate_idxs = tree.query_ball_point(last_pos, max_dist, return_sorted=True)
    candidate_idxs = [i for i in candidate_idxs if i in remaining]

    if not candidate_idxs:
      cluster_done = True
      continue

    for i in candidate_idxs:
      seq = seqs[i]
      lag = seqs_lag(seqs[cur_seq], seq)
      if lag > max_lag:
        cluster_done = True
        break

      delta_azi = seqs_azi_delta(seqs[cur_seq], seq)
      if delta_azi > max_azimuth_delta:
        continue

      remaining.remove(i)
      cur_seq = i
      cluster.append(seq)
      last_pos = frame_mercator(seq[-1])
      break

  clusters.append(cluster)
  return clusters

def stitch(
  frames,
  max_dist = DEFAULT_STITCH_MAX_DISTANCE,
  max_lag = DEFAULT_STITCH_MAX_LAG,
  max_azimuth_delta = DEFAULT_STITCH_MAX_ANGLE,
  verbose=False,
):
  if not frames:
    # TODO
    return []

  sorted_frames = sorted(frames, key=lambda f: f.get('timestamp'))

  by_sequence = {}
  for frame in sorted_frames:
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

  to_tz = get_timezone(sorted_frames[0])
  if verbose:
    print(f'First frame in TZ={to_tz}')

  seq_by_day = {}
  for seq in seqs:
    d = json_iso_str_to_date(seq[0].get('timestamp'), to_tz)
    day = d.strftime('%Y-%m-%d')
    seq_by_day.setdefault(day, [])
    seq_by_day[day].append(seq)

  if verbose:
    print(f'Found frames across {len(seq_by_day)} days (assuming {to_tz})')

  for day, seqs in seq_by_day.items():
    seq_by_day[day] = sorted(seqs, key=lambda s: s[0].get('timestamp'))

  clusters = []

  for day, seqs in seq_by_day.items():
    clusters += cluster_seqs(seqs, max_dist, max_lag, max_azimuth_delta)

  stitched = [[f for seq in cluster for f in seq] for cluster in clusters]
  skipped = [[ f for seq in cluster for f in seq] for cluster in skip_stitching]
  if verbose:
    print(f'Stitched {len(stitched)} paths!')
    print(f'Skipped {len(skipped)} paths.')

  for sequence in stitched:
    for i in range(1, len(sequence)):
      azi = frame_azi(sequence[i], sequence[i - 1])
      sequence[i]['heading'] = azi
      if i == 1:
        sequence[0]['heading'] = azi

  return stitched + skipped

def read_seqs(root, manifest, verbose):
  if verbose:
    print(f'Reading {root}')

  contents = os.listdir(root)
  for content in tqdm(contents):
    p = os.path.join(root, content)
    if os.path.isfile(p):
      continue

    seq_contents = os.listdir(p)
    jpgs = set([f for f in seq_contents if f.endswith('.jpg')])
    meta_path = os.path.join(p, 'meta.json')

    with open(meta_path, 'r') as f:
      meta = json.load(f)
      for img in meta.keys():
        if img in jpgs:
          d = meta[img]
          sid = f"{d['sequence']}/{d['idx']}.jpg"
          manifest[sid] = d
          manifest[sid]['loc'] = os.path.join(p, img)

def traversal_gte(seq, min_seq_size):
  if len(seq) < 2:
    return False

  total = 0

  for i in range(1, len(seq)):
    cur = (seq[i]['position']['lat'], seq[i]['position']['lon'])
    prev = (seq[i - 1]['position']['lat'], seq[i - 1]['position']['lon'])
    d = geopy.distance.distance(
      cur,
      prev,
    ).meters
    total += d

    if total >= min_seq_size:
      return True

  return False

def restitch(
  root,
  outdir,
  max_dist=DEFAULT_STITCH_MAX_DISTANCE,
  max_lag=DEFAULT_STITCH_MAX_LAG,
  max_angle=DEFAULT_STITCH_MAX_ANGLE,
  min_seq_size=None,
  verbose=True,
):
  manifest = {}
  read_seqs(root, manifest, verbose)

  frames = manifest.values()
  stitched = stitch(frames, max_dist, max_lag, max_angle, verbose)

  if verbose:
    print(f'{len(stitched)} sequences')

  if min_seq_size is not None:
    if verbose:
      print(f'Filtering out sequences < {min_seq_size} m')
    stitched = [seq for seq in tqdm(stitched) if traversal_gte(seq, min_seq_size)]
    if verbose:
      print(f'{len(stitched)} sequences remaining')

  if verbose:
    print(f'Linking new sequences in {outdir}')

  os.makedirs(outdir, exist_ok=True)

  for i, seq in enumerate(tqdm(stitched)):
    folder = f'{str(uuid.uuid4())}-{str(i)}'
    local_dir = os.path.join(outdir, folder)
    os.makedirs(local_dir, exist_ok=True)

    local_meta_path = os.path.join(local_dir, 'meta.json')
    img_paths = ["{}.jpg".format(i) for i in range(len(seq))]
    meta = {
      img_path:
        { key: frame[key] for key in frame if key != 'url' }
      for frame, img_path in zip(seq, img_paths)
    }
    with open(local_meta_path, 'w') as f:
      json.dump(meta, f, indent=4)

    for img_path, d in meta.items():
      new_path = os.path.join(local_dir, img_path)
      old_path = d['loc']
      os.link(old_path, new_path)

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('-R', '--restitch', type=str)
  parser.add_argument('-o', '--out', type=str)
  parser.add_argument('-d', '--max_dist', type=float, default=DEFAULT_STITCH_MAX_DISTANCE)
  parser.add_argument('-l', '--max_lag', type=float, default=DEFAULT_STITCH_MAX_ANGLE)
  parser.add_argument('-z', '--max_angle', type=float, default=DEFAULT_STITCH_MAX_LAG)
  parser.add_argument('-m', '--min_seq_size', type=float)
  parser.add_argument('-v', '--verbose', action='store_true')
  args = parser.parse_args()

  if args.restitch:
    assert(args.out)
    restitch(
      args.restitch,
      args.out,
      args.max_dist,
      args.max_lag,
      args.max_angle,
      args.min_seq_size,
      args.verbose,
    )
