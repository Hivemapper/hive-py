import subprocess

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

def clahe_smart_clip(
  img_path,
  out_path,
  verbose = False,
  x_pct = 15,
  y_pct = 30,
  bins = 512,
):
  """
  from "A Generic Image Processing Pipeline for Enhancing Accuracy
    and Robustness of Visual Odometry"
  """
  vals = brightness_stats(img_path, verbose)
  _min = vals['min']
  _max = vals['max']
  _mean = vals['mean']
  _median = vals['median']

  clip = ((_max - _min) / _median) if _median > 0 else _mean

  clahe(img_path, out_path, x_pct, y_pct, bins, clip, verbose)
