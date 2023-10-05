# hive-py

## Install
Clone locally and run

```
pip install -r requirements.txt
```

## Notes & Limitations
- Generate a base64 encoded string like f'my-user-name:{apiKey}' to use as input for `authorization`
- The Imagery API demo restricts queries to Polygons with a maximum area of 1 km^2

# Usage
### CLI
```
> python -m imagery.query
usage: query.py [-h] -i GEOJSON -s START_DAY -e END_DAY [-x] [-d MAX_DIST] [-l MAX_LAG] [-z MAX_ANGLE] -o OUTPUT_DIR [-g] -a AUTHORIZATION [-c NUM_THREADS] [-v]

options:
  -h, --help            show this help message and exit
  -i INPUT_FILE, --input_file GEOJSON
  -s START_DAY, --start_day START_DAY
  -e END_DAY, --end_day END_DAY
  -x, --stitch
  -d MAX_DIST, --max_dist MAX_DIST
  -l MAX_LAG, --max_lag MAX_LAG
  -z MAX_ANGLE, --max_angle MAX_ANGLE
  -o OUTPUT_DIR, --output_dir OUTPUT_DIR
  -g, --export_geojson
  -a AUTHORIZATION, --authorization AUTHORIZATION
  -w WIDTH, --width WIDTH
  -c NUM_THREADS, --num_threads NUM_THREADS
  -v, --verbose
```

### Python API
**Query and download**
```
from imagery import download_files, query_frames

# make the API call to query available data
frames = query_frames(geojson_file, start_day, end_day, output_dir, authorization)

# download the content into folders grouped by its session id
download_files(frames, output_dir)
```

## Example
### Query imagery for a GeoJSON Polygon Feature
```
python -m imagery.query -v --input_file "test_feature.json" --start_day "2023-07-28" --end_day "2023-07-28" --output_dir "temp" --authorization <your encoded key string>
```

### Query imagery for a GeoJSON Polygon FeatureCollection; stitch together; save a GeoJSON of LineStrings
```
python -m imagery.query -v -x -g --input_file "test_feature_col.json" --start_day "2023-07-28" --end_day "2023-07-28" --output_dir "temp" --authorization <your encoded key string>
```

### Query imagery for a GeoJSON Polygon FeatureCollection; save a GeoJSON of points
```
python -m imagery.query -v -g --input_file "test_feature_col.json" --start_day "2023-07-28" --end_day "2023-07-28" --output_dir "temp" --authorization <your encoded key string>
```

### Converting .shp to Hivemapper-optimized GeoJSON
```
> python -m util.geo -h
usage: geo.py [-h] -s SHAPEFILE -o OUTPUT_JSON [-w WIDTH] [-q]

options:
  -h, --help            show this help message and exit
  -s SHAPEFILE, --shapefile SHAPEFILE
  -o OUTPUT_JSON, --output_json OUTPUT_JSON
  -w WIDTH, --width WIDTH
  -q, --quiet

```