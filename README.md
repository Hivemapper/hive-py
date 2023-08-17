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
usage: query.py [-h] -i GEOJSON -s START_DAY -e END_DAY -o OUTPUT_DIR -a AUTHORIZATION [-c NUM_THREADS] [-v]

options:
  -h, --help            show this help message and exit
  -i GEOJSON, --geojson GEOJSON
  -s START_DAY, --start_day START_DAY
  -e END_DAY, --end_day END_DAY
  -o OUTPUT_DIR, --output_dir OUTPUT_DIR
  -a AUTHORIZATION, --authorization AUTHORIZATION
  -c NUM_THREADS, --num_threads NUM_THREADS
  -v, --verbose
```

### Python API
**Query and download**
```
from imagery import query_frames

# make the API call to query available data
frames = query_frames(geojson_file, start_day, end_day, output_dir, authorization)

# download the content into folders grouped by its session id
download_files(frames, output_dir, num_threads)
```

## Example
### Query imagery for a GeoJSON Polygon Feature
```
python -m imagery.query --geojson "test_feature.json" --start_day "2023-07-31" --end_day "2023-07-31" --output_dir "temp" --authorization <your encoded key string>
```

### Query imagery for a GeoJSON Polygon FeatureCollection
```
python -m imagery.query --geojson "test_feature_col.json" --start_day "2023-07-31" --end_day "2023-07-31" --output_dir "temp" --authorization <your encoded key string>
```
