# hive-py

## Install
Clone locally and run

```
pip install -r requirements.txt
```

## Notes & Limitations
- Generate a base64 encoded string like f'my-user-name:{apiKey}' to use as input for `authorization`
- The Imagery API demo restricts queries to Polygons with a maximum area of 1 km^2
  - This wrapper supports automatically breaking up large geometries into smaller geometries behind the scenes

# Usage
### Imagery CLI
```
> python -m imagery.query
usage: query.py [-h] -i INPUT_FILE [-s START_DAY] [-e END_DAY] [-L] [-x] [-d MAX_DIST] [-l MAX_LAG] [-z MAX_ANGLE] -o OUTPUT_DIR [-g] [-w WIDTH] [-M]
                [-I CUSTOM_ID_FIELD] [-S CUSTOM_MIN_DATE_FIELD] [-k] [-E] [-K SKIP_GEO_FILE] [-P IMAGE_POST_PROCESSING] -a AUTHORIZATION [-c NUM_THREADS] [-v] [-C]

options:
  -h, --help            show this help message and exit
  -i INPUT_FILE, --input_file INPUT_FILE
  -s START_DAY, --start_day START_DAY
  -e END_DAY, --end_day END_DAY
  -L, --latest
  -x, --stitch
  -d MAX_DIST, --max_dist MAX_DIST
  -l MAX_LAG, --max_lag MAX_LAG
  -z MAX_ANGLE, --max_angle MAX_ANGLE
  -o OUTPUT_DIR, --output_dir OUTPUT_DIR
  -g, --export_geojson
  -w WIDTH, --width WIDTH
  -M, --merge_metadata
  -I CUSTOM_ID_FIELD, --custom_id_field CUSTOM_ID_FIELD
  -S CUSTOM_MIN_DATE_FIELD, --custom_min_date_field CUSTOM_MIN_DATE_FIELD
  -k, --camera_intrinsics
  -E, --update_exif
  -K SKIP_GEO_FILE, --skip_geo_file SKIP_GEO_FILE
  -P IMAGE_POST_PROCESSING, --image_post_processing IMAGE_POST_PROCESSING
  -a AUTHORIZATION, --authorization AUTHORIZATION
  -c NUM_THREADS, --num_threads NUM_THREADS
  -v, --verbose
  -C, --cache
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
python -m imagery.query -v -M --input_file "test_feature.json" --start_day "2023-07-28" --end_day "2023-07-28" --output_dir "temp" --authorization <your encoded key string>
```

### Query imagery for a GeoJSON Polygon Feature, use cache for resumable
```
python -m imagery.query -v -M -C --input_file "test_feature.json" --start_day "2023-07-28" --end_day "2023-07-28" --output_dir "temp" --authorization <your encoded key string>
```

### Query imagery for a GeoJSON Polygon FeatureCollection; stitch together; save a GeoJSON of LineStrings
```
python -m imagery.query -v -M -x -g --input_file "test_feature_col.json" --start_day "2023-07-28" --end_day "2023-07-28" --output_dir "temp" --authorization <your encoded key string>
```

### Query imagery for a GeoJSON Polygon FeatureCollection; save a GeoJSON of points
```
python -m imagery.query -v -M -g --input_file "test_feature_col.json" --start_day "2023-07-28" --end_day "2023-07-28" --output_dir "temp" --authorization <your encoded key string>
```

### Query imagery for a GeoJSON Polygon Feature, add camera intrinsics and encode to exif
```
python -m imagery.query -v -M -k -E --input_file "test_feature.json" --start_day "2023-07-28" --end_day "2023-07-28" --output_dir "temp" --authorization <your encoded key string>
```
`Focal Length` is encoded in pixel units (i.e., not mm)
`Lens` is encoded as `<k1> <k2>`

### Converting .shp to Hivemapper-optimized GeoJSON
```
> python -m util.geo -h
usage: geo.py [-h] [-s SHAPEFILE] [-c CSVFILE] -o OUTPUT_JSON [-w WIDTH] [-I CUSTOM_ID_FIELD] [-S CUSTOM_MIN_DATE_FIELD] [-q]

options:
  -h, --help            show this help message and exit
  -s SHAPEFILE, --shapefile SHAPEFILE
  -c CSVFILE, --csvfile CSVFILE
  -o OUTPUT_JSON, --output_json OUTPUT_JSON
  -w WIDTH, --width WIDTH
  -I CUSTOM_ID_FIELD, --custom_id_field CUSTOM_ID_FIELD
  -S CUSTOM_MIN_DATE_FIELD, --custom_min_date_field CUSTOM_MIN_DATE_FIELD
  -q, --quiet
```

### Skipping last output frames areas
```
> python -m util.geo -h
python -m imagery.query -v -M -g --input_file "test_feature_col.json" --start_day "2023-07-28" --end_day "2023-07-28" --output_dir "temp" --authorization <your encoded key string> -K last_out/frames.geojson
```

### Skipping multiple output frames areas
```
> python -m util.geo -h
python -m imagery.query -v -M -g --input_file "test_feature_col.json" --start_day "2023-07-28" --end_day "2023-07-28" --output_dir "temp" --authorization <your encoded key string> -K last_out/frames.geojson,another_out/frames.geojson
```

### Querying API Usage
```
usage: info.py [-h] -a AUTHORIZATION [-b] [-l LIMIT] [-t] [-v]

options:
  -h, --help            show this help message and exit
  -a AUTHORIZATION, --authorization AUTHORIZATION
  -b, --balance
  -l LIMIT, --limit LIMIT
  -t, --history
  -v, --verbose
```

### Querying Remaining API Credit Balance
``` 
python -m account.info -ba <your encoded key string>
```

### Querying API Transaction history (default limit of 25)
``` 
python -m account.info -ta <your encoded key string>
```

## Post Processing
- Install ImageMagick >=7.0.0
- Use Python >=3.7
```
python -m imagery.query -v -M -x -g --input_file "test_feature_col.json" --start_day "2023-07-28" --end_day "2023-07-28" --output_dir "temp" --authorization <your encoded key string> -P clahe-smart-clip
```

### `clahe-smart-clip` (Contrast Limited Adaptive Histogram Equalization with Smart Clipping)
- https://en.wikipedia.org/wiki/Adaptive_histogram_equalization#Contrast_Limited_AHE
- https://imagemagick.org/script/clahe.php
- https://www.mdpi.com/1424-8220/22/22/8967

It's highly recommended to use the module directly in order to preserve the original imagery, as well as to tune values for your own purposes.

By default, settings are naively configured to sacrifice aesthetics to improve unsupervised feature detection. Some general deep learning inference use cases and human in the loop use cases may also see benefits from these default settings.

#### Mitigating Direct Sunlight
![directsun](https://github.com/Hivemapper/hive-py/assets/3093002/46b84ee7-eb5f-4527-92d1-6d48c36b3436)
![clahe1](https://github.com/Hivemapper/hive-py/assets/3093002/f6554add-dd1f-44a2-a0b8-2d5b8fc0d82e)

#### Mitigating Heavy Shadows
![dark1](https://github.com/Hivemapper/hive-py/assets/3093002/8a2cd6cf-910a-4f2b-b680-3c1f003d33f7)
![clahe2](https://github.com/Hivemapper/hive-py/assets/3093002/8e46c7f4-8ff1-4f62-a2bb-d16fdd1a06c0)

