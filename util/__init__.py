import csv
import os
import shutil

from datetime import datetime
from tqdm import tqdm
from zipfile import ZipFile
from . import geo, optical_flow, stitching

def replace_dirs_with_zips(output_dir, zip_images_only = False, verbose=False):
  if verbose:
    print(f'Compressing folders in {output_dir}...')

  contents = os.listdir(output_dir)
  for content in tqdm(contents):
    p = os.path.join(output_dir, content)
    if os.path.isfile(p):
      continue

    new_path = p + '.zip'
    with ZipFile(new_path, 'w') as zf:
      for f in os.listdir(p):
        if zip_images_only and f.endswith('json'):
          continue
        zf.write(os.path.join(p, f), f)
    shutil.rmtree(p)

def write_csv_from_csv(
  input_path,
  output_path,
  custom_id_field,
  tracked_by_id,
  output_dir_field_name,
  custom_date_formatting=None,
  output_date_field_name=None,
  success_field_name=None
):
  out_file = open(output_path, 'w', newline='')

  with open(input_path, newline='') as csvfile:
    reader = csv.reader(csvfile)
    writer = csv.writer(out_file, quoting=csv.QUOTE_MINIMAL)
    custom_id_idx = -1
    output_dir_idx = -1
    output_date_idx = -1
    success_id_idx = -1
    for row in reader:
      needs_id_field = custom_id_idx == -1
      needs_output_dir_field = output_dir_idx == -1
      needs_output_date_field = output_date_idx == -1 and output_date_field_name
      needs_success_field = success_id_idx == -1 and success_field_name
      if needs_id_field or needs_output_dir_field or needs_success_field:
        for i, col in enumerate(row):
          if col == custom_id_field:
            custom_id_idx = i
          elif needs_output_dir_field and col == output_dir_field_name:
            output_dir_idx = i
          elif needs_success_field and col == success_field_name:
            success_id_idx = i
          elif needs_output_date_field and col == output_date_field_name:
            output_date_idx = i
        writer.writerow(row)
        continue

      custom_id = row[custom_id_idx]
      if custom_id in tracked_by_id:
        row[output_dir_idx] = custom_id
        if success_id_idx > -1:
          row[success_id_idx] = True
        if output_date_idx > -1:
          a_date = tracked_by_id.get(custom_id)
          a_formatted_date = a_date
          if custom_date_formatting is not None:
            a_formatted_date = datetime.fromisoformat(a_formatted_date.split('.')[0])
            a_formatted_date = a_formatted_date.strftime(custom_date_formatting)
          row[output_date_idx] = a_formatted_date
      elif success_id_idx > -1:
        row[success_id_idx] = False
      writer.writerow(row)

  out_file.close()

__all__ = [
    'replace_dirs_with_zips',
    'write_csv_from_csv'
    'geo',
    'optical_flow', 
    'stitching'
]
