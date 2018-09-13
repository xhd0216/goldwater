import argparse
import logging
import os
import shutil
import sys

from download import download_all_years, download_weekly_file
import upload

def get_header_prefix(header):
  """ return the prefix of a header """
  return header.replace('_', '&').replace('-', '&').replace(' ', '&').split('&')[0].lower()



def generate_fields_for_table(path):
  """ generate fields and types """
  header_file = os.path.join(path, 'headers')
  out_file = os.path.join(path, 'fields')
  with open(header_file, 'r') as f, open(out_file, 'w') as g:
    for hs in f.readlines():
      field = hs.split(';')[0]
      field = field.replace('%', 'pct') \
                   .replace('(', '_') \
                   .replace('-', '_') \
                   .replace(' ', '_') \
                   .replace(')', ' ').strip() \
                   .replace('__', '_') \
                   .lower()
      pref = get_header_prefix(field)
      if pref in ['as']:
        field_type = 'date'
      elif pref in ['cftc', 'contract', 'futonly', 'market', 'report']:
        field_type = 'str'
      elif pref in ['pct', 'conc', 'concentration']:
        field_type = 'float'
      elif pref in ['asset', 'change', 'cit', 'comm', 'commercial', 'dealer', 'lev', 'm', 'ncomm', 'noncommercial', 'nonreportable', 'nonrept', 'open', 'other', 'prod', 'swap', 'tot', 'total', 'traders']:
        field_type = 'int'
      else:
        raise ValueError('invalid prefix %s in %s', pref, path)
      g.write(field + ':' + field_type + '\n')
  return out_file


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--year', action='store')
  parser.add_argument('--path', action='store', required=True)
  parser.add_argument('--unzip', action='store_true', default=False)
  parser.add_argument('--force', action='store_true', default=False)
  parser.add_argument('--table', action='store')
  parser.add_argument('--update', action='store_true', default=False)

  opts = parser.parse_args()

  if opts.update:
    download_weekly_file(None)
    exit(0)
  if not opts.year:
    ret = download_all_years(opts.table, opts.path, opts.unzip)
    if opts.unzip:
      def get_table_name(fold):
        """ foler is like ./data/DFO/deahis_2018 , return DFO """
        return os.path.basename(os.path.split(fold)[0])
      for folder in ret:
        if '2018' not in folder:
          continue
        txt = [f for f in os.listdir(folder) if f[-4:].lower() == '.txt']
        assert len(txt) == 1
        filepath = os.path.join(folder, txt[0])
        with open(filepath, 'r') as f:
          lines = f.readlines()
        headers = upload.get_headers(lines[0])
        data = upload.split(lines[1], ',')
        assert len(headers) == len(data)
        up_dir = os.path.join(folder, '../')
        with open(os.path.join(up_dir, 'headers'), 'w') as f:
          for a in zip(headers, data):
            f.write(a[0] + ';' + a[1] + '\n')
        fields = generate_fields_for_table(up_dir)
        upload.create_db_file(up_dir)

      for folder in ret:
        upload.insert_to_table(folder)

  exit(0)



if __name__=='__main__':
  logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
  main()
