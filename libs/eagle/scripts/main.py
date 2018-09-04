import argparse
import logging
import os
import shutil
import sys

from download import download_all_years
import upload

def get_header_prefix(header):
  """ return the prefix of a header """
  return header.replace('_', '&').replace('-', '&').replace(' ', '&').split('&')[0].lower()

"""
=============== TFF
['As', 'Asset', 'CFTC', 'Change', 'Conc', 'Contract', 'Dealer', 'FutOnly', 'Lev', 'Market', 'NonRept', 'Open', 'Other', 'Pct', 'Report', 'Tot', 'Traders']
=============== CITS
['As', 'CFTC', 'CIT', 'Change', 'Comm', 'Contract', 'Market', 'NComm', 'NonRept', 'Open', 'Pct', 'Tot', 'Traders']
=============== FO
['%', 'As', 'CFTC', 'Change', 'Commercial', 'Concentration', 'Contract', 'Market', 'Noncommercial', 'Nonreportable', 'Open', 'Total', 'Traders']
=============== DFO
['As', 'CFTC', 'Change', 'Conc', 'Contract', 'FutOnly', 'M', 'Market', 'NonRept', 'Open', 'Other', 'Pct', 'Prod', 'Report', 'Swap', 'Tot', 'Traders']
=============== FOC
['%', 'As', 'CFTC', 'Change', 'Commercial', 'Concentration', 'Contract', 'Market', 'Noncommercial', 'Nonreportable', 'Open', 'Total', 'Traders']
=============== TFOC
['As', 'Asset', 'CFTC', 'Change', 'Conc', 'Contract', 'Dealer', 'FutOnly', 'Lev', 'Market', 'NonRept', 'Open', 'Other', 'Pct', 'Report', 'Tot', 'Traders']
=============== DFOC
['As', 'CFTC', 'Change', 'Conc', 'Contract', 'FutOnly', 'M', 'Market', 'NonRept', 'Open', 'Other', 'Pct', 'Prod', 'Report', 'Swap', 'Tot', 'Traders']
"""


def generate_fields_for_table(path):
  """ generate fields and types """
  header_file = os.path.join(path, 'headers_2018')
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


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--year', action='store')
  parser.add_argument('--path', action='store', required=True)
  parser.add_argument('--unzip', action='store_true', default=False)
  parser.add_argument('--force', action='store_true', default=False)
  parser.add_argument('--table', action='store')

  opts = parser.parse_args()

  if not opts.year:
    ret = download_all_years(opts.table, opts.path, opts.unzip)
    if opts.unzip:
      for folder in ret:
        txt = [f for f in os.listdir(folder) if f[-4:].lower() == '.txt']
        assert len(txt) == 1
        filepath = os.path.join(folder, txt[0])
        with open(filepath, 'r') as f:
          lines = f.readlines()
        headers = upload.get_headers(lines[0])
        data = upload.split(lines[1], ',')
        assert len(headers) == len(data)
        with open(os.path.join(folder, 'headers'), 'w') as f:
          for a in zip(headers, data):
            f.write(a[0] + ';' + a[1] + '\n')
        if '2018' in folder:
          print "=====================", folder
          shutil.copyfile(os.path.join(folder, 'headers'), os.path.join(folder, '../', 'headers_2018'))
          generate_fields_for_table(os.path.join(folder, '../'))
  else:
    download_by_year(opts.year, opts.path)

  exit(0)


if __name__=='__main__':
  logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
  main()
