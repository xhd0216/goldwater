import argparse
import logging
import os
import sys

from download import download_all_years
import upload




def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--year', action='store')
  parser.add_argument('--path', action='store', required=True)
  parser.add_argument('--unzip', action='store_true', default=False)
  parser.add_argument('--force', action='store_true', default=False)

  opts = parser.parse_args()

  if not opts.year:
    ret = download_all_years(opts.path, opts.unzip)
    for folder in ret:
      print "========", folder
      txt = [f for f in os.listdir(folder) if f[-4:] == '.txt']
      assert len(txt) == 1
      filepath = os.path.join(folder, txt[0])
      print "========", filepath
      with open(filepath, 'r') as f:
        lines = f.readlines()
        header_line = lines[0]
        data_line = lines[1]
      headers = upload.get_headers(header_line)
      data = upload.split(lines[1], ',')
      assert len(headers) == len(data)
      for a in zip(headers, data):
        print a
      break
      
  else:
    download_by_year(opts.year, opts.path)

  exit(0)


if __name__=='__main__':
  logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
  main()
