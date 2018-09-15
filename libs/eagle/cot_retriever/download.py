import argparse
from collections import namedtuple
import datetime
import logging
import os
import requests
import zipfile

from upload import insert_to_table_2, insert_gld_table

links = namedtuple('links', ['description', 'link', 'since', 'early', 'weekly'])

this_year = datetime.datetime.now().year

LINK_TMPLS = {
  'DFO': links('Disaggregated Futures Only Reports',
        'https://www.cftc.gov/files/dea/history/fut_disagg_txt_%s.zip',
        2010, '2006_2016',
        'https://www.cftc.gov/dea/newcot/f_disagg.txt'),
  'DFOC': links('Disaggregated Futures-and-Options Combined Reports',
        'https://www.cftc.gov/files/dea/history/com_disagg_txt_%s.zip',
        2010, '2006_2016',
        'https://www.cftc.gov/dea/newcot/c_disagg.txt'),
  'TFF': links('Traders in Financial Futures ; Futures Only Reports',
        'https://www.cftc.gov/files/dea/history/fut_fin_txt_%s.zip',
        2010, '2006_2016',
        'https://www.cftc.gov/dea/newcot/FinFutWk.txt'),
  'TFOC': links('Traders in Financial Futures ; Futures-and-Options Combined Reports',
        'https://www.cftc.gov/files/dea/history/com_fin_txt_%s.zip',
        2010, '2006_2016',
        'https://www.cftc.gov/dea/newcot/FinComWk.txt'),
  'FO': links('Futures Only Reports',
        'https://www.cftc.gov/files/dea/history/deacot%s.zip',
        1986, '1986_2016',
        'https://www.cftc.gov/dea/newcot/deafut.txt'),
  'FOC': links('Futures-and-Options Combined Reports',
        'https://www.cftc.gov/files/dea/history/deahistfo_%s.zip',
        1995, '1995_2016',
        'https://www.cftc.gov/dea/newcot/deacom.txt'),
  'CITS': links('Commodity Index Trader Supplement',
        'https://www.cftc.gov/files/dea/history/dea_cit_txt_%s.zip',
        2006, '2006_2016',
        'https://www.cftc.gov/dea/newcot/deacit.txt'),
}

def get_url_content(url):
  resp = requests.get(url)
  if resp.status_code != 200:
    logging.error('failed to download url %s, response code: %s', url, resp.status_code)
    return None
  return resp.content


GLD_DOWNLOAD_URL = 'http://www.spdrgoldshares.com/assets/dynamic/GLD/GLD_US_archive_EN.csv'

def download_gld_price(url=GLD_DOWNLOAD_URL, comm='gld', store_path=None):
  resp = get_url_content(url)
  lines = resp.split('\n')[6:]
  if store_path is not None:
    open(os.path.join(store_path, comm), 'wb').write(resp.content)
    insert_gld_table(lines, store_path, comm)
  return lines



def download_by_year(link, year, path, force=False):
  """ download files by year """
  url = link.link % year
  filename = url.split('/')[-1]
  # bug fixing
  if link.description == 'Futures-and-Options Combined Reports' and year >= 2004:
    url = 'https://www.cftc.gov/files/dea/history/deahistfo%s.zip' % year
    filename = 'deahistfo_%s.zip' % year
  localpath = os.path.join(path, filename)
  if os.path.exists(localpath):
    if force:
      logging.warning('overwriting file %s', localpath)
    else:
      logging.error('file already exists %s', localpath)
      return localpath
  resp = get_url_content(url)
  open(localpath, 'wb').write(resp)
  logging.info('downloaded file: %s', localpath)
  return localpath


def unzip_file(path, new_path=None, force=False):
  """ unzip a file """
  if new_path is None:
    # assume the file name ends with .zip
    new_path = path[:-4]

  if os.path.exists(new_path):
    if force:
      logging.warning('file %s already exists, overwriting', new_path)
    else:
      logging.error('path %s already exists', new_path)
      return new_path

  zip_ref = zipfile.ZipFile(path, 'r')
  zip_ref.extractall(new_path)
  zip_ref.close()
  return new_path


def download_all_years(table, path, unzip=False, force=False):
  """ download all data """
  ret = []
  if table is None:
    types = LINK_TMPLS
  else:
    if table not in LINK_TMPLS:
      logging.error('unknkown table type: %s', table)
      return ret
    types = {table: LINK_TMPLS[table]}

  for key, link in types.iteritems():
    sub_path = os.path.join(path, key)
    if not os.path.exists(sub_path):
      os.makedirs(sub_path)
    year = link.since
    while year <= this_year:
      logging.info('downloading year: %s', year)
      filename = download_by_year(link, year, sub_path, force=force)
      print filename
      if unzip:
        try:
          res = unzip_file(filename, force=force)
        except Exception as e:
          logging.error('failed to unzip file %s', filename)
          logging.error(e)
          raise e
        ret.append(res)
      else:
        ret.append(filename)
      year += 1
  return ret


def download_weekly_file(path, table=None, store=None):
  """ download weekly file """
  if table is None:
    types = LINK_TMPLS
  else:
    if table not in LINK_TMPLS:
      #logging.error('unknkown table type: %s', table)
      raise ValueError('unknown table type: %s' % table)
    types = {table: LINK_TMPLS[table]}

  for key, link in types.iteritems(): 
    resp = get_url_content(link.weekly)
    if not resp:
      continue
    weekly_file = os.path.join(path, 'weekly_%s' % key)
    field_file = os.path.join(path, key, 'fields')
    db_file = os.path.join(path, key, 'db.db')
    open(weekly_file, 'wb').write(resp)

    insert_to_table_2(weekly_file, field_file, db_file, header_on=False)
