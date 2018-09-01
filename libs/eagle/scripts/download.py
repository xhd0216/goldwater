from collections import namedtuple

links = namedtuple('links', ['description', 'link', 'since', 'early'])

LINK_TMPLS = [
  links('Disaggregated Futures Only Reports', 
        'https://www.cftc.gov/files/dea/history/fut_disagg_txt_%s.zip',
        2010, '2006_2016'),
  links('Disaggregated Futures-and-Options Combined Reports',
        'https://www.cftc.gov/files/dea/history/com_disagg_txt_2018.zip',
        2010, '2006_2016'),
  links('Traders in Financial Futures ; Futures Only Reports',
        'https://www.cftc.gov/files/dea/history/fut_fin_txt_2018.zip',
        2010, '2006_2016'),
  links('Traders in Financial Futures ; Futures-and-Options Combined Reports',
        'https://www.cftc.gov/files/dea/history/com_fin_txt_2018.zip',
        2010, '2006_2016'),
  links('Futures Only Reports',
        'https://www.cftc.gov/files/dea/history/deacot2018.zip',
        1986, '1986_2016'),
  links('Futures-and-Options Combined Reports',
        'https://www.cftc.gov/files/dea/history/deahistfo2018.zip',
        1995, '1995_2016'),
  links('Commodity Index Trader Supplement',
        'https://www.cftc.gov/files/dea/history/dea_cit_txt_2018.zip',
        2006, '2006_2016'),
]


def download_by_year(year):
  """ download files by year """
  pass
  
def main():
  pass

