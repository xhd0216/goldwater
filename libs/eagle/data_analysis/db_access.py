import requests
import sqlalchemy as sa

import cot_retriever.db as db

headers = {'Accept-Encoding': 'gzip, deflate, br', 'Connection': 'keep-alive', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8', 'Upgrade-Insecure-Requests': 1, 'Host': 'www.alphavantage.co', 'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0'}

url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=gld&apikey=GLP4NLROVIIPJAEP'

resp = requests.get(url, params=headers)

import json

js = json.loads(resp.content)
print json.dumps(js, indent=3)

def test_data():
  engine = sa.create_engine('sqlite:///' + '../cot_retriever/data/DFO/db.db')
  res = db.retrieve_commodity_data(engine, 'GOLD')
  


