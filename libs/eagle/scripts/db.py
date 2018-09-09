import sqlalchemy as sa


BEGIN_DATE = '1970-01-01'
END_DATE = '2070-01-01'
TABLE_NAME = 'table'

def retrieve_commodity_data(engine, comm, start_date=None, end_date=None):
  meta = sa.MetaData(engine, reflect=True)
  table = meta.tables[TABLE_NAME]
  pattern = '%' + comm + '%'
  print pattern
  query = table.select().where(table.c['market_and_exchange_names'].like(pattern))
  print query
  res = engine.execute(query)
  ind = 0
  for r in res:
    ind += 1
    print "row %s" % ind
    assert len(res.keys()) == len(r)
    

if __name__ == '__main__':
  print "begin"
  engine = sa.create_engine('sqlite:///./data/DFO/db.db')
  retrieve_commodity_data(engine, 'GOLD')
