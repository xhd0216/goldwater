import sqlalchemy as sa

import cot_retriever.db as db

def test_data():
  engine = sa.create_engine('sqlite:///' + '../cot_retriever/data/DFO/db.db')
  res = db.retrieve_commodity_data(engine, 'GOLD')
  


