import os
import sqlalchemy as sa

import db

dir_path = os.path.dirname(os.path.realpath(__file__))
engine = sa.create_engine("sqlite:////%s/data/TFF/db.db" % dir_path)
ret = db.retrieve_commodity_data(engine, "s&p")


for r in ret.fetchall():
  print r
