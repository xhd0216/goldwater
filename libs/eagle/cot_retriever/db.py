import datetime
import logging
import sqlalchemy as sa
import sys

from upload import TABLE_NAME, DATABASE_NAME

BEGIN_DATE = '1970-01-01'
END_DATE = '2070-01-01'
MARKET_EXCHANGE_NAME = 'market_and_exchange_names'
DISAGG_RECORD_DATE = 'report_date_as_yyyy_mm_dd'

DISAGG_ALLS = [
	'open_interest_all',
	'prod_merc_positions_long_all',
	'prod_merc_positions_short_all',
	'swap_positions_long_all',
	'swap_positions_short_all',
	'swap_positions_spread_all',
	'm_money_positions_long_all',
	'm_money_positions_short_all',
	'm_money_positions_spread_all',
	'other_rept_positions_long_all',
	'other_rept_positions_short_all',
	'other_rept_positions_spread_all',
	'tot_rept_positions_long_all',
	'tot_rept_positions_short_all',
	'nonrept_positions_long_all',
	'nonrept_positions_short_all',
]

DISAGG_CHANGES = [
	'change_in_open_interest_all',
	'change_in_prod_merc_long_all',
	'change_in_prod_merc_short_all',
	'change_in_swap_long_all',
	'change_in_swap_short_all',
	'change_in_swap_spread_all',
	'change_in_m_money_long_all',
	'change_in_m_money_short_all',
	'change_in_m_money_spread_all',
	'change_in_other_rept_long_all',
	'change_in_other_rept_short_all',
	'change_in_other_rept_spread_all',
	'change_in_tot_rept_long_all',
	'change_in_tot_rept_short_all',
	'change_in_nonrept_long_all',
	'change_in_nonrept_short_all',
]

def get_record_date(rec):
  return datetime.datetime.strptime(rec[DISAGG_RECORD_DATE], '%Y-%m-%d').date()

def assert_date(a, b):
  c = b - a
  if c != datetime.timedelta(7):
    logging.info("date difference is not 7: last %s, now %s" % (a, b))
  assert c <= datetime.timedelta(8) and c >= datetime.timedelta(6)

def assert_changes(a, b):
  for (c, d) in zip(DISAGG_ALLS, DISAGG_CHANGES):
    if b[c] - b[d] != a[c]:
      logging.info('data not exactly matched: %s' % get_record_date(b))
      #print 'warning: data not valid:\ndate: %s\ncolumn: %s\nnew: %s\nold: %s\nchange: %s\nreal diff: %s\n' % (get_record_date(b), c, b[c], a[c], b[d], b[c]-a[c])
      assert abs(b[c] - b[d] - a[c]) == 1

def retrieve_commodity_data(engine, comm):
  meta = sa.MetaData(engine, reflect=True)
  table = meta.tables[TABLE_NAME]
  pattern = '%' + comm + '%'
  query = table.select().where(table.c[MARKET_EXCHANGE_NAME].like(pattern)).order_by(DISAGG_RECORD_DATE)
  res = engine.execute(query)
  last_record = None
  for r in res:
    if last_record is not None:
      assert_date(get_record_date(last_record), get_record_date(r))
      assert_changes(last_record, r)
    last_record = r
  return res


if __name__ == '__main__':
  logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
  # validate DFO and DFOC db
  # FO and FOC has different set of columns
  for db in ['DFO', 'DFOC']:
    print 'working on', db
    engine = sa.create_engine('sqlite:///./data/%s/db.db' % db)
    for comm in ['GOLD', 'SILVER', 'WHEAT-SRW - CHICAGO BOARD OF TRADE']:
      print 'working on', comm
      retrieve_commodity_data(engine, comm)
