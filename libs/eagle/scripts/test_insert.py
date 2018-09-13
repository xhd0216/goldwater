from upload import insert_to_table_2

def test_1():
  data_path = './f_disagg.txt'
  fields_path = './data/DFO/fields'
  db_path = './data/DFO/db.db'

  insert_to_table_2(data_path, fields_path, db_path, header_on=False)
  
