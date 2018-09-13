import os
import sqlalchemy as sa

TABLE_NAME='data_table'
DATABASE_NAME='db.db'

def insert_to_table(path):
  txt = [f for f in os.listdir(path) if f[-4:].lower() == '.txt']
  assert len(txt) == 1
  data_path = os.path.join(path, txt[0])

  up_dir = os.path.join(path, '../')
  fields_path = os.path.join(up_dir, 'fields')
  db_path = os.path.join(up_dir, 'db.db')
  insert_to_table_2(data_path, field_path, db_path, header_on=True)


def insert_to_table_2(data_path, fields_path, db_path, header_on):
  # get column name:type
  res = get_columns(fields_path)
  ret = []
  with open(data_path, 'r') as d:
    lines = d.readlines()
  if header_on:
    lines = lines[1:]

  # get all rows
  for line in lines:
    row = {}
    data = split(line, ',')
    assert len(data) == len(res)
    for i in range(len(data)):
      try:
        if res[i][1] == sa.Integer:
          if data[i] == '.':
            dt = 0
          else:
            dt = int(data[i])
        elif res[i][1] == sa.Float:
          if data[i] == '.':
            dt = 0
          else:
            dt = float(data[i])
        elif res[i][1] == sa.String(60) or res[i][1] == sa.String(20):
          dt = data[i]
        else:
          #raise ValueError('unknow type %s', res[i][1])
          dt = data[i]
      except Exception as e:
        print i, line, data, res, path
        raise e
      row[res[i][0]] = dt
    ret.append(row)
  engine = sa.create_engine('sqlite:///' + db_path)
  meta = sa.MetaData(engine, reflect=True)
  table = meta.tables[TABLE_NAME]
  conn = engine.connect()
  try:
    conn.execute(table.insert().prefix_with("OR REPLACE"), ret)
  except Exception as e:
    print e
  conn.close()
  print "done:", data_path


def create_db_file(path, db_name=DATABASE_NAME):
  db_file = os.path.join(path, db_name)
  engine = sa.create_engine('sqlite:///' + db_file)
  meta = sa.MetaData(engine)
  if not engine.dialect.has_table(engine, TABLE_NAME):
    table = create_table_schema(meta, TABLE_NAME, os.path.join(path, 'fields'))
    table.create()


def create_table_schema(meta, name, path):
  cols = create_table_columns(path)
  return sa.Table(name, meta, *cols) 


def create_table_columns(path):
  """ create columns give path to 'fields' file """
  res = get_columns(path)
  ret = []
  for (nam, typ) in res:
    if ('market' in nam and 'names' in nam) or 'yyyy_mm_dd' in nam:
      ret.append(sa.Column(nam, typ, primary_key=True))
    else:
      ret.append(sa.Column(nam, typ))
  return ret


def get_columns(path):
  ret = []
  with open(path, 'r') as f:
    for field in f.readlines():
      line = field.split(':')
      nam = line[0]
      typ = line[1].strip()
      if typ == 'int':
        typ = sa.Integer
      elif typ == 'str':
        typ = sa.String(60)
      elif typ == 'date':
        typ = sa.String(20)
      elif typ == 'float':
        typ = sa.Float
      else:
        raise ValueError('unknown type %s', typ)
      ret.append((nam, typ))
  return ret


def get_columns_type(path):
  """ return an array of types """
  ret = []
  with open(path, 'r') as f:
    for field in f.readlines()[1:]:
      ret.append(field.split(':')[1])
  return ret


def find_next_sep(line, start, sep=','):
  """ find next comma, disregard sep between quotes (")  """
  """ e.g., line='abc , "5,000"', start=4 should return next sep length (end of line) """
  quotes = set(['"'])
  index = start + 1
  stack = []
  length = len(line)
  while index <= length:
    if index == length:
      if len(stack) == 0:
        # reach the end
        return length
      else:
        raise ValueError('unmatched quotes')
    char = line[index]
    if char in quotes:
      if len(stack) == 0 or stack[-1] != char:
        stack.append(char)
      else:
        stack.pop()
    elif char == sep:
      if len(stack) == 0:
        return index
    index += 1


def split(line, sep=','):
  """ like str.split """
  """ assumption: the line does not end with sep """
  start = -1
  length = len(line)
  ret = []
  while start != length:
    index = find_next_sep(line, start)
    token = line[start+1: index].strip().replace('"', '')
    ret.append(token)
    start = index
  return ret


def get_headers(header_line):
  """ get fields of table """
  return [h.replace('"', '').strip() for h in header_line.split(',')]


def write_headers(table_name, headers, path):
  """ get a dict of (name, type) """
  with open(os.path.join(path, table_name+'_headers.txt'), 'w') as f:
    for header in headers:
      f.write(header + '\n')
