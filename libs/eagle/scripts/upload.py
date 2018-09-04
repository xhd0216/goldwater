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
  """ like split """
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
