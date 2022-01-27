import os,gzip

def head(path, max_lines=0):
  if path.endswith('.gz'):
    f = gzip.open(path, errors='replace')
  else:
    f = open(path)#, errors='replace')
  if f is not None:
    n_lines = 0
    for line in f.readlines():
      if max_lines > 0 and n_lines > max_lines:
        break
      n_lines += 1
      yield line.strip()
    f.close()

def tail(path, max_lines=0):
  if path.endswith('.gz'):
    f = gzip.open(path, errors='replace')
  else:
    f = open(path)#, errors='replace')
  if f is not None:
    n_lines = 0
    f.seek(0, os.SEEK_END)
    position = f.tell()
    line = ''
    while position >= 0:
      if max_lines > 0 and n_lines > max_lines:
        break
      f.seek(position)
      next_char = f.read(1)
      if next_char == "\n":
         n_lines += 1
         yield line[::-1]
         line = ''
      else:
         line += next_char
      position -= 1
    yield line[::-1]
    f.close()

