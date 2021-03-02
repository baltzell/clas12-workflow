#!/usr/bin/env python3

import sys
import os
import zlib
import hashlib
import argparse

cli = argparse.ArgumentParser('Compare /cache files with their /mss counterparts.')
cli.add_argument('--crc32',help='compare crc32 checksums (slow)',default=False,action='store_true')
cli.add_argument('--md5',help='compare md5 checksums (slow)',default=False,action='store_true')
cli.add_argument('path', nargs='+',help='path of directory or file on /cache (repeatable)')
args = cli.parse_args(sys.argv[1:])

class TapeStub():
  def __init__(self,path):
    self.path = path
    with open(self.path,'r') as f:
      for line in f.readlines():
        key,val = line.strip().split('=')
        if key == 'size':
          self.size = int(val)
        elif key == 'md5':
          self.md5 = val
        elif key == 'crc32':
          self.crc32 = val

def crc32(path):
  ret = 0
  with open(path,'rb') as f:
    for line in f.readlines():
      ret = zlib.crc32(line, ret)
  return '%x'%ret

def md5(path):
  with open(path,'rb') as f:
    md5 = hashlib.md5()
    md5.update(f.read())
    return str(md5.hexdigest())

mismatch=[]

for x in args.path:
  if not x.startswith('/cache/'):
    print('Ignoring non-/cache path: '+x)
    continue
  if not os.path.isfile(x) and not os.path.isdir(x):
    print('Ignoring non-existiend path:  '+x)
    continue
  for adir,subdirs,files in os.walk(x):
    for f in files:
      cache = adir+'/'+f
      mss = '/mss'+cache[6:]
      stub = TapeStub(mss)
      if os.stat(cache).st_size != stub.size:
        mismatch.append(cache)
      elif os.stat(cache).st_size > 3e9:
        print('Bypassing checksum on large file:  '+cache)
      elif args.crc32 and stub.crc32 != crc32(cache):
        mismatch.append(cache)
      elif args.md5 and stub.md5 != md5(cache):
        mismatch.append(cache)

if len(mismatch) == 0:
  print('No Mismatches Found.')
else:
  print(str(len(mismatch))+' Mismatches Found:'+'\n'.join(mismatch))

