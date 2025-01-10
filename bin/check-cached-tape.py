#!/usr/bin/env python3
import os
import sys
import logging
import argparse

import JLabTape

logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[ %(name)-15s ] %(message)s')

cli = argparse.ArgumentParser('Compare /cache files with their /mss counterparts.')
cli.add_argument('--crc32',help='compare crc32 checksums (slow)',default=False,action='store_true')
cli.add_argument('--md5',help='compare md5 checksums (slow)',default=False,action='store_true')
cli.add_argument('path', nargs='+',help='path of directory or file on /cache (repeatable)')
args = cli.parse_args(sys.argv[1:])

def check_cache_file(cache_path):
  if not os.path.isfile('/mss'+cache_path[6:]):
    return True
  cache = JLabTape.CachedFile(cache_path, crc32=args.crc32, md5=args.md5)
  tape = JLabTape.TapeStub('/mss'+cache_path[6:])
  return cache == tape

mismatch=[]

for x in args.path:
  if not x.startswith('/cache/'):
    print('Ignoring non-/cache path: '+x)
    continue
  if os.path.isfile(x):
    if not check_cache_file(x):
      mismatch.append(x)
  elif os.path.isdir(x):
    for adir,subdirs,files in os.walk(x):
      for f in files:
        if not check_cache_file(adir+'/'+f):
          mismatch.append(adir+'/'+f)
  else:
    print('Ignoring non-existent path:  '+x)
    continue

if len(mismatch) == 0:
  print('No Mismatches Found.')
else:
  print(str(len(mismatch))+' Mismatches Found:\n'+'\n'.join(mismatch))

