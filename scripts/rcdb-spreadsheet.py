#!/usr/bin/env python2

import sys,logging,argparse

from RcdbManager import RcdbManager

logging.basicConfig(level=logging.CRITICAL,format='%(levelname)-9s[%(name)-15s] %(message)s')
logger=logging.getLogger(__name__)

cli=argparse.ArgumentParser(description='Dump RCDB to a spreadsheet.')
cli.add_argument('-j',help='use JSON format (default=CSV)', default=False, action='store_true')
cli.add_argument('rmin',metavar='RUNMIN',help='minimum run number',type=int)
cli.add_argument('rmax',metavar='RUNMAX',help='maximum run number',type=int)

args=cli.parse_args(sys.argv[1:])

if args.rmax < args.rmin:
  cli.error('rmax cannot be less than rmin')

rm=RcdbManager()
run=args.rmin-1
first=False

while True:

  run = rm.db.get_next_run(run)

  rm.load(run.number)
  if not args.j:
    if first:
      print(rm.csvHeader())
      first=False
    print((rm.csvRun(run.number)))

if args.j:
  print(rm)

