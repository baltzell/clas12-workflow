#!/usr/bin/env python

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
for run in range(args.rmin,args.rmax+1):
  rm.load(run)

if args.j:
  print(rm)
else:
  print((rm.csv()))

