#!/usr/bin/env python
import sys,argparse,logging
from RunFileUtil import RunFileGroups
from RcdbManager import RcdbManager

logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[%(name)-14s] %(message)s')
logger=logging.getLogger(__name__)

cli=argparse.ArgumentParser(description='Generate a run list and check RCDB.')
cli.add_argument('runStart',metavar='#',help='first run number',type=int)
cli.add_argument('runEnd',metavar='#',help='last run number',type=int)
cli.add_argument('fileList',metavar='PATH',help='file containing list of files',type=str)
args=cli.parse_args(sys.argv[1:])

rcdb=RcdbManager()
rfgs=RunFileGroups()

for run in range(args.runStart,args.runEnd+1):
  rfgs.addRun(run)

for fileName in open(args.fileList,'r').readlines():
  rfgs.addFile(fileName)

for run,rfg in rfgs.rfgs.items():
  if rfg.size()>10:
    print((str(run)+' '+str(rfg.size())))

missing=[]
for run in rfgs.getRunList(10):
  try:
    t=rcdb.getTorusScale(run)
    s=rcdb.getSolenoidScale(run)
    r=rcdb.getRunStartTime(run)
  except AttributeError:
    missing.append(run)
if len(missing)>0:
  print(('Missing from RCDB: ',missing))


