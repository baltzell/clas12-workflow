#!/usr/bin/env python
import os,sys,argparse,subprocess,logging

from RunFileUtil import getRunList
from RunFileUtil import RunFileGroups,RunFile

# WARNING:  requires hipo-utils to be in $PATH

logging.basicConfig(level=logging.ERROR,format='%(levelname)-9s[%(name)-14s] %(message)s')
logger=logging.getLogger(__name__)

cli=argparse.ArgumentParser(description='Merge a directory of HIPO files by run number.')
cli.add_argument('-i',metavar='string',help='input files specification (directory, glob, file containing list of files), repeatable',type=str,default=[],action='append',required=True)
cli.add_argument('-o',metavar='path',help='output prefix (automatically suffixed with "_run#.hipo")',type=str,required=True)
args=cli.parse_args(sys.argv[1:])

rfgs=RunFileGroups()
rfgs.addRuns(getRunList(args.i))
rfgs.findFiles(args.i)

for rfg in rfgs.getGroups():
  rf=RunFile(rfg[0])
  out=args.o+'_%.5d.hipo'%rf.runNumber
  if os.path.exists(out):
    sys.exit('File already exists:  '+out)

for rfg in rfgs.getGroups():
  rf=RunFile(rfg[0])
  out=args.o+'_%.5d.hipo'%rf.runNumber
  cmd=['hipo-utils','-merge','-o',out]
  cmd.extend(rfg)
  try:
    print(subprocess.check_output(cmd))
  except:
    if os.path.isfile(out):
      os.remove(out)
    sys.exit(1)

