#!/usr/bin/env python
import os,sys,argparse,subprocess,logging,traceback,datetime

import ChefUtil
from RunFileUtil import getRunList
from RunFileUtil import RunFileGroups,RunFile

# WARNING:  requires hipo-utils to be in $COATJAVA/bin else $PATH
# FIXME:  check for hipo-utils and report error if can't find it

logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[%(name)-14s] %(message)s')
logger=logging.getLogger(__name__)

cli=argparse.ArgumentParser(description='Merge a directory of HIPO files by run number.')
cli.add_argument('-i',metavar='string',help='input files specification (directory, glob, file containing list of files), repeatable',type=str,default=[],action='append',required=True)
cli.add_argument('-o',metavar='path',help='output prefix (automatically suffixed with "_run#.hipo")',type=str,required=True)
cli.add_argument('-a',help='allow and skip pre-existing outputs',default=False,action='store_true')
cli.add_argument('-A',help='allow and delete pre-existing outputs',default=False,action='store_true')
cli.add_argument('-d',help='dry run',default=False,action='store_true')
args=cli.parse_args(sys.argv[1:])

# collect the input runs/files: 
rfgs=RunFileGroups()
rfgs.addRuns(getRunList(args.i))
rfgs.findFiles(args.i)

ChefUtil.mkdir(os.path.dirname(args.o))

# if not allowing pre-existing outputs, abort ASAP if any exist:
if not args.a:
  for rfg in rfgs.getGroups():
    rf=RunFile(rfg[0])
    out=args.o+'_%.6d.hipo'%rf.runNumber
    if os.path.exists(out):
      if args.A:
        logger.warning('Removing pre-existing output file:  '+out)
        os.remove(out)
      else:
        logger.error('File already exists:  '+out)
        sys.exit(1)

if len(rfgs.getGroups())==0:
  logger.error('Found no runs')
else:
  logger.info('Merging %d runs ...',len(rfgs.getGroups()))

outFiles=[]

# run `hipo-utils -merge` once per run number:
for rfg in rfgs.getGroups():
  rf=RunFile(rfg[0])
  out=args.o+'_%.6d.hipo'%rf.runNumber
  if args.a and os.path.exists(out):
    logger.warning('Skipping pre-existing output file:  '+out)
    continue
  hu='hipo-utils'
  if os.getenv('COATJAVA') is not None:
    hu=os.getenv('COATJAVA')+'/bin/hipo-utils'
  cmd=[hu,'-merge','-o',out]
  cmd.extend(rfg)
  outFiles.append(out)
  print((datetime.datetime.now()))
  print((' '.join(cmd)))
  if args.d:
    continue
  try:
    process=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,universal_newlines=True)
    for line in iter(process.stdout.readline, ''):
      if len(line.strip())>0:
        print((line.rstrip()))
    process.wait()
    if process.returncode!=0 or ChefUtil.hipoIntegrityCheck(out)!=0:
      for o in outFiles: os.remove(o)
      logger.critical(('Integrity check failure'+out))
      sys.exit(process.returncode)
  except:
    print((traceback.format_exc()))
    if os.path.isfile(out):
      os.remove(out)
    sys.exit(1)

