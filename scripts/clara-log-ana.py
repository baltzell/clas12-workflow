#!/usr/bin/env python

import os,sys,argparse

from ClaraLog import ClaraLog
from ClaraStats import ClaraStats
import Matcher
import LogFinder

cli=argparse.ArgumentParser(description='Collect job statistics from CLARA logs.')
cli.add_argument('-i',help='draw histos interactively',default=False,action='store_true')
cli.add_argument('-r',help='reload the log caches',default=False,action='store_true')
cli.add_argument('-o',metavar='rootfile',help='output ROOT file name',type=str,default=None)
cli.add_argument('-n',metavar='#',help='maximum number of log files',type=int,default=0)
cli.add_argument('-m',metavar='string',help='match all in filenames',type=str,default=[],action='append')
cli.add_argument('-M',metavar='string',help='match any in filenames',type=str,default=[''],action='append')
cli.add_argument('-t',metavar='title',help='title for plot',type=str,default=None)
cli.add_argument('-f',metavar='flavor',help='choose specific flavors',type=str,default=[],action='append')
cli.add_argument('p',metavar='path',nargs='*')
args=cli.parse_args(sys.argv[1:])

if args.r:
  LogFinder.RECACHE=True

# generate list of logfiles:
logfiles=[]
for path in args.p:
  if os.path.isfile(path):
    if path.endswith('orch.log') or path.endswith('.out'):
      basename=path
      if path.find('/')>=0:
        basename=path.split('/').pop()
      if Matcher.matchAll(basename,args.m) and Matcher.matchAny(basename,args.M):
        logfiles.append(path)
  elif os.path.isdir(path):
    for d,x,files in os.walk(path):
      for f in files:
        if f.endswith('orch.log') or f.endswith('.out'):
          if Matcher.matchAll(f,args.m) and Matcher.matchAny(f,args.M):
            logfiles.append(d+'/'+f)

if len(logfiles)==0:
  sys.exit('ERROR:  Found no valid log files.  Check path.')

cs=ClaraStats()
cs.title=args.t
if len(args.f)>0:
  cs.setFlavors(args.f)

# analyze the logfiles:
for logfile in logfiles:
  clog=ClaraLog(logfile)
  cs.fill(clog,clog.t1)
  # update the plot:
  if args.i and clog.isComplete() and cs.successes%10==0:
    cs.draw()
    print cs
  # printout unknown errors:
  if clog.errors.getBit('UDF'):
    print 'UDF:  ',logfile,str(clog.lastline)
  #if clog.errors.getBit('TRUNC'):
    #print logfile.replace('_orch.log','_fe_dpe.log')
  # abort, we already got the requested statistics:
  if args.n>0 and s.successes>args.n:
    break

print cs

if args.o is not None:
  cs.save(args.o)
if args.i:
  cs.draw()
  print 'Done.  Press any key to close.'
  raw_input()

