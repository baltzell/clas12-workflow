#!/usr/bin/env python3

import os,sys,argparse

from ClaraLog import ClaraLog
from ClaraStats import ClaraStats
import Matcher
import LogFinder

cli=argparse.ArgumentParser(description='Collect job statistics from CLARA logs.',epilog='Note, to pass argument values that start with a dash, use the "=" syntax, e.g. "clara-log-ana.py -m=-123-".')
cli.add_argument('-i',help='draw histos interactively',default=False,action='store_true')
cli.add_argument('-r',help='reload the log caches',default=False,action='store_true')
cli.add_argument('-v',help='verbose mode',default=False,action='store_true')
cli.add_argument('-o',metavar='output prefix',help='output ROOT file name',type=str,default=None)
cli.add_argument('-n',metavar='#',help='maximum number of log files',type=int,default=0)
cli.add_argument('-M',metavar='string',help='match all in filenames',type=str,default=[],action='append')
cli.add_argument('-m',metavar='string',help='match any in filenames',type=str,default=[],action='append')
cli.add_argument('-t',metavar='title',help='title for plot',type=str,default=None)
cli.add_argument('-f',metavar='flavor',help='choose specific flavors',type=str,default=[],action='append')
cli.add_argument('p',metavar='path',nargs='*')
args=cli.parse_args(sys.argv[1:])

if args.r:
  LogFinder.RECACHE=True

if args.i and os.environ.get('DISPLAY') is None:
  cli.error('DISPLAY not set.  Did you enable X-forwarding in your ssh connection?')

# generate list of logfiles:
logfiles=[]
for path in args.p:
  if os.path.isfile(path):
    if path.endswith('orch.log') or path.endswith('.out') or path.endswith('orch.log.gz') or path.endswith('.out.gz'):
      basename=path
      if path.find('/')>=0:
        basename=path.split('/').pop()
      if Matcher.matchAll(basename,args.M) and Matcher.matchAny(basename,args.m):
        logfiles.append(path)
  elif os.path.isdir(path):
    for d,x,files in os.walk(path):
      for f in files:
        if f.endswith('orch.log') or f.endswith('.out') or f.endswith('orch.log.gz') or f.endswith('.out.gz'):
          if Matcher.matchAll(f,args.M) and Matcher.matchAny(f,args.m):
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
  if args.i and clog.isComplete():
    if cs.successes==1 or cs.successes%10==0:
      cs.draw()
      if args.v:
        print(cs)
  # printout unknown errors:
  if args.v and clog.errors.getBit('UDF'):
    print('UDF:    ',logfile,'\n',str(clog.lastline))
  if args.v and clog.errors.getBit('TRUNC'):
    print('TRUNC::::::::::::::::::::::::  ')
    print(logfile,'\n',str(clog.lastline),'\n',clog.slurmlog)
  # abort, we already got the requested statistics:
  if args.n>0 and cs.successes>args.n:
    break

print(cs)

if args.o is not None:
  cs.save(args.o)
if args.i:
  cs.draw()
  print('Done.  Press any key to close.')
  input()

