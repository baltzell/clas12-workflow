#!/usr/bin/env python
import sys,pwd,grp,argparse,datetime,getpass
import ROOT
from SlurmStatus import SlurmStatus
from SlurmStatus import SlurmQuery

project_groups={'clas12':'clas12','clas':'clas','hps':'hps'}

cli=argparse.ArgumentParser(description='Query SLURM job status archive.')
cli.add_argument('-u',metavar='user',help='username (repeatable), default is current user unless project is defined', type=str, default=[], action='append')
cli.add_argument('-p',metavar='project',help='project name (e.g. clas/clas12/hps/hallb-pro)', type=str, default=None, choices=list(project_groups.keys()))
cli.add_argument('-d',metavar='#',help='number of days to span (default=7)', type=int, default=7)
cli.add_argument('-e',metavar='YYYY-MM-DD',help='end date of query span, at 24:00 (default=today)', type=str, default=None)
cli.add_argument('-M',metavar='string',help='match all in job names (repeatable)', type=str, default=[], action='append')
cli.add_argument('-m',metavar='string',help='match any in job names (repeatable)', type=str, default=[], action='append')
cli.add_argument('-s',metavar='string',help='match job state (repeatable)', type=str, default=[], action='append', choices=SlurmStatus._STATES)
cli.add_argument('-w',metavar='#.#',help='minimum wall time in hours',type=float,default=None)
cli.add_argument('-W',metavar='#.#',help='maximum wall time in hours',type=float,default=None)
cli.add_argument('-g',help='generate plots',default=False,action='store_true')

args=cli.parse_args(sys.argv[1:])

if len(args.u)==0:
  if args.p is None:
    args.u=[getpass.getuser()]
  else:
    # no user defined, get all users in the project's group:
    if args.p not in project_groups:
      cli.error('Unknown project:  '+args.p)
    group=project_groups[args.p]
    gid=grp.getgrnam(group).gr_gid
    args.u.extend(grp.getgrgid(gid).gr_mem)

if args.e is not None:
  try:
    args.e=datetime.datetime.strptime(args.e,'%Y-%m-%d')
    # move to 00:00:00 the following day:
    args.e+=datetime.timedelta(days=1)
  except:
    cli.error('Invalid date format:  '+args.e)

canvases=[]

for user in args.u:
  try:
    pwd.getpwnam(user)
  except:
    print(('Unknown user: '+user))
    continue
  sq=SlurmQuery(user,args.p)
  sq.matchAny=args.m
  sq.matchAll=args.M
  sq.minimumWallHours=args.w
  sq.maximumWallHours=args.W
  sq.setDayDelta(args.d)
  if args.e is not None:
    sq.setDayEnd(args.e)
  if len(args.s)>0:
    sq.states=args.s
  print(sq)
  if args.g:
    canvases.append(sq.getCanvas('cputime'))
    canvases.append(sq.getCanvas('walltime'))
    canvases.append(sq.getCanvas('cputime/walltime'))

if args.g:
  print('Done.  Press any key to close.')
  input()

