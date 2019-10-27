#!/usr/bin/env python
import sys,pwd,grp,argparse,datetime

from SlurmStatus import SlurmQuery

project_groups={'clas12':'clas12','clas':'clas','hps':'hps'}

cli=argparse.ArgumentParser(description='Query SLURM job status.')
cli.add_argument('-u',metavar='user',help='username (repeatable)', type=str, default=[], action='append')
cli.add_argument('-p',metavar='project',help='project name (e.g. clas/clas12/hps)', type=str, default=None, choices=project_groups.keys())
cli.add_argument('-d',metavar='#',help='Number of days to span', type=int, default=7)
cli.add_argument('-e',metavar='YYYY-MM-DD',help='End date of query span (at 24:00)', type=str, default=None)
cli.add_argument('-M',metavar='string',help='match all in job names (repeatable)', type=str, default=[], action='append')
cli.add_argument('-m',metavar='string',help='match any in job names (repeatable)', type=str, default=[], action='append')
args=cli.parse_args(sys.argv[1:])

if len(args.u)==0:
  if args.p is None:
    cli.error('At least one of --user or --project must be specified.')
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
    args.e+=datetime.timedelta(days=1)
  except:
    cli.error('Invalid date format:  '+args.e)

for user in args.u:
  try:
    pwd.getpwnam(user)
  except:
    pass
  sq=SlurmQuery(user,args.p)
  sq.matchAny=args.m
  sq.matchAll=args.M
  sq.setDayDelta(args.d)
  if args.e is not None:
    sq.setDayEnd(args.e)
  sq.showTable()

