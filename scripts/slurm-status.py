#!/usr/bin/env python
import sys,pwd,grp,argparse,datetime

from SlurmStatus import SlurmQuery
from Matcher import matchAll,matchAny

project_groups={'clas12':'clas12','clas':'clas','hps':'hps'}

cli=argparse.ArgumentParser(description='Query SLURM job status.')
cli.add_argument('--user',metavar='user',help='username (repeatable)', type=str, default=[], action='append')
cli.add_argument('--project',metavar='project',help='project name (e.g. clas/clas12/hps)', type=str, default=None, choices=project_groups.keys())
cli.add_argument('--days',metavar='#',help='Number of days to span', type=int, default=7)
cli.add_argument('--end',metavar='YYYY-MM-DD',help='End date of query span (at 24:00)', type=str, default=None)
args=cli.parse_args(sys.argv[1:])

if len(args.user)==0:
  if args.project is None:
    cli.error('At least one of --user or --project must be specified.')
  else:
    if args.project not in project_groups:
      cli.error('Unknown project:  '+args.project)
    group=project_groups[args.project]
    gid=grp.getgrnam(group).gr_gid
    args.user.extend(grp.getgrgid(gid).gr_mem)

if args.end is not None:
  try:
    args.end=datetime.datetime.strptime(args.end,'%Y-%m-%d')
    args.end+=datetime.timedelta(days=1)
  except:
    cli.error('Invalid date format:  '+args.end)

for user in args.user:
  try:
    pwd.getpwnam(user)
  except:
    pass
  sq=SlurmQuery(user,args.project)
  sq.setDayDelta(args.days)
  if args.end is not None:
    sq.setDayEnd(args.end)
  sq.showTable()

