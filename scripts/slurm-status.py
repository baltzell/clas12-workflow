#!/usr/bin/env python
import sys,pwd,argparse,datetime

from SlurmStatus import SlurmQuery

cli=argparse.ArgumentParser(description='Query SLURM job status.')
cli.add_argument('users',metavar='user',help='username', type=str, nargs='+')
cli.add_argument('--days',metavar='#',help='Number of days to span', type=int, default=7)
cli.add_argument('--end',metavar='YYYY-MM-DD',help='End date of query span (at 24:00)', type=str, default=None)
args=cli.parse_args(sys.argv[1:])

if args.end is not None:
  try:
    args.end=datetime.datetime.strptime(args.end,'%Y-%m-%d')
    args.end+=datetime.timedelta(days=1)
  except:
    cli.error('Invalid date format:  '+args.end)

for user in args.users:
  try:
    pwd.getpwnam(user)
  except:
    cli.error('User does not exist:  '+user)
  sq=SlurmQuery(user)
  sq.setDayDelta(args.days)
  sq.setDayEnd(args.end)
  print sq.getTable()

