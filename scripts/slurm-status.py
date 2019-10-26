#!/usr/bin/env python
import sys,argparse

from SlurmStatus import SlurmQuery

cli=argparse.ArgumentParser(description='Query SLURM job status.')
cli.add_argument('--user',metavar='NAME',help='user name', type=str, required=True)
cli.add_argument('--days',metavar='#',help='# days to span', type=int, default=7)
cli.add_argument('--end',metavar='DATE',help='end date of query span', type=str, default=None)
args=cli.parse_args(sys.argv[1:])

sq=SlurmQuery(args.user)
print sq.getTable()

