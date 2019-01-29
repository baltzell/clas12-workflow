#!/usr/bin/env python
import sys
import subprocess
import argparse
from SwifStatus import SWIF
from CLAS12SwifStatus import CLAS12SwifStatus

def getWorkflowNames():
  workflows=[]
  for line in subprocess.check_output([SWIF,'list']).splitlines():
    line=line.strip()
    if line.find('workflow_name')==0:
      workflows.append(line.split('=')[1].strip())
  return workflows

def processWorkflow(workflow,args):

  status = CLAS12SwifStatus(workflow,args)
  status.mergeTags()

  if args.joblogs and status.isComplete() and status.isPreviousComplete():
    status.moveJobLogs()

  if args.retry:
    result = status.retryProblems()
    if len(result)>0:
      print status.getPrettyStatus()
      print result

  if args.save:
    if status.isComplete():
      if status.isPreviousComplete():
        return
      print 'WORKFLOW FINISHED:  '+workflow
    status.saveStatus()
    status.saveLog()
    if args.details:
      status.saveDetails()

  else:
    print status.getPrettyStatus()
    if args.details:
      print status.getPrettyJsonDetails()
    if status.isComplete():
      print 'WORKFLOW FINISHED:  '+workflow+'\n'

  if args.clas12mon:
    status.saveDatabase()

if __name__ == '__main__':

  df='\n(default=%(default)s)'

  cli = argparse.ArgumentParser()
  cli.add_argument('--list',    help='list workflows',    action='store_true',default=False)
  cli.add_argument('--save',    help='save to logs',      action='store_true',default=False)
  cli.add_argument('--retry',   help='retry problem jobs',action='store_true',default=False)
  cli.add_argument('--publish', help='rsync to www dir',  action='store_true',default=False)
  cli.add_argument('--details', help='show job details',  action='store_true',default=False)
  cli.add_argument('--joblogs', help='move job logs when complete', action='store_true',default=False)
  cli.add_argument('--workflow',help='workflow name',     action='append',default=[])
  cli.add_argument('--logdir',  help='local log directory'+df, type=str,default=None)
  cli.add_argument('--webdir',  help='rsync target dir'+df,    type=str,default=None)
  cli.add_argument('--webhost', help='rsync target host'+df,   type=str,default='jlabl5')
  cli.add_argument('--clas12mon',help='write to clas12mon db',action='store_true',default=False)

  args = cli.parse_args()

  if args.save and not args.logdir:
    sys.exit('ERROR:  must define --logdir if using the --save option')

  if args.publish and not args.webdir:
    sys.exit('ERROR:  must define --webdir if using the --publish option')

  if len(args.workflow)==0:
    args.workflow=getWorkflowNames()

  if args.list:
    print '\n'.join(args.workflow)

  else:
    for workflow in args.workflow:
      processWorkflow(workflow,args)
    if args.save and args.publish:
      rsyncCmd=['rsync','-avz',args.logdir+'/',args.webhost+':'+args.webdir]
      subprocess.check_output(rsyncCmd)

