#!/usr/bin/env python
import sys,subprocess,argparse,logging
from SwifStatus import getWorkflowNames,deleteWorkflow
from CLAS12SwifStatus import CLAS12SwifStatus

logging.basicConfig(level=logging.WARNING,format='%(levelname)-9s[%(name)-15s] %(message)s')
logger=logging.getLogger(__name__)

# TODO: switch to JSON format (didn't know it was available at the time)

def processWorkflow(workflow,args):

  status = CLAS12SwifStatus(workflow,args)

#  if args.joblogs:
#    if status.isComplete() and status.isPreviousComplete():
#      status.moveJobLogs()

  # print details of jobs with problems:
  if args.problems:
    status.showPersistentProblems()

  # if retrying, only print status if problems exist:
  if args.retry:
    result = status.retryProblems()
    if len(result)>0 and not args.quiet:
      print status.getPrettyStatus()
      print result

  # otherwise always print status:
  else:
    print status.getPrettyStatus()
    if args.details:
      print status.getPrettyJsonDetails()

  # save job status in text file
  if args.save:
    if status.isComplete():
      if status.isPreviousComplete():
        return
      print 'WORKFLOW FINISHED:  '+workflow
    status.saveStatus()
    status.saveLog()
    if args.details:
      status.saveDetails()

  if args.clas12mon:
    status.saveDatabase()

if __name__ == '__main__':

  df='\n(default=%(default)s)'

  cli = argparse.ArgumentParser()
  cli.add_argument('--list',    help='list workflows',    action='store_true',default=False)
  cli.add_argument('--retry',   help='retry problem jobs',action='store_true',default=False)
  cli.add_argument('--save',    help='save to logs',      action='store_true',default=False)
  cli.add_argument('--details', help='show job details',  action='store_true',default=False)
  cli.add_argument('--problems',help='show problem jobs', action='store_true',default=False)
  cli.add_argument('--quiet',   help='do not print retries', action='store_true',default=False)
#  cli.add_argument('--joblogs', help='move job logs when complete', action='store_true',default=False)
  cli.add_argument('--workflow',help='workflow name (else all workflows)', action='append',default=[])
  cli.add_argument('--logdir',  help='local log directory'+df, type=str,default=None)
#  cli.add_argument('--publish', help='rsync to www dir',  action='store_true',default=False)
#  cli.add_argument('--webdir',  help='rsync target dir'+df,    type=str,default=None)
#  cli.add_argument('--webhost', help='rsync target host'+df,   type=str,default='jlabl5')
  cli.add_argument('--clas12mon',help='write to clas12mon db',action='store_true',default=False)
  cli.add_argument('--delete',  help='delete workflow',   action='store_true',default=False)

  args = cli.parse_args()

  if args.save and not args.logdir:
    sys.exit('ERROR:  must define --logdir if using the --save option')

#  if args.publish and not args.webdir:
#    sys.exit('ERROR:  must define --webdir if using the --publish option')

  if len(args.workflow)==0:
    args.workflow=getWorkflowNames()

  if args.list:
    print '\n'.join(args.workflow)

  else:
    for workflow in args.workflow:
      if args.delete:
        deleteWorkflow(workflow)
      else:
        processWorkflow(workflow,args)
#        if args.save and args.publish:
#          rsyncCmd=['rsync','-avz',args.logdir+'/',args.webhost+':'+args.webdir]
#          subprocess.check_output(rsyncCmd)

