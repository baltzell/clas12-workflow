#!/usr/bin/env python
import re,sys,subprocess,argparse,logging
from SwifStatus import getWorkflowNames,deleteWorkflow,SWIF_PROBLEMS
from CLAS12SwifStatus import CLAS12SwifStatus,getHeader
from Matcher import matchAny

logging.basicConfig(level=logging.WARNING,format='%(levelname)-9s[%(name)-15s] %(message)s')
logger=logging.getLogger(__name__)

# TODO: switch to JSON format (didn't know it was available at the time)

PROBLEMS=SWIF_PROBLEMS[:]
PROBLEMS.append('ANY')

def processWorkflow(workflow,args):

  status = CLAS12SwifStatus(workflow,args)

#  if args.input:
#    status.getStatus(args.input)

#  if args.joblogs:
#    if status.isComplete() and status.isPreviousComplete():
#      status.moveJobLogs()

  if args.missing:
    print('\nMissing outputs in '+workflow+':')
    print(('\n'.join(status.findMissingOutputs())))
    return

  if args.stats or args.runstats:
    print('\nCompletion status summary for '+workflow+':')
    if args.stats:
      print(status.summarize('mode')),
    if args.runstats:
      print(status.summarize('run')),
    return

  if args.problemstats or args.problemnodes:
    print('\nProblem summary for '+workflow+':')
    print(status.summarizeProblems(args.problemnodes))
    return

  if len(args.listrun)>0:
    print('\nJobs associated with run numbers: '+','.join([str(x) for x in args.listrun]))
    print('\n'.join(status.getJobNamesByRun(args.listrun)))
    return

  if len(args.abandonrun)>0:
    print(status.abandonJobsByRun(args.abandonrun))
    return

  # print details of jobs with problems:
  if args.problems:
    print(status.getPersistentProblemJobs(args.problems))
    return

  # if retrying or abandoning, only print status if problems exist:
  if len(args.abandon)>0 or args.retry or len(args.clas12mon)>0:

    if len(args.abandon)>0:
      res = status.abandonProblems(args.abandon)
      if len(res)>0 and not args.quiet:
        print(status.getPrettyStatus())
        print(res)

    if args.retry:
      sunzinps=[]
      if 'SWIF-USER-NON-ZERO' in status.getProblems():
        sunzinps=status.getPersistentProblemInputs('SWIF-USER-NON-ZERO')
      res = status.retryProblems()
      if len(sunzinps)>0:
        print('\n\nSWIF-USER-NON-ZERO Inputs:\n'+'\n'.join(sunzinps))
      if len(res)>0 and not args.quiet:
        print(status.getPrettyStatus())
        print(res)

  # otherwise always print status:
  else:
    print((status.getPrettyStatus()+'\n'))
    if args.details:
      print(status.getPrettyJsonDetails())

  # save job status in text file
  if args.save:
    if status.isComplete():
      if status.isPreviousComplete():
        return
      print('WORKFLOW FINISHED:  '+workflow)
    status.saveStatus()
    status.saveLog()
    if args.details:
      status.saveDetails()

  if len(args.clas12mon)>0 and matchAny(getHeader(workflow)['tag'],args.clas12mon):
    status.saveDatabase()

if __name__ == '__main__':

  df='\n(default=%(default)s)'

  cli = argparse.ArgumentParser('Do SWIF stuff.',epilog='Valid PROBLEMs: '+','.join(PROBLEMS))
  cli.add_argument('--list',    help='list workflows',    action='store_true',default=False)
  cli.add_argument('--retry',   help='retry problem jobs',action='store_true',default=False)
  cli.add_argument('--save',    help='save to logs',      action='store_true',default=False)
  cli.add_argument('--details', help='show job details',  action='store_true',default=False)
  cli.add_argument('--quiet',   help='do not print retries', action='store_true',default=False)
#  cli.add_argument('--joblogs', help='move job logs when complete', action='store_true',default=False)
  cli.add_argument('--logdir',  metavar='PATH',help='local log directory'+df, type=str,default=None)
#  cli.add_argument('--publish', help='rsync to www dir',  action='store_true',default=False)
#  cli.add_argument('--webdir',  help='rsync target dir'+df,    type=str,default=None)
#  cli.add_argument('--webhost', help='rsync target host'+df,   type=str,default='jlabl5')
  cli.add_argument('--clas12mon',metavar='TAG',help='write matching workflows to clas12mon (repeatable)',type=str,default=[],action='append')
  cli.add_argument('--delete',   help='delete workflow',   action='store_true',default=False)
  cli.add_argument('--workflow', metavar='NAME',help='workflow name (or regex) else all workflows', action='append',default=[])
  cli.add_argument('--missing',  help='find missing output files', action='store_true',default=False)
  cli.add_argument('--stats',    help='show completion status of each workflow component', action='store_true',default=False)
  cli.add_argument('--runstats', help='show completion status of each run number', action='store_true',default=False)
  cli.add_argument('--problemstats', help='show summary of all problems that have occured during the workflow', default=False,action='store_true')
  cli.add_argument('--problemnodes', help='show summary of all problems per node that have occured during the workflow', default=False,action='store_true')
  cli.add_argument('--listrun',   metavar='#', help='list all job names associated with particular run numbers', action='append', default=[], type=int)
  cli.add_argument('--abandonrun', metavar='#', help='abandon all jobs associated with particular run numbers', action='append', default=[], type=int)
  cli.add_argument('--abandon',  help='abandon problem jobs (repeatable)',  metavar='PROBLEM', action='append',default=[],choices=PROBLEMS)
  cli.add_argument('--problems',help='show jobs whose most recent attempt was problematic', metavar='PROBLEM',nargs='?',const='ANY',default=False,choices=PROBLEMS)
#  cli.add_argument('--input',    help='read workflow status from JSON file instead of querying SWIF', default=None,type=str)

  args = cli.parse_args()

  if args.save and not args.logdir:
    cli.error('Must define --logdir if using the --save option')

  workflows=[]
  if len(args.workflow)==0:
    workflows.extend(getWorkflowNames())
    if args.delete and 'YES' != raw_input('Really delete all workflows?  If so, type "YES" and press return ...'):
        sys.exit('Aborted.')
#  elif args.input:
#    workflow.append(args.input.replace('.json'))
  else:
    for x in getWorkflowNames():
      for y in args.workflow:
        if re.match('^'+y+'$',x) is not None:
          workflows.append(x)

#  if args.publish and not args.webdir:
#    sys.exit('ERROR:  must define --webdir if using the --publish option')

  if args.list:
    print('\n'.join(workflows))

  else:
    for workflow in workflows:
      if args.delete:
        deleteWorkflow(workflow)
      else:
        processWorkflow(workflow,args)
#        if args.save and args.publish:
#          rsyncCmd=['rsync','-avz',args.logdir+'/',args.webhost+':'+args.webdir]
#          subprocess.check_output(rsyncCmd)

