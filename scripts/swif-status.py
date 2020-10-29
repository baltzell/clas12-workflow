#!/usr/bin/env python
import sys,subprocess,argparse,logging
import Matcher
from SwifStatus import getWorkflowNames,deleteWorkflow,SWIF_PROBLEMS
from CLAS12SwifStatus import CLAS12SwifStatus,getHeader

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

  if args.delete:
    deleteWorkflow(workflow)
    return

  if args.missing or args.missingTape:
    print('\nMissing outputs in '+workflow+':')
    print(('\n'.join(status.findMissingOutputs(args.missingTape))))
    return

  if args.stats or args.runStats:
    print('\nCompletion status summary for '+workflow+':')
    if args.stats:
      print(status.summarize('mode')),
    if args.runstats:
      print(status.summarize('run')),
    return

  if args.problemStats or args.problemNodes:
    print('\nProblem summary for '+workflow+':')
    print(status.summarizeProblems(args.problemNodes))
    return

  if len(args.listRun)>0:
    print('\nJobs associated with run numbers: '+','.join([str(x) for x in args.listRun]))
    print('\n'.join(status.getJobNamesByRun(args.listRun)))
    return

  if len(args.abandonRun)>0:
    print(status.abandonJobsByRun(args.abandonRun))
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
      if len(res)>0 and not args.quiet:
        print(status.getPrettyStatus())
        print('\n'+str(res))
      if len(sunzinps)>0 and len(sunzinps)<11:
        print('\nSWIF-USER-NON-ZERO Inputs:\n'+'\n'.join(sunzinps)+'\n')

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
#    if args.publish:
#      subprocess.check_output(['rsync','-avz',args.logdir+'/',args.webhost+':'+args.webdir])

  if len(args.clas12mon)>0 and Matcher.matchAny(getHeader(workflow)['tag'],args.clas12mon):
    status.saveDatabase()


if __name__ == '__main__':

  df='\n(default=%(default)s)'

  cli = argparse.ArgumentParser('Do SWIF stuff.',epilog='Valid PROBLEMs: '+','.join(PROBLEMS)+'\n\nNote, to pass argument values that start with a dash, use the "=" syntax, e.g. "swif-status.py -m=-123-".')
  cli.add_argument('--list',         help='list workflows',    action='store_true',default=False)
  cli.add_argument('--retry',        help='retry problem jobs',action='store_true',default=False)
  cli.add_argument('--save',         help='save to logs',      action='store_true',default=False)
  cli.add_argument('--details',      help='show job details',  action='store_true',default=False)
  cli.add_argument('--quiet',        help='do not print retries', action='store_true',default=False)
  cli.add_argument('--logdir',       help='local log directory'+df, metavar='PATH',type=str,default=None)
  cli.add_argument('--clas12mon',    help='write matching workflows to clas12mon (repeatable)',metavar='TAG',type=str,default=[],action='append')
  cli.add_argument('--delete',       help='delete workflow', action='store_true',default=False)
  cli.add_argument('--workflow',     help='workflow name (or regex) else all workflows', metavar='NAME', action='append',default=[])
  cli.add_argument('--missing',      help='find missing output files', action='store_true',default=False)
  cli.add_argument('--missingTape',  help='same as --missing, but assumeg /mss if they were originally written to /cache', action='store_true',default=False)
  cli.add_argument('--stats',        help='show completion status of each workflow component', action='store_true',default=False)
  cli.add_argument('--runStats',     help='show completion status of each run number', action='store_true',default=False)
  cli.add_argument('--problemStats', help='show summary of all problems that have occured during the workflow', default=False,action='store_true')
  cli.add_argument('--problemNodes', help='show summary of all problems per node that have occured during the workflow', default=False,action='store_true')
  cli.add_argument('--listRun',      help='list all job names associated with particular run numbers', metavar='#', action='append', default=[], type=int)
  cli.add_argument('--abandonRun',   help='abandon all jobs associated with particular run numbers', metavar='#', action='append', default=[], type=int)
  cli.add_argument('--abandon',      help='abandon problem jobs (repeatable)',  metavar='PROBLEM', action='append',default=[],choices=PROBLEMS)
  cli.add_argument('--problems',     help='show jobs whose most recent attempt was problematic', metavar='PROBLEM',nargs='?',const='ANY',default=False,choices=PROBLEMS)
  cli.add_argument('--matchAll',     help='match workflow names containing all of these substrings (repeatable)', metavar='string', type=str, default=[], action='append')
  cli.add_argument('--matchAny',     help='match workflow names containing any of these substrings (repeatable)', metavar='string', type=str, default=[], action='append')
#  cli.add_argument('--joblogs',    help='move job logs when complete', action='store_true',default=False)
#  cli.add_argument('--publish',    help='rsync to www dir', action='store_true',default=False)
#  cli.add_argument('--webdir',     help='rsync target dir'+df, type=str,default=None)
#  cli.add_argument('--webhost',    help='rsync target host'+df, type=str,default='jlabl5')
#  cli.add_argument('--input',      help='read workflow status from JSON file instead of querying SWIF', default=None,type=str)

  args = cli.parse_args()

#  if args.publish and not args.webdir:
#    sys.exit('ERROR:  must define --webdir if using the --publish option')

  if len(args.workflow)>0:
    if len(args.matchAll)>0 or len(args.matchAny)>0:
      cli.error('--workflow not supported in conjuction with either --matchAll or --matchAny')

  if args.save and not args.logdir:
    cli.error('Must define --logdir if using the --save option')

  # generate the list of workflows to process:
  workflows=[]
  for wf in getWorkflowNames():
    if len(args.workflow)==0:
      if Matcher.matchAll(wf,args.matchAll) and Matcher.matchAny(wf,args.matchAny):
        workflows.append(wf)
    elif wf in args.workflow:
      workflows.append(wf)

  # require user input before deleting workflows:
  if args.delete and len(workflows)>0:
    if 'YES' != raw_input('Really delete these workflows?\n'+'\n'.join(workflows)+'\nIf so, type "YES" and press return ...'):
      sys.exit('Aborted')

  if args.list:
    print('\n'.join(workflows))

  else:
    for workflow in workflows:
      processWorkflow(workflow,args)

