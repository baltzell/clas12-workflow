#!/usr/bin/env python2
import os,re,sys,subprocess,argparse,logging
import Matcher
import SwifStatus
import CLAS12SwifStatus

logging.basicConfig(level=logging.WARNING,format='%(levelname)-9s[%(name)-15s] %(message)s')
logger=logging.getLogger(__name__)

def separator(label):
  width = 62
  label = '' if label is None else ' '+label.strip()+' '
  left = int((width - len(label))/2)
  right = width - left - len(label)
  return ('-'*left) + label + ('-'*right)

tally = SwifStatus.Stats()

def processWorkflow(workflow,args):

  status = CLAS12SwifStatus.CLAS12SwifStatus(workflow,args)

  tally.add(status.getSummaryStats('mode'))

#  if args.input:
#    status.getStatus(args.input)

#  if args.jobLogs:
#    if status.isComplete() and status.isPreviousComplete():
#      status.moveJobLogs()

  if args.delete:
    SwifStatus.deleteWorkflow(workflow)
    return

  if args.deleteComplete:
    if status.isComplete():
      SwifStatus.deleteWorkflow(workflow)
    return

  if args.listDirs:
    print(('\n'.join(status.getOutputDirs())))
    return

  if args.missing or args.missingTape:
    print('\nMissing outputs in '+workflow+':')
    print(('\n'.join(status.findMissingOutputs(args.missingTape))))
    return

  if args.stats or args.runStats or args.phaseStats:
    print('\n'+separator(workflow))
    if args.stats:
      print(status.getSummaryStats('mode')),
    if args.runStats:
      print(status.getSummaryStats('run')),
    if args.phaseStats:
      print(status.getSummaryStats('phase')),
    return

  if args.problemStats or args.problemNodes:
    print('\nProblem summary for '+workflow+':')
    print(status.summarizeProblems(pernode=True))
    return

  if len(args.listRun)>0:
    print('\nJobs associated with run numbers: '+','.join([str(x) for x in args.listRun]))
    print('\n'.join(status.getJobNamesByRun(args.listRun)))
    return

  if len(args.abandonRun)>0:
    print(status.abandonJobsByRun(args.abandonRun))
    return

  # print contents of logs from jobs problems:
  if args.problemLogs is not False or args.problemLogsTail is not False:
    if args.problemLogsTail > 0:
      status.tailPersistentProblemLogs(args.problemLogs,args.problemLogsTail)
    else:
      print('\n'.join(status.getPersistentProblemLogs(args.problemLogs)))
    return

  # print inputs of jobs with problems:
  if args.problemInputs:
    print(('\n'.join(status.getPersistentProblemInputs(args.problemInputs))))
    return

  # print details of jobs with problems:
  if args.problems:
    print(status.getPersistentProblemJobs(args.problems))
    return

  # if retrying or abandoning, only print status if problems exist:
  if len(args.abandon)>0 or len(args.retry)>0 or len(args.clas12mon)>0:

    if len(args.abandon)>0:
      res = status.abandonProblems(args.abandon)
      if len(res)>0 and not args.quiet:
        print(status.getPrettyStatus())
        print(res)

    if len(args.retry)>0:
      user_error_type = 'SLURM_FAILED'
      if user_error_type in args.retry:
        sunz_inputs=[]
        if user_error_type in status.getCurrentProblems():
          sunz_inputs = status.getPersistentProblemInputs(user_error_type)
      res = status.retryProblems(args.retry)
      if len(res)>0 and not args.quiet:
        print(status.getPrettyStatus())
        print('\n'+'\n'.join(res))
        print(status.summarizeProblems(pernode=True))
      if user_error_type in args.retry and len(sunz_inputs)>0:
        n = min(len(sunz_inputs),9)
        print('\n'+user_error_type+' Inputs:\n'+'\n'.join(sunz_inputs[0:n]))
        if len(sunz_inputs)>10:
          print('... (truncated)')
        print('\n')

  # otherwise always print status:
  else:
    print((status.getPrettyStatus()+'\n'))
    if args.details:
      print(status.getPrettyJsonDetails())

  # save job status in text file
#  if args.save:
#    if status.isComplete():
#      if status.isPreviousComplete():
#        return
#      print('WORKFLOW FINISHED:  '+workflow)
#    status.saveStatus()
#    status.saveLog()
#    if args.details:
#      status.saveDetails()
##    if args.publish:
##      subprocess.check_output(['rsync','-avz',args.logDir+'/',args.webhost+':'+args.webdir])

  if len(args.clas12mon)>0:
    if Matcher.matchAny(CLAS12SwifStatus.getHeader(workflow)['tag'],args.clas12mon):
      status.saveDatabase()


if __name__ == '__main__':

  df='\n(default=%(default)s)'

  epilog = 'Note, to pass argument values that start with a dash, use the "=" syntax, e.g. "swif-status.py --matchAny=-123-".'
  cli = argparse.ArgumentParser('Do SWIF stuff.',epilog=epilog)
  cli.add_argument('--list',         help='list workflows',    action='store_true',default=False)
  cli.add_argument('--workflow',     help='regex of workflow names, else all workflows (repeatable)', metavar='NAME', action='append',default=[])
  cli.add_argument('--retry',        help='retry problem jobs',metavar='PROBLEMTYPE', nargs='?',action='append',default=[])
  cli.add_argument('--details',      help='show all job details', action='store_true', default=False)
  cli.add_argument('--quiet',        help='do not print retries (for cron jobs)', action='store_true',default=False)
  cli.add_argument('--delete',       help='delete workflow(s)', action='store_true',default=False)
  cli.add_argument('--stats',        help='show completion status of each workflow component', action='store_true',default=False)
  cli.add_argument('--runStats',     help='show completion status of each run number', action='store_true',default=False)
  cli.add_argument('--phaseStats',   help='show completion status of each phase', action='store_true',default=False)
  cli.add_argument('--listRun',      help='list all job names associated with the given run number(s) (repeatable)', metavar='#', action='append', default=[], type=int)
  cli.add_argument('--listDirs',     help='list all output directories associated with the workflow', action='store_true',default=False)
  cli.add_argument('--missing',      help='list missing output files for jobs reported as success', action='store_true',default=False)
  cli.add_argument('--missingTape',  help='same as --missing, but assume /mss if originally written to /cache', action='store_true',default=False)
  cli.add_argument('--abandonRun',   help='abandon all jobs associated with particular run numbers (repeatable)', metavar='#', action='append', default=[], type=int)
  cli.add_argument('--abandon',      help='abandon jobs corresponding to a problem type (repeatable)',  metavar='PROBLEM', action='append',default=[])
  cli.add_argument('--problems',     help='show details of jobs whose most recent attempt was problematic', metavar='PROBLEM',nargs='?',const='ANY',default=False)
  cli.add_argument('--problemStats', help='show summary of all problems during the workflow', default=False,action='store_true')
  cli.add_argument('--problemNodes', help='same as --problemStats but per node', default=False,action='store_true')
  cli.add_argument('--problemInputs',help='generate list of input files for jobs with problems', metavar='PROBLEM',nargs='?',const='ANY',default=False)
  cli.add_argument('--problemLogs',  help='directory to print names of log files with problems', metavar='PATH',nargs='?',const=None,default=False)
  cli.add_argument('--problemLogsTail', help='number of lines to tail from the problem logs', metavar='#',type=int,nargs='?',const=10,default=False)
  cli.add_argument('--clas12mon',    help='write workflows with matching tag to clas12mon (repeatable)',metavar='TAG',type=str,default=[],action='append')
  cli.add_argument('--matchAll',     help='restrict to workflows containing all of these substrings (repeatable)', metavar='string', type=str, default=[], action='append')
  cli.add_argument('--matchAny',     help='restrict to workflows containing any of these substrings (repeatable)', metavar='string', type=str, default=[], action='append')
  cli.add_argument('--deleteComplete', help='delete all completed workflows', default=False, action='store_true')
#  cli.add_argument('--logDir',      help='local log directory'+df, metavar='PATH',type=str,default=None)
#  cli.add_argument('--save',        help='save to logs', action='store_true',default=False)
#  cli.add_argument('--jobLogs',    help='move job logs when complete', action='store_true',default=False)
#  cli.add_argument('--publish',    help='rsync to www dir', action='store_true',default=False)
#  cli.add_argument('--webdir',     help='rsync target dir'+df, type=str,default=None)
#  cli.add_argument('--webhost',    help='rsync target host'+df, type=str,default='jlabl5')
#  cli.add_argument('--input',      help='read workflow status from JSON file instead of querying SWIF', default=None,type=str)

  args = cli.parse_args()

#  if args.publish and not args.webdir:
#    sys.exit('ERROR:  must define --webdir if using the --publish option')
#  if args.save and not args.logDir:
#    cli.error('Must define --logDir if using the --save option')

  if len(args.workflow)>0:
    if len(args.matchAll)>0 or len(args.matchAny)>0:
      cli.error('--workflow not supported in conjuction with either --matchAll or --matchAny')

  if args.problemLogs is not False and args.problemLogs is not None:
    if not os.path.isdir(args.problemLogs):
      cli.error('optional argument to --problemLogs must be a directory')

  # generate the list of workflows to process:
  workflows=[]
  for wf in SwifStatus.getWorkflowNames():
    if len(args.workflow)==0:
      if Matcher.matchAll(wf,args.matchAll) and Matcher.matchAny(wf,args.matchAny):
        workflows.append(wf)
    else:
      for x in args.workflow:
        if re.match(x,wf) is not None:
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
    if args.stats:
      print('\n'+separator('TOTAL'))
      print(tally)

