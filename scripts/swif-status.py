#!/usr/bin/env python
import os
import subprocess
import argparse
from SwifStatus import *

def mkdir(path):
  if os.access(path,os.F_OK):
    if not os.access(path,os.W_OK):
      raise IOError('Permissions error on '+path)
  else:
    os.makedirs(path)

def getWorkflowNames():
  workflows=[]
  for line in subprocess.check_output([SWIF,'list']).splitlines():
    line=line.strip()
    if line.find('workflow_name')==0:
      workflows.append(line.split('=')[1].strip())
  return workflows

def publish(args):
  rsyncCmd=['rsync','-avz',args.logdir+'/',args.webhost+':'+args.webdir]
  subprocess.check_output(rsyncCmd)

class CLAS12SwifStatus(SwifStatus):

  def __init__(self,workflow,args):
    SwifStatus.__init__(self,workflow)
    self.logFilename    =args.logdir+'/logs/'+workflow+'.log'
    self.statusFilename =args.logdir+'/status/'+workflow+'.txt'
    self.detailsFilename=args.logdir+'/details/'+workflow+'.txt'
    self.previous=None
    try:
      with open(self.statusFilename,'r') as statusFile:
        self.previous=SwifStatus(workflow)
        self.previous.loadStatusFromString('\n'.join(statusFile.readlines()))
    except:
      pass

  def saveStatus(self):
    with open(self.statusFilename,'w') as statusFile:
      statusFile.write(self.getPrettyStatus())
      if self.isComplete(): statusFile.write('\nWORKFLOW FINISHED:  '+workflow+'\n')
      statusFile.close()

  def saveLog(self):
    with open(self.logFilename,'a+') as logFile:
      logFile.write('\n'+self.getPrettyJsonStatus())
      if self.isComplete(): logFile.write('\nWORKFLOW FINISHED:  '+workflow+'\n')
      logFile.close()

  def saveDetails(self):
    with open(self.detailsFilename,'w') as detailsFile:
      detailsFile.write(self.getPrettyJsonDetails())
      detailsFile.close()

  def moveJobLogs(self):
    workDir = self.getTagValue('workDir')
    if workDir is not None:
      src=os.getenv('HOME')+'/.farm_out'
      dest=workDir+'/farm_out/'+self.workflow
      mkdir(dest)
      subprocess.check_output(['mv','%s/%s*'%(src,workflow),dest])

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
  cli.add_argument('--logdir',  help='local log directory'+df, type=str,default='/work/clas12/baltzell/clas12-workflow')
  cli.add_argument('--webdir',  help='rsync target dir'+df,    type=str,default='/home/baltzell/public_html/clas12/wflow/clas12')
  cli.add_argument('--webhost', help='rsync target host'+df,   type=str,default='jlabl5')

  args = cli.parse_args()

  if len(args.workflow)==0:
    args.workflow=getWorkflowNames()

  if args.list:
    print '\n'.join(args.workflow)

  else:

    for workflow in args.workflow:

      status = CLAS12SwifStatus(workflow,args)

      #if status.isComplete() and status.previous.isComplete():
      #  status.moveJobLogs()

      if args.save:

        mkdir(args.logdir+'/status/')
        mkdir(args.logdir+'/logs/')
        mkdir(args.logdir+'/details/')

        if status.isComplete():
          if status.previous.isComplete():
            continue
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

      if args.retry:
        result = status.retryProblems()
        if len(result)>0:
          print '\n'.join(str(result))

    if args.publish:
      publish(args)


