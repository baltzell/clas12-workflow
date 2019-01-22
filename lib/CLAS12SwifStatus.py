import os
import subprocess
import requests
import json

from ChefUtil import mkdir
from SwifStatus import SwifStatus

class CLAS12SwifStatus(SwifStatus):
  def __init__(self,workflow,args):
    SwifStatus.__init__(self,workflow)
    self.logFilename    =args.logdir+'/logs/'+workflow+'.json'
    self.statusFilename =args.logdir+'/status/'+workflow+'.json'
    self.detailsFilename=args.logdir+'/details/'+workflow+'.json'
    self.dburl='https://clas12mon.jlab.org/api/SwifEntries'
    self.dbauth=None
    self.previous=None
    try:
      with open(self.statusFilename,'r') as statusFile:
        self.previous=SwifStatus(workflow)
        self.previous.loadStatusFromString('\n'.join(statusFile.readlines()))
    except:
      pass
    try:
      with open(os.getenv('HOME')+'/'+'.clas12mon.auth') as authFile:
        self.dbauth=authFile.read().strip()
    except:
      pass
  def saveDatabase(self):
    if self.dbauth is None:
      print 'Missing credentials in $HOME/.clas12mon.auth'
    else:
      data={'entry':json.dumps(self.status)}
      headers={'Authorization':self.dbauth}
      requests.post(self.dburl,data=data,headers=headers)
  def isPreviousComplete(self):
    return self.previous is not None and self.previous.isComplete()
  def saveStatus(self):
    mkdir(args.logdir+'/status/')
    with open(self.statusFilename.replace('.json','.txt'),'w') as statusFile:
      statusFile.write(self.getPrettyStatus())
      if self.isComplete(): statusFile.write('\n\nWORKFLOW FINISHED:  '+workflow+'\n')
      statusFile.close()
    with open(self.statusFilename,'w') as statusFile:
      statusFile.write(self.getPrettyJsonStatus())
      statusFile.close()
  def saveLog(self):
    mkdir(args.logdir+'/logs/')
    with open(self.logFilename,'a+') as logFile:
      logFile.write('\n'+self.getPrettyJsonStatus())
      logFile.close()
  def saveDetails(self):
    mkdir(args.logdir+'/details/')
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
  import argparse
  a=argparse.ArgumentParser()
  a.add_argument('--logdir')
  args=a.parse_args(['--logdir','/home/baltzell/logs/clas12-workflow'])
  s=CLAS12SwifStatus('DecAndRecTest3_R4013x1_x30',args)
  s.status=s.previous.status
  #s.saveDatabase()

