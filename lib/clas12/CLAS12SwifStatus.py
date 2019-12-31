import os,json,requests,subprocess

from ChefConfig import CHOICES
from ChefUtil import mkdir
from SwifStatus import SwifStatus

# FIXME: move logging up to SwifStatus

class CLAS12SwifStatus(SwifStatus):
  def __init__(self,name,args):
    SwifStatus.__init__(self,name)
    self.args=args
    self.logFilename    =None
    self.statusFilename =None
    self.detailsFilename=None
    if self.args.logdir is not None:
      self.logFilename    =args.logdir+'/logs/'+name+'.json'
      self.statusFilename =args.logdir+'/status/'+name+'.json'
      self.detailsFilename=args.logdir+'/details/'+name+'.json'
    self.dburl='https://clas12mon.jlab.org/api/SwifEntries'
    self.dbauth=None
    self.previous=None
    try:
      with open(self.statusFilename,'r') as statusFile:
        self.previous=SwifStatus(name)
        self.previous.loadStatusFromString('\n'.join(statusFile.readlines()))
    except:
      pass
    try:
      with open(os.getenv('HOME')+'/'+'.clas12mon.auth') as authFile:
        self.dbauth=authFile.read().strip()
    except:
      pass
  def isPreviousComplete(self):
    return self.previous is not None and self.previous.isComplete()
  def saveStatus(self):
    mkdir(self.args.logdir+'/status/')
    with open(self.statusFilename.replace('.json','.txt'),'w') as statusFile:
      statusFile.write(self.getPrettyStatus())
      if self.isComplete(): statusFile.write('\n\nWORKFLOW FINISHED:  '+self.name+'\n')
      statusFile.close()
    with open(self.statusFilename,'w') as statusFile:
      statusFile.write(self.getPrettyJsonStatus())
      statusFile.close()
  def saveLog(self):
    mkdir(self.args.logdir+'/logs/')
    with open(self.logFilename,'a+') as logFile:
      logFile.write('\n'+self.getPrettyJsonStatus())
      logFile.close()
  def saveDetails(self):
    mkdir(self.args.logdir+'/details/')
    with open(self.detailsFilename,'w') as detailsFile:
      detailsFile.write(self.getPrettyJsonDetails())
      detailsFile.close()
  def saveDatabase(self):
    if self.dbauth is None:
      print 'Missing clas12mon credentials.'
      return
    elif self.name.count('-')<2:
      print 'Invalid workflow name for clas12mon:  '+self.name
      return
    else:
      # Assume workflow name is prefixed with 'runGroup-task-'.
      # Note, we used to instead store these in Swif job tags, which was
      # cleaner but created unecessary overhead on Swif due to needing
      # to read full workflow status to just to extract a couple job tags.
      # Revisit if Swif later provides for global workflow tags.
      tags=self.name.split('-')
      run_group=tags[0]
      task=tags[1]
      if runGroup not in CHOICES['runGroup'] or task not in CHOICES['model']:
        print 'Invalid workflow name for clas12mon:  '+self.name
        return
      status=self.getPrunedStatus()
      # convert to json string, and strip off leading/trailing
      # square brackets for clas12mon:
      data={}
      data['entry'] = json.dumps(status).lstrip('[').rstrip().rstrip(']')
      data['run_group'] = run_group
      data['task'] = task
      headers={'Authorization':self.dbauth}
      return requests.post(self.dburl,data=data,headers=headers)
  def __saveDatabase(self):
    # don't do this, requires full workflow details just to get rungroup
    # and task, unnecessary load on Swif
    pass
    # pull all job tags for clas12mon:
    if not self.tagsMerged:
      self.mergeTags()
    if self.dbauth is None:
      print 'Missing credentials in $HOME/.clas12mon.auth'
    else:
      # pull out special tags for clas12mon:
      task=self.removeTag('task')
      run_group=self.removeTag('run_group')
      # get status without any nulls:
      status=self.getPrunedStatus()
      # convert to json string, and strip off leading/trailing
      # square brackets for clas12mon:
      data={}
      data['entry'] = json.dumps(status).lstrip('[').rstrip().rstrip(']')
      data['run_group'] = run_group
      data['task'] = task
      headers={'Authorization':self.dbauth}
      return requests.post(self.dburl,data=data,headers=headers)
  def moveJobLogs(self):
    workDir = self.getTagValue('workDir')
    if workDir is not None:
      src=os.getenv('HOME')+'/.farm_out'
      dest=workDir+'/farm_out/'+self.name
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

