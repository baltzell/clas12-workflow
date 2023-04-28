import os,re,json,requests,subprocess

from ChefConfig import CHOICES,fullModel
from ChefUtil import mkdir
from SwifStatus import SwifStatus

# FIXME: move logging up to SwifStatus

class CLAS12SwifStatus(SwifStatus):

  def __init__(self,name,args,source=None):
    SwifStatus.__init__(self,name,source)
    self.args=args
    self.logFilename    =None
    self.statusFilename =None
    self.detailsFilename=None
    #if self.args.logdir is not None:
    #  self.logFilename    =args.logdir+'/logs/'+name+'.json'
    #  self.statusFilename =args.logdir+'/status/'+name+'.json'
    #  self.detailsFilename=args.logdir+'/details/'+name+'.json'
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

  def findMissingOutputs(self,tape=False):
    ret=[]
    # remove transient outputs:
    for x in SwifStatus.findMissingOutputs(self,tape=False):
      if re.search('train/\d\d\d\d\d\d/skim\d',x) is None:
        ret.append(x)
    return ret

  def saveLog(self):
    #mkdir(self.args.logdir+'/logs/')
    with open(self.logFilename,'a+') as logFile:
      logFile.write('\n'+self.getPrettyJsonStatus())
      logFile.close()

  def saveDetails(self):
    #mkdir(self.args.logdir+'/details/')
    with open(self.detailsFilename,'w') as detailsFile:
      detailsFile.write(self.getPrettyJsonDetails())
      detailsFile.close()

  def isCompleteInDatabase(self):
    r = requests.get(self.dburl)
    r.raise_for_status()
    j = json.loads(r.text)
    for i in range(len(j)-1,-1,-1):
      e = j[i]['entry']
      if e['workflow_name'] == self.name:
        return e['succeeded']+e.get('abandoned',0) == e['jobs']
    return False

  def getStatusForDatabase(self):
    s = self.getPrunedStatus().pop(0)
    s['pending'] = s['dispatched_pending']
    s['preparing'] = s['dispatched_preparing']
    s['running'] = s['dispatched_running']
    s['reaping'] = s['dispatched_reaping']
    del s['abandoned']
    del s['workflow_site']
    del s['max_concurrent']
    del s['workflow_user']
    del s['workflow_id']
    del s['dispatched_other']
    del s['undispatched']
    del s['summary_ts']
    del s['dispatched_preparing']
    del s['dispatched_reaping']
    del s['dispatched_running']
    del s['dispatched_pending']
    return s

  def saveDatabase(self,full=False):
    data = {'run_group':self.name.split('-').pop(0)}
    status = self.getStatusForDatabase()
    if data.get('workflow_suspended',0) == 0:
      data['entry'] = json.dumps(status)
      r = requests.post(self.dburl,data=data,headers={'Authorization':self.dbauth})
      r.raise_for_status()

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

