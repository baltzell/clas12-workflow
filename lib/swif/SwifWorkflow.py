import tempfile,subprocess,collections,json
from RunFileUtil import RunFileGroups
from SwifJob import SwifJob
from SwifStatus import SWIF,SwifStatus

class SwifWorkflow(RunFileGroups):

  def __init__(self,name):
    RunFileGroups.__init__(self)
    self.setPhaseSize(0)
    self.name=name
    self.jobs=[]
    self.phase=0
    # new for swif2:
    self.maxConcurrent=int(1e4)
    self.site='jlab/enp'
    #self.storage='enp:luster'
    #self.siteLogin=None

  def getStatus(self):
    return SwifStatus(self.name)

  def addJob(self,job):
    if isinstance(job,list):
      for j in job: self.addJob(j)
    if not isinstance(job,SwifJob):
      raise TypeError('Must be a SwifJob')
    job.setNumber(len(self.jobs)+1)
    self.jobs.append(job)

  def setPhaseSize(self,phaseSize):
    RunFileGroups.setGroupSize(self,phaseSize)

  def getJobs(self,phase):
    jobs=[]
    for job in self.jobs:
      if job.phase == phase:
        jobs.append(job)
    return jobs

  def getJson(self):
    data = collections.OrderedDict()
    data['name'] = self.name
    data['site'] = self.site
    data['max_dispatched'] = self.maxConcurrent
    data['jobs']=[job.toJson() for job in self.jobs]
    return json.dumps(data,**{'indent':2,'separators':(',',': ')})

  def submitShell(self):
    for cmd in self.getShell():
      print((subprocess.check_output(cmd)))

  def submitJson(self):
    with tempfile.NamedTemporaryFile(mode='w') as jsonFile:
      jsonFile.write(self.getJson())
      jsonFile.flush()
      print((subprocess.check_output([SWIF,'import','-file',jsonFile.name])))
    print((subprocess.check_output([SWIF,'run','-workflow',self.name])))
    # sometimes SWIF's not ready yet, so don't do this:
    #print((subprocess.check_output([SWIF,'status','-workflow',self.name])))

class SwifPhase():
  def __init__(self,phase,jobs):
    if not isinstance(phase,int):
      raise TypeError('phase must be an integer')
    if not isinstance(jobs,list):
      raise TypeError('jobs must be a list')
    self.phase=phase
    self.jobs=jobs
  def __str__(self):
    return 'Phase %d : %s (%d)'%(self.phase,self.jobs[0],len(self.jobs))

if __name__ == '__main__':
  name = 'test'
  workflow = SwifWorkflow(name)
  for ii in range(3):
    job = SwifJob(name)
    job.setDisk('3GB')
    job.setRam('500MB')
    job.setTime('30m')
    job.setPartition('priority')
    job.addTag('meal',['breakfast','lunch','dinner'][ii])
    job.addInput('in.evio','/mss/hallb/hps/physrun2021/data/hps_014244/hps_014244.evio.01235')
    job.addOutput('x.txt','/home/baltzell/swif2-test/xx-%d.txt'%ii)
    job.setCmd('ls -l > x.txt')
    workflow.addJob(job)
  print((workflow.getShell()))
  print((workflow.getJson()))
  #workflow.submitJson()

