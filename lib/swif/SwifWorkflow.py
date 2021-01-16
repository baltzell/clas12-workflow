import tempfile,subprocess,collections,json
from RunFileUtil import RunFileGroups
from SwifJob import SwifJob
from SwifStatus import SWIF

class SwifWorkflow(RunFileGroups):

  def __init__(self,name):
    RunFileGroups.__init__(self)
    self.setPhaseSize(0)
    self.name=name
    self.jobs=[]
    self.phase=0
    # new for swif2:
    self.maxConcurrent=1e4
    self.site='jlab/enp'
    #self.storage='enp:luster'
    #self.siteLogin=None

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

  def getShell(self):
    ret=[]
    cmd=[SWIF,'create','-workflow',self.name,'-site',self.site]
    cmd.extend['-max-concurrent',self.maxConcurrent]
    ret.append(cmd)
    for job in self.jobs:
      ret.append(job.getShell())
    return ret

  def getJson(self):
    data = collections.OrderedDict()
    data['name'] = self.name
    data['site'] = self.site
    data['max-concurrent'] = self.maxConcurrent
    data['jobs']=[job.getJson() for job in self.jobs]
    return json.dumps(data,**{'indent':2,'separators':(',',': ')})

  def submitShell(self):
    for cmd in self.getShell():
      print((subprocess.check_output(cmd)))

  def submitJson(self):
    with tempfile.NamedTemporaryFile() as jsonFile:
      jsonFile.write(self.getJson())
      jsonFile.flush()
      print((subprocess.check_output([SWIF,'import','-file',jsonFile.name])))
    print((subprocess.check_output([SWIF,'run','-workflow',self.name])))
    print((subprocess.check_output([SWIF,'status','-workflow',self.name])))

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
  name = 'myWorkflow'
  workflow = SwifWorkflow(name)
  for ii in range(3):
    job = SwifJob(name)
    job.setPhase(ii+1)
    job.addTag('meal',['breakfast','lunch','dinner'][ii])
    job.addInput('in.evio','/mss/clas12/foo%d.evio'%ii)
    job.addOutput('out.evio','/my/dir/foo%d.xml'%ii)
    job.setCmd('evio2xml in.evio > out.xml')
    job.setLogDir('/tmp/log')
    workflow.addJob(job)
  print((workflow.getShell()))
  print((workflow.getJson()))
  #workflow.submitJson()

