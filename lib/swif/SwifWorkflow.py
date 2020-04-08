import tempfile,subprocess
from RunFileUtil import RunFileGroups
from SwifJob import SwifJob

SWIF='/site/bin/swif'

class SwifWorkflow(RunFileGroups):

  def __init__(self,name):
    RunFileGroups.__init__(self)
    self.setPhaseSize(1000)
    self.name=name
    self.jobs=[]
    self.phase=0

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
    return '\n'.join([job.getShell() for job in self.jobs])

  def getJson(self):
    json  = '{"name":"'+self.name+'","jobs":[\n'
    json += ',\n'.join([job.getJson() for job in self.jobs])
    json += '\n]}'
    return json

  def submitShell(self):
    for job in self.jobs:
      print(subprocess.check_output(job.getShell().split()))

  def submitJson(self):
    with tempfile.NamedTemporaryFile() as jsonFile:
      jsonFile.write(self.getJson())
      jsonFile.flush()
      print(subprocess.check_output([SWIF,'import','-file',jsonFile.name]))
    print(subprocess.check_output([SWIF,'run','-workflow',self.name]))
    print(subprocess.check_output([SWIF,'status','-workflow',self.name]))

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
  print(workflow.getShell())
  print(workflow.getJson())
  #workflow.submitJson()

