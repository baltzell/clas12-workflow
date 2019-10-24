import os

from SwifJob import SwifJob

class Job(SwifJob):
  def __init__(self,workflow):
    SwifJob.__init__(self,workflow)
    self.addEnv('CCDB_CONNECTION','mysql://clas12reader@clasdb-farm.jlab.org/clas12')
    self.addEnv('RCDB_CONNECTION','mysql://rcdb@clasdb-farm.jlab.org/rcdb')
    self.addEnv('MALLOC_ARENA_MAX','2')

class DecodingJob(Job):
  def __init__(self,workflow):
    Job.__init__(self,workflow)
    self.setRam('3GB')
    self.addTag('mode','decode')

class ClaraJob(Job):
  def __init__(self,workflow):
    Job.__init__(self,workflow)
    #self.addEnv('CLARA_HOME','/group/clas12/packages/clara/4.3.11c_6.3.1')
    self.addEnv('CLARA_HOME','/group/clas12/packages/clara/4.3.11_6c.3.4')
    self.addEnv('JAVA_OPTS','-Xmx10g -Xms8g')
    self.addTag('mode','recon')
    self.setRam('12GB')
    self.setDisk('20GB')
    self.setTime('24h')
    self.setCores(16)
    self.addInput('clara.sh',os.path.dirname(os.path.realpath(__file__))+'/scripts/clara.sh')

if __name__ == '__main__':

  job=ClaraJob('wflow')
  job.setTrack('debug')
  job.setCmd('./clara.sh -t %d -l /volatile/clas12/users/baltzell/clara-test/nostage %s'%(16,job.getJobName()))
  job.addInput('clara.yaml','/volatile/clas12/users/baltzell/clara-test/data.yaml')
  job.addInput('clas_006501.evio.00000.hipo','/cache/clas12/rg-b/production/decoded/6b.1.1/006501/clas_006501.evio.00000.hipo')
  job.addOutput('rec_clas_006501.evio.00000.hipo','/volatile/clas12/users/baltzell/clara-test/nostage')

  from SwifWorkflow import SwifWorkflow
  wflow=SwifWorkflow('wflow')
  wflow.addJob(job)
  print(wflow.getJson())

