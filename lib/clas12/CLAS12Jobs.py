import os

import ChefUtil
from RunFileUtil import RunFile
from SwifJob import SwifJob

class Job(SwifJob):
  def __init__(self,workflow,cfg):
    SwifJob.__init__(self,workflow)
    self.addEnv('CCDB_CONNECTION','mysql://clas12reader@clasdb-farm.jlab.org/clas12')
    self.addEnv('RCDB_CONNECTION','mysql://rcdb@clasdb-farm.jlab.org/rcdb')
    self.addEnv('MALLOC_ARENA_MAX','2')
    self.inputData=[]
    self.outputData=[]
    self.cfg=cfg
  def addInputData(self,filename):
    self.inputData.append(filename)
    basename=filename.split('/').pop()
    runno=RunFile(filename).runNumber
    fileno=RunFile(filename).fileNumber
    self.addTag('run','%.6d'%runno)
    self.addTag('file','%.5d'%fileno)

class DecodingJob(Job):
  def __init__(self,workflow,cfg):
    Job.__init__(self,workflow,cfg)
    self.setRam('3GB')
    self.addTag('mode','decode')

class ClaraJob(Job):
  def __init__(self,workflow,cfg):
    Job.__init__(self,workflow,cfg)
    self.addEnv('CLARA_HOME',cfg['clara'])
    self.addEnv('JAVA_OPTS','-Xmx10g -Xms8g')
    self.addTag('mode','recon')
    self.setRam('12GB')
    self.setDisk('20GB')
    self.setTime('24h')
    self.setCores(16)
    self.addInput('clara.sh',os.path.dirname(os.path.realpath(__file__))+'/scripts/clara.sh')
    self.addInput('clara.yaml',cfg['reconYaml'])
  def addOutputData(self,basename):
    outDir='%s/recon/%s/'%(self.cfg['outDir'],self.getTag('run'))
    ChefUtil.mkdir(outDir)
    self.addTag('outDir',outDir)
    self.outputData.append(outDir+'/'+basename)
    self.addOutput(basename,outDir+'/'+basename)
  def addInputData(self,filename):
    Job.addInputData(self,filename)
    basename=filename.split('/').pop()
    self.addInput(basename,filename)
    self.addOutputData('rec_'+basename)

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

