import os

from CLAS12Job import CLAS12Job

class CLAS12ClaraJob(CLAS12Job):
  def __init__(self,workflow):
    CLAS12Job.__init__(self,workflow)
    self.env['MALLOC_ARENA_MAX']='2'
    self.env['CLARA_HOME']='/group/clas12/packages/clara/4.3.11c_6.3.1'
    self.addTag('mode','recon')
    self.addInput('clara.sh',os.path.dirname(os.path.realpath(__file__))+'/scripts/clara.sh')

if __name__ == '__main__':
  job=CLAS12ClaraJob('job')
  job.setTrack('debug')
  job.setRam('16GB')
  job.setDisk('20GB')
  job.setTime('24h')
  job.setCores(16)
  job.setCmd('./clara.sh -t %d -l /volatile/clas12/users/baltzell/clara-test/nostage %s'%(16,job.getJobName()))
  job.addInput('clara.yaml','/volatile/clas12/users/baltzell/clara-test/data.yaml')
  job.addInput('clas_006501.evio.00000.hipo','/cache/clas12/rg-b/production/decoded/6b.1.1/006501/clas_006501.evio.00000.hipo')
  job.addOutput('rec_clas_006501.evio.00000.hipo','/volatile/clas12/users/baltzell/clara-test/nostage')

  from SwifWorkflow import SwifWorkflow
  wflow=SwifWorkflow('wflow')
  wflow.addJob(job)
  print wflow.getJson()

