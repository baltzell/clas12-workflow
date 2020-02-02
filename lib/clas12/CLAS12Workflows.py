import logging

from CLAS12Workflow import CLAS12Workflow

_LOGGER=logging.getLogger(__name__)

class MinimalDependency(CLAS12Workflow):
#
# Minimal job-job dependencies, and no phases.  No staging/temporary
# disk space is used.  Maximal batch farm footprint, limited only by
# single job dependencies.
#
  def __init__(self,name,cfg):
    CLAS12Workflow.__init__(self,name,cfg)

  def generate(self):

    _LOGGER.info('Generating a MinimalDependency workflow ...')

    for xx in self.getGroups():

      if self.cfg['model'].find('dec')>=0:

        if self.cfg['model'].find('mrg')>=0:
          xx = self.decodemerge(self.phase,xx)
        else:
          xx = self.decode(self.phase,xx)

      if self.cfg['model'].find('rec')>=0:
        xx = self.reconclara(self.phase,xx)

      if self.cfg['model'].find('ana')>=0:
        xx = self.train(self.phase,xx)

    if self.cfg['model'].find('ana')>=0:
      xx = self.trainmerge(self.phase,self.jobs)
      xx = self.trainclean(self.phase,self.jobs)


class RollingRuns(CLAS12Workflow):
#
# Phased, with N runs per phase, where N is the number of tasks
# (i.e. decode/merge/recon/train).  No explicit job-job depenedencies.
# Staging disk space is used if merging, in which case 3*M GB is
# required, where M is the number of files per run (or phaseSize).
#
# This workflow has the benefit of putting files back on tape ordered
# by run, at the cost of potential bottlenecks of stuck tapes for
# inputs.  Similarly, its batch farm footprint is throttled by run
# sizes, allowing other workflows to run in parallel.
#
  def __init__(self,name,cfg):
    CLAS12Workflow.__init__(self,name,cfg)

  def generate(self):

    _LOGGER.info('Generating a RollingRuns workflow ...')

    # master-queue:
    queue=self.getGroups()

    # sub-queues:
    decodeQ,mergeQ,deleteQ,moveQ,reconQ,trainQ=[],[],[],[],[],[]

    while True:

      self.phase += 1

      if len(trainQ)>0:
        self.train(self.phase,trainQ.pop(0))

      if len(reconQ)>0:
        reconJobs=self.reconclara(self.phase,[reconQ.pop(0)])
        if self.cfg['model'].find('ana')>=0:
          trainQ.append(reconJobs)

      if len(deleteQ)>0:
        self.delete(self.phase,deleteQ.pop(0))
        moveJobs=self.move(self.phase,moveQ.pop(0))
        if self.cfg['model'].find('rec')>=0:
          reconQ.extend(moveJobs)

      if len(mergeQ)>0:
        decodedFiles = mergeQ.pop(0)
        mergeJobs = self.merge(self.phase,decodedFiles)
        moveQ.append([x.outputData[0] for x in mergeJobs])
        deleteQ.append(decodedFiles)

      if len(decodeQ)>0:
        if self.cfg['workDir'] is None:
          decodeJobs = self.decodemerge(self.phase,decodeQ.pop(0))
          if self.cfg['model'].find('rec')>=0:
            reconQ.extend(decodeJobs)
        else:
          decodeJobs = self.decode(self.phase,decodeQ.pop(0))
          if self.cfg['model'].find('mrg')>=0:
            mergeQ.append([x.outputData[0] for x in decodeJobs])
          elif self.cfg['model'].find('rec')>=0:
            reconQ.extend(decodeJobs)

      if len(queue)>0:
        if self.cfg['model'].find('dec')>=0:
          decodeQ.append(queue.pop())
        elif self.cfg['model'].find('rec')>=0:
          reconQ.extend(queue.pop())
        elif self.cfg['model'].find('ana')>=0:
          trainQ.append(queue.pop())

      if len(decodeQ)==0 and len(mergeQ)==0 and len(deleteQ)==0 and len(reconQ)==0 and len(trainQ)==0:
        break

    if self.cfg['model'].find('ana')>=0:
      xx = trainmerge(self.phase,self.jobs)



#if __name__ == '__main__':
#  import os,sys
#  from ChefConfig import getConfig
#  cli,cfg = getConfig(sys.argv[1:])
#  cfg['outDir']=os.getenv('HOME')+'/tmp/clas12-workflow/outDir'
#  cfg['workDir']=os.getenv('HOME')+'/tmp/clas12-workflow/workDir'
#  workflow = RollingDecoding('test',cfg)
#  workflow.setPhaseSize(1000)
#  workflow.addRun(4013)
#  workflow.addFiles(open('/home/baltzell/clas12/rga/rga-spring-files.txt','r').readlines())
#  workflow.generate()
#  print(workflow.getShell())
#  print(workflow.getJson())

