import logging

from SwifWorkflow import SwifPhase
from CLAS12Workflow import CLAS12Workflow

_LOGGER=logging.getLogger(__name__)

RUN_PHASING_CUTOFF=100

######################################################################
#
# In general, at the cost of throughput, the phasing here serves to:
# 1. avoid huge queues, allowing other jobs to queue instead
# 3. promote getting full runs processed
# 2. promote run-ordering on tape
#
######################################################################


class MinimalDependency(CLAS12Workflow):
######################################################################
#
# Minimal job-job dependencies, with *optional* phasing.
#
# If phasing is not used (i.e. phaseSize<0), this becomes the maximum
# throughput model, in the sense that there's as little dependcies and
# throttling as possible.
#
######################################################################
#
# If phasing is used, all the jobs corresponding to a given run are
# included in the same phase, which means all swif antecedents are in
# the same phase.
#
# For example, if phaseSize==2, then each phase contains:
#
# d(i) d(i+1) r(i) r(i+1) a(i) a(i+1)
#
# where i is the run number index and d/r/a is dec/rec/ana.
#
# This type of phasing has the potential drawback of trickling
# at the end of each phase when the analysis trains/merge/cleanup
# are running, and also slower at the beginning when it's just
# decoding jobs.
#
######################################################################
  def __init__(self,name,cfg):
    CLAS12Workflow.__init__(self,name,cfg)

  def generate(self):

    _LOGGER.info('Generating a MinimalDependency workflow')

    nruns,nfiles = 0,0

    for xx in self.getGroups():

      jput_jobs = []

      nruns += 1
      nfiles += len(xx)

      # interpret phase size as #files:
      if self.cfg['phaseSize']>=RUN_PHASING_CUTOFF and nfiles>self.cfg['phaseSize']:
        nfiles = 0
        self.phase += 1

      # interpret phase size as #runs:
      elif self.cfg['phaseSize']>0 and nruns>self.cfg['phaseSize']:
        nruns = 0
        self.phase += 1

      if self.cfg['model'].find('dec')>=0:

        if self.cfg['model'].find('mrg')>=0:
          xx = self.decodemerge(self.phase,xx)
        else:
          xx = self.decode(self.phase,xx)
        jput_jobs.extend(xx)

      if self.cfg['model'].find('rec')>=0:
        xx = self.reconclara(self.phase,xx)
        jput_jobs.extend(xx)

      if self.cfg['model'].find('his')>=0:
        yy = self.histo(self.phase,xx)

      if self.cfg['model'].find('ana')>=0:
        xx = self.train(self.phase,xx)
        xx.extend(self.trainmerge(self.phase,xx))
        jput_jobs.extend(xx)
        self.trainclean(self.phase,xx)

      self.jput(self.phase+1,jput_jobs)


class RollingRuns(CLAS12Workflow):
######################################################################
# 
# Stagger runs' tasks' across phases, in a "round".  These means all
# swif antecedents are in a previous phase, so no job has antecedents
# that are not ready at the time it is submitted.
#
# For example, if phaseSize==2, then each phase contains:
#
# d(i) d(i-1) r(i-2) r(i-3) a(i-4) a(i-5)
#
# where i is the run number index and d/r/a = dec/rec/ana.
#
# This phasing model has the advantage of letting decoding/analysis
# jobs sort of run in the background, while the overall throughput
# is dictated by the longer reconstruction jobs.
#
######################################################################
  def __init__(self,name,cfg):
    CLAS12Workflow.__init__(self,name,cfg)

  def generate(self):

    _LOGGER.info('Generating a RollingRuns workflow')

    # master-queue:
    queue=self.getGroups()

    # sub-queues:
    decodeQ,mergeQ,reconQ,trainQ=[],[],[],[]

    nruns,nfiles = 0,0

    while True:

      jput_jobs = []

      if len(trainQ)>0:
        xx = trainQ.pop(0)
        trainJobs = self.train(xx.phase,xx.jobs)
        trainJobs.extend(self.trainmerge(xx.phase,trainJobs))
        jput_jobs.extend(trainJobs)
        self.trainclean(xx.phase,trainJobs)

      if len(reconQ)>0:
        xx = reconQ.pop(0)
        reconJobs=self.reconclara(xx.phase,xx.jobs)
        jput_jobs.extend(reconJobs)
        if self.cfg['model'].find('ana')>=0:
          trainQ.append(SwifPhase(xx.phase+1,reconJobs))

      if len(decodeQ)>0:
        xx = decodeQ.pop(0)
        if self.cfg['model'].find('mrg')>=0:
          decodeJobs = self.decodemerge(xx.phase,xx.jobs)
        else:
          decodeJobs = self.decode(xx.phase,xx.jobs)
        jput_jobs.extend(decodeJobs)
        if self.cfg['model'].find('rec')>=0:
          reconQ.append(SwifPhase(xx.phase+1,decodeJobs))

      if len(queue)>0:

        files = queue.pop(0)
        nruns += 1
        nfiles += len(files)

        if self.cfg['model'].find('dec')>=0:
          decodeQ.append(SwifPhase(self.phase,files))
        elif self.cfg['model'].find('rec')>=0:
          reconQ.append(SwifPhase(self.phase,files))
        elif self.cfg['model'].find('ana')>=0:
          trainQ.append(SwifPhase(self.phase,files))

        # interpret phase size as #files:
        if self.cfg['phaseSize']>=RUN_PHASING_CUTOFF and nfiles>self.cfg['phaseSize']:
          nruns = 0
          nfiles = 0
          self.phase += 1

        # interpret phase size as #runs:
        elif self.cfg['phaseSize']>0 and nruns>self.cfg['phaseSize']:
          nruns = 0
          nfiles = 0
          self.phase += 1

      self.jput(self.phase+1,jput_jobs)

      if len(decodeQ)+len(reconQ)+len(trainQ) == 0:
        break



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

