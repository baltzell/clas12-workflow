import os
from SwifWorkflow import SwifWorkflow
from SwifJob import SwifJob
from RunFileUtil import RunFile
from ChefUtil import *

class CLAS12DecodingWorkflow(SwifWorkflow):

  def __init__(self,name,cfg):
    SwifWorkflow.__init__(self,name)
    self.cfg=cfg
    self.setPhaseSize(self.cfg['phaseSize'])
    mkdir(self.cfg['outDir'])
    mkdir(self.cfg['workDir'])
    mkdir(self.cfg['workDir']+'/singles')
    mkdir(self.cfg['workDir']+'/merged')
    mkdir(self.cfg['workDir']+'/logs/'+self.name)
    for run in self.cfg['runs']:
      self.addRun(run)
    self.rcdb=RcdbManager()

  def addJob(self,job):
    job.setLogDir(self.cfg['workDir']+'/logs/'+self.name)
    SwifWorkflow.addJob(self,job)

  def addRun(self,run):
    SwifWorkflow.addRun(self,run)
    mkdir(self.cfg['outDir']+'/'+str(run))
    mkdir(self.cfg['workDir']+'/singles/'+str(run))
    mkdir(self.cfg['workDir']+'/merged/'+str(run))

  #
  # recon:  add jobs for running single-threaded recon
  # - one job per file
  # - return list of reconstructed hipo files
  #
  def recon(self,phase,hipoFiles):

    reconned=[]

    for hipoFileName in hipoFiles:

      runno = RunFile(hipoFileName).runNumber
      fileno = RunFile(hipoFileName).fileNumber
      reconBaseName = os.path.basename(hipoFileName).replace('.hipo','.recon.hipo')

      nFiles = 1
      subDir = 'singles'
      if reconBaseName.count('-')>0:
        nFiles = self.cfg['mergeSize']
        subDir = 'merged'

      outDir = '%s/recon/%s/%d'%(self.cfg['workDir'],subDir,runno)
      reconFileName = outDir+'/'+reconBaseName
      mkdir(outDir)

      job=SwifJob(self.name)
      job.setPhase(phase)
      job.setRam('9GB')
      job.setTime('%dh'%(12*nFiles))
      job.setDisk('%dGB'%(4*nFiles))
      job.addTag('run','%.5d'%runno)
      job.addTag('file','%.5d'%fileno)
      job.addTag('mode','recon')
      job.addTag('coatjava',self.cfg['coatjava'])
      job.addTag('workDir',self.cfg['workDir'])
      job.addInput('in.hipo',hipoFileName)
      job.addOutput('out.hipo',reconFileName)

      cmd= ' setenv GEOMDBVAR may_2018_engineers ;'
      cmd+=' setenv USESTT true ;'
      cmd+=' setenv SOLSHIFT -1.9 ;'
      cmd+=' %s/bin/notsouseful-util -c 2 -i in.hipo -o out.hipo'%self.cfg['coatjava']
      cmd+=' && ls out.hipo'
      cmd+=' && %s/bin/hipo4utils -test out.hipo'%self.cfg['coatjava']
      cmd+=' || rm -f out.hipo && ls out.hipo'
      job.setCmd(cmd)

      self.addJob(job)

    return hipoFiles

  #
  # decode:  add jobs for decoding evio files
  # - one job per file
  # - return list of output hipo files
  #
  def decode(self,phase,evioFiles):

    hipoFiles=[]

    for evioFileName in evioFiles:

      runno = RunFile(evioFileName).runNumber
      fileno = RunFile(evioFileName).fileNumber
      hipoBaseName = os.path.basename(evioFileName)+'.hipo'
      hipoFileName = self.cfg['workDir']+'/singles/'+str(runno)+'/'+hipoBaseName

      hipoFiles.append(hipoFileName)

      job=SwifJob(self.name)
      job.setPhase(phase)
      job.addTag('run','%.5d'%runno)
      job.addTag('file','%.5d'%fileno)
      job.addTag('mode','decode')
      job.addTag('coatjava',self.cfg['coatjava'])
      job.addTag('workDir',self.cfg['workDir'])
      job.addInput('in.evio',evioFileName)
      job.addOutput('out.hipo',hipoFileName)

      s = self.cfg['solenoid']
      t = self.cfg['torus']
      if s is None: s = self.rcdb.getSolenoidScale(runno)
      if t is None: t = self.rcdb.getTorusScale(runno)
      decoderOpts = '-c 2 -s %.4f -t %.4f'%(s,t)

      cmd='%s/bin/decoder4 %s -o out.hipo in.evio'%(self.cfg['coatjava'],decoderOpts)
      cmd+=' && ls out.hipo'
      cmd+=' && %s/bin/hipo4utils -test out.hipo'%self.cfg['coatjava']
      cmd+=' || rm -f out.hipo && ls out.hipo'
      job.setCmd(cmd)

      self.addJob(job)

    return hipoFiles

  #
  # merge:  add jobs for merging hipo files
  # - one job per merge
  # - return list of output merged hipo files
  #
  def merge(self,phase,hipoFiles):

    inputs,merged=[],[]

    for ii in range(len(hipoFiles)):

      inputs.append(hipoFiles[ii])

      if len(inputs)>=self.cfg['mergeSize'] or ii>=len(hipoFiles)-1:

        runno = RunFile(inputs[0]).runNumber
        fileno1 = RunFile(inputs[0]).fileNumber
        fileno2 = RunFile(inputs[len(inputs)-1]).fileNumber
        outFile=self.cfg['workDir']+'/merged/'+str(runno)+'/'+\
            self.cfg['mergePattern']%(runno,fileno1,fileno2)
        merged.append(outFile)

        job=SwifJob(self.name)
        job.setPhase(phase)
# Note this RAM request is for PBS, on SLURM will be much lower
        job.setRam('9GB')
        job.setTime(getMergeTimeReq(self.cfg['mergeSize']))
        job.setDisk(getMergeDiskReq(self.cfg['mergeSize']))
        job.addTag('run','%.5d'%runno)
        job.addTag('file','%.5d-%.5d'%(fileno1,fileno2))
        job.addTag('mode','merge')
        job.addTag('coatjava',self.cfg['coatjava'])
        job.addTag('workDir',self.cfg['workDir'])
        job.addOutput('out.hipo',outFile)

        cmd = 'rm -f '+outFile+' ; '
        cmd += '%s/bin/hipo4utils -merge -o out.hipo'%self.cfg['coatjava']
        for ii in range(len(inputs)):
          job.addInput('in%.4d.hipo'%ii,inputs[ii])
          cmd += ' in%.4d.hipo'%ii
        cmd+=' && ls out.hipo'
        cmd+=' && %s/bin/hipo4utils -test out.hipo'%self.cfg['coatjava']
        cmd+=' || rm -f out.hipo && ls out.hipo'
        job.setCmd(cmd)

        self.addJob(job)
        inputs=[]

    return merged

  #
  # delete:  add jobs to delete files from disk
  # - one job per 200 files
  #
  def delete(self,phase,deletes):
    files = list(deletes)
    while len(files)>0:
      deletes=[]
      while len(deletes)<200 and len(files)>0:
        deletes.append(files.pop(0))
      if len(deletes)>0:
        runno = RunFile(deletes[0]).runNumber
        job=SwifJob(self.name)
        job.setPhase(phase)
        job.setRam('1GB')
        job.setTime('%ds'%(60+3*len(deletes)))
        job.setDisk('100MB')
        job.addTag('run','%.5d'%runno)
        f1=RunFile(deletes[0]).fileNumber
        f2=RunFile(deletes[len(deletes)-1]).fileNumber
        job.addTag('file','%.5d-%.5d'%(f1,f2))
        job.addTag('mode','delete')
        cmds = [ '(sleep 1 ; rm -f %s)'%delete for delete in deletes ]
        job.setCmd(' ; '.join(cmds))
        self.addJob(job)

  #
  # move:  add jobs to move files to final destination
  # - one job per 200 files
  #
  def move(self,phase,moves):
    files = list(moves)
    while len(files)>0:
      moves=[]
      while len(moves)<200 and len(files)>0:
        moves.append(files.pop(0))
      if len(moves)>0:
        runno = RunFile(moves[0]).runNumber
        job=SwifJob(self.name)
        job.setPhase(phase)
        job.setRam('1GB')
        job.setTime('%ds'%(60+3*len(moves)))
        job.setDisk('100MB')
        job.addTag('run','%.5d'%runno)
        job.addTag('mode','move')
        job.addTag('outDir',self.cfg['outDir'])
        cmd = '(sleep 1 ; set d=%s ; touch $d ; mv -f $d %s/%s)'
        cmds = [ cmd%(move,self.cfg['outDir'],str(runno)) for move in moves ]
        job.setCmd(' ; '.join(cmds))
        self.addJob(job)

#
# ThreePhaseDecoding
#
# Requires N GB of temporary disk space
#
# 0 = decode N
# 1 = merge N
# 2 = delete+move N
# 3 ...
#
class ThreePhaseDecoding(CLAS12DecodingWorkflow):

  def __init__(self,name,cfg):
    CLAS12DecodingWorkflow.__init__(self,name,cfg)

  def generate(self):

    self.jobs=[]
    phase=0
    for evioFiles in self.getGroups():

      phase += 1
      hipoFiles = self.decode(phase,evioFiles)

      phase += 1
      mergedFiles = self.merge(phase,hipoFiles)

      phase += 1
      self.move(phase,mergedFiles)
      self.delete(phase,hipoFiles)

#
# DecodingReconTest
#
# WARNING:  Does NOT delete anything!
#
# 0 = decode
# 1 = merge
# 2 = recon
# 4 ...
#
class DecodingReconTest(CLAS12DecodingWorkflow):

  def __init__(self,name,cfg):
    CLAS12DecodingWorkflow.__init__(self,name,cfg)

  def generate(self):

    self.jobs=[]
    phase=0
    for evioFiles in self.getGroups():

      phase += 1
      hipoFiles = self.decode(phase,evioFiles)

      phase += 1
      mergedFiles = self.merge(phase,hipoFiles)

      phase += 1
      self.recon(phase,hipoFiles)
      self.recon(phase,mergedFiles)

#
# RollingDecoding
#
# Requires 3*N GB of temporary disk space
#
# 0 = decode N0
# 1 = decode N1, merge N0
# 2 = decode N2, merge N1, delete+move N0
# 3 = decode N3, merge N2, delete+move N1
# ...
# i   = decode N(i), merge N(i-1), delete+move N(i-2),
# i+1 = merge N(i), delete+move N(i-1)
# i+2 = delete+move N(i)
#
class RollingDecoding(CLAS12DecodingWorkflow):

  def __init__(self,name,cfg):
    CLAS12DecodingWorkflow.__init__(self,name,cfg)

  def generate(self):

    self.jobs=[]
    phase=0

    decodeQueue=self.getGroups()
    mergeQueue,deleteQueue,moveQueue=[],[],[]

    while True:

      phase += 1

      if len(deleteQueue)>0:
        self.delete(phase,deleteQueue.pop(0))
        self.move(phase,moveQueue.pop(0))

      if len(mergeQueue)>0:
        hipoFiles = mergeQueue.pop(0)
        mergedFiles = self.merge(phase,hipoFiles)
        deleteQueue.append(hipoFiles)
        moveQueue.append(mergedFiles)

      if len(decodeQueue)>0:
        hipoFiles = self.decode(phase,decodeQueue.pop(0))
        mergeQueue.append(hipoFiles)

      if len(decodeQueue)==0 and len(mergeQueue)==0 and len(deleteQueue)==0:
        break

#
# SinglesOnlyDecoding
#
# Requires at least N GB of temporary disk space.
#
# Just writes decoded HIPO files to disk, no merging, and requires an
# independent (cron) task to merge and move (and maintain N GB).
#
class SinglesOnlyDecoding(CLAS12DecodingWorkflow):

  def __init__(self,name,cfg):
    CLAS12DecodingWorkflow.__init__(self,name,cfg)

  def generate(self):

    self.jobs=[]
    phase=0

    for evioFiles in self.getGroups():
      phase += 1
      self.decode(phase,evioFiles)


if __name__ == '__main__':
  import sys
  from ChefConfig import getConfig
  cli,cfg = getConfig(sys.argv[1:])
  cfg['outDir']=os.getenv('HOME')+'/tmp/clas12-workflow/outDir'
  cfg['workDir']=os.getenv('HOME')+'/tmp/clas12-workflow/workDir'
  workflow = RollingDecoding('test',cfg)
  workflow.setPhaseSize(1000)
  workflow.addRun(4013)
  workflow.addFiles(open('/home/baltzell/clas12/rga-spring.list','r').readlines())
  workflow.generate()
  print workflow.getShell()
  print workflow.getJson()

