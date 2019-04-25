import os
from SwifJob import SwifJob
from SwifWorkflow import SwifWorkflow
from RcdbManager import RcdbManager
from RunFileUtil import RunFile
import ChefUtil

class CLAS12Workflow(SwifWorkflow):

  def __init__(self,name,cfg):
    SwifWorkflow.__init__(self,name)
    self.rcdb=RcdbManager()
    self.cfg=cfg
    self.setPhaseSize(self.cfg['phaseSize'])
    self.setCombineRuns(self.cfg['multiRun'])
    self.logDir='%s/logs/%s'%(self.cfg['outDir'],self.name)
    self.addRuns(self.cfg['runs'])
    self.findFiles(self.cfg['inputs'])
    self._mkdirs()

  def _mkdirs(self):
    ChefUtil.mkdir(self.logDir)
    for run in self.getRunList():
      ChefUtil.mkdir('%s/%.6d'%(self.cfg['outDir'],run))
      if self.cfg['workDir'] is not None:
        ChefUtil.mkdir('%s/singles/%.6d'%(self.cfg['workDir'],run))
        ChefUtil.mkdir('%s/merged/%.6d'%(self.cfg['workDir'],run))

  def addJob(self,job):
    job.setLogDir(self.logDir)
    SwifWorkflow.addJob(self,job)

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

      if nFiles>1:
        outDir = '%s/recon/%s/%.6d'%(self.cfg['workDir'],subDir,runno)
      else:
        outDir = '%s/recon/%.6d/'%(self.cfg['outDir'],runno)

      reconFileName = outDir+'/'+reconBaseName
      ChefUtil.mkdir(outDir)

      job=SwifJob(self.name)
      job.setPhase(phase)
      job.setRam('4000MB')
      job.setTime('%dh'%(12*nFiles))
      job.setDisk('%dGB'%(4*nFiles))
      job.addTag('run','%.6d'%runno)
      job.addTag('file','%.5d'%fileno)
      job.addTag('mode','recon')
      job.addTag('coatjava',self.cfg['coatjava'])
      job.addTag('outDir',outDir)
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

      if self.cfg['workDir'] is None:
        outDir = '%s/%.6d/'%(self.cfg['outDir'],runno)
      else:
        outDir = '%s/singles/%.6d/'%(self.cfg['workDir'],runno)

      hipoBaseName = self.cfg['singlePattern']%(runno,fileno)
      hipoFileName = outDir + hipoBaseName
      hipoFiles.append(hipoFileName)

      job=SwifJob(self.name)
      job.setPhase(phase)
      job.addTag('run','%.6d'%runno)
      job.addTag('file','%.5d'%fileno)
      job.addTag('mode','decode')
      job.addTag('coatjava',self.cfg['coatjava'])
      job.addTag('outDir',outDir)
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
        outDir='%s/merged/%.6d/'%(self.cfg['workDir'],runno)
        outFile=outDir+self.cfg['mergePattern']%(runno,fileno1,fileno2)
        merged.append(outFile)

        job=SwifJob(self.name)
        job.setPhase(phase)
# Note this RAM request is for PBS, on SLURM will be much lower
        job.setRam('9GB')
        job.setTime(ChefUtil.getMergeTimeReq(self.cfg['mergeSize']))
        job.setDisk(ChefUtil.getMergeDiskReq(self.cfg['mergeSize']))
        job.addTag('run','%.6d'%runno)
        job.addTag('file','%.5d-%.5d'%(fileno1,fileno2))
        job.addTag('mode','merge')
        job.addTag('coatjava',self.cfg['coatjava'])
        job.addTag('outDir',outDir)
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
        job.addTag('run','%.6d'%runno)
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
        job.addTag('run','%.6d'%runno)
        job.addTag('mode','move')
        job.addTag('outDir',self.cfg['outDir'])
        cmd = '(sleep 1 ; set d=%s ; touch -c $d ; mv -f $d %s/%.6d)'
        cmds = [ cmd%(move,self.cfg['outDir'],runno) for move in moves ]
        job.setCmd(' ; '.join(cmds)+' ; true)
        self.addJob(job)

