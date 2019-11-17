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
    self.cfg=cfg
  def addInputData(self,filename):
    basename=filename.split('/').pop()
    self.inputData.append(filename)
    self.addInput(basename,filename)
    runno=RunFile(filename).runNumber
    fileno=RunFile(filename).fileNumber
    self.addTag('run','%.6d'%runno)
    if self.getTag('file') is None:
      self.addTag('file','%.5d'%fileno)
  def addOutputData(self,basename,directory,tag=None):
    ChefUtil.mkdir(directory,tag)
    self.addTag('outDir',directory)
    self.outputData.append(directory+'/'+basename)
    self.addOutput(basename,directory+'/'+basename)

class MergingJob(Job):
  def __init__(self,workflow,cfg):
    Job.__init__(self,workflow,cfg)
    self.setRam('1GB')
    self.setTime(ChefUtil.getMergeTimeReq(cfg['mergeSize']))
    self.setDisk(ChefUtil.getMergeDiskReq(cfg['mergeSize']))
    self.addTag('coatjava',cfg['coatjava'])
    self.addTag('mode','merge')
  def addInputData(self,filenames):
    runno = RunFile(filenames[0]).runNumber
    fileno1 = RunFile(filenames[0]).fileNumber
    fileno2 = RunFile(filenames[len(filenames)-1]).fileNumber
    outBasename=self.cfg['mergePattern']%(runno,fileno1,fileno2)
    outDir='%s/merged/%.6d/'%(self.cfg['workDir'],runno)
    self.addOutputData(outBasename,outDir,'staging')
    cmd=' set o=%s ; rm -f $o ; '%outBasename
    cmd+='%s/bin/hipo-utils -merge -o $o'%self.cfg['coatjava']
    for ii in range(len(filenames)):
      Job.addInputData(self,filenames[ii])
      basename=filenames[ii].split('/').pop()
      cmd+=' '+basename
    cmd+=' && ls $o && if (`stat -c%s $o` < 100) rm -f $o'
    cmd+=' && %s/bin/hipo-utils -test $o'%self.cfg['coatjava']
    cmd+=' || rm -f $o ; ls $o'
    Job.setCmd(self,cmd)

class DecodeAndMergeJob(Job):
  def __init__(self,workflow,cfg):
    Job.__init__(self,workflow,cfg)
    self.setRam('3GB')
    self.addTag('mode','decmrg')
    self.addTag('coatjava',cfg['coatjava'])
  def addInputData(self,eviofiles):
    # FIXME:  this assume 2 GB EVIO file
    self.setDisk('%.0fMB'%(1000*(2.1*(1+0.3)*len(eviofiles)+1)))
    self.setTime('%.0fh'%(len(eviofiles)))
    decodedfiles=[]
    for eviofile in eviofiles:
      Job.addInputData(self,eviofile)
#      print self.getTag('file')
      basename=self.cfg['singlePattern']%(int(self.getTag('run')),int(self.getTag('file')))
      decodedfiles.append(basename)
    runno = RunFile(eviofiles[0]).runNumber
    fileno1 = RunFile(eviofiles[0]).fileNumber
    fileno2 = RunFile(eviofiles[len(eviofiles)-1]).fileNumber
    mergedfile=self.cfg['mergePattern']%(runno,fileno1,fileno2)
    outDir='%s/%.6d/'%(self.cfg['decDir'],runno)
    self.addOutputData(mergedfile,outDir)
    # decode:
    decoderOpts = ChefUtil.getDecoderOpts(runno,self.cfg)
    cmd='true'
    for decodedfile,eviofile in zip(decodedfiles,eviofiles):
      cmd+=' && (set o=%s && set i=%s'%(decodedfile,os.path.basename(eviofile))
      cmd+=' && %s/bin/decoder %s -o $o $i'%(self.cfg['coatjava'],decoderOpts)
      cmd+=' && ls $o && if (`stat -c%s $o` < 100) rm -f $o'
      cmd+=' && %s/bin/hipo-utils -test $o'%self.cfg['coatjava']
      cmd+=' || rm -f $o && ls $o )'
    # merge:
    cmd+=' && set o=%s && rm -f $o && '%mergedfile
    cmd+='%s/bin/hipo-utils -merge -o $o %s'%(self.cfg['coatjava'],' '.join(decodedfiles))
    cmd+=' && ls $o && if (`stat -c%s $o` < 100) rm -f $o'
    cmd+=' && %s/bin/hipo-utils -test $o'%self.cfg['coatjava']
    cmd+=' || rm -f $o && ls $o'
    Job.setCmd(self,cmd)

class DecodingJob(Job):
  def __init__(self,workflow,cfg):
    Job.__init__(self,workflow,cfg)
    self.setRam('3GB')
    self.addTag('mode','decode')
    self.addTag('coatjava',cfg['coatjava'])
  def addInputData(self,filename):
    Job.addInputData(self,filename)
    basename=self.cfg['singlePattern']%(int(self.getTag('run')),int(self.getTag('file')))
    if self.cfg['workDir'] is None:
      outDir = '%s/%.6d/'%(self.cfg['decDir'],int(self.getTag('run')))
      Job.addOutputData(self,basename,outDir)
    else:
      outDir = '%s/singles/%.6d/'%(self.cfg['workDir'],int(self.getTag('run')))
      Job.addOutputData(self,basename,outDir,'staging')
  def setCmd(self):
    ChefUtil.getDecoderOpts(self.getTag('run'),self.cfg)
    cmd =' set o=%s ; set i=%s'%(os.path.basename(self.outputData[0]),os.path.basename(self.inputData[0]))
    cmd+=' ; %s/bin/decoder %s -o $o $i'%(self.cfg['coatjava'],decoderOpts)
    cmd+=' && ls $o && if (`stat -c%s $o` < 100) rm -f $o'
    cmd+=' && %s/bin/hipo-utils -test $o'%self.cfg['coatjava']
    cmd+=' || rm -f $o ; ls $o'
    Job.setCmd(self,cmd)

class ClaraJob(Job):
  THRD_MEM_REQ={0:0,   16:12, 24:16, 32:16}
  THRD_MEM_LIM={0:256, 16:10, 24:14, 32:14}
  def __init__(self,workflow,cfg):
    Job.__init__(self,workflow,cfg)
    self.addEnv('CLARA_HOME',cfg['clara'])
    self.addEnv('JAVA_OPTS','-Xmx%dg -Xms8g'%ClaraJob.THRD_MEM_LIM[cfg['threads']])
    self.setRam(str(ClaraJob.THRD_MEM_REQ[cfg['threads']])+'GB')
    self.setCores(self.cfg['threads'])
    self.addTag('mode','recon')
    # TODO: choose time based on #events:
    self.setTime('24h')
    # TODO: choose disk based on #events:
    self.setDisk('20GB')
    self.addInput('clara.sh',os.path.dirname(os.path.realpath(__file__))+'/../scripts/clara.sh')
    self.addInput('clara.yaml',cfg['reconYaml'])
  def addInputData(self,filename):
    Job.addInputData(self,filename)
    basename=filename.split('/').pop()
    outDir='%s/recon/%s/'%(self.cfg['outDir'],self.getTag('run'))
    Job.addOutputData(self,'rec_'+basename,outDir)
  def setCmd(self,hack):
    cmd = './clara.sh -t '+str(self.getCores())
    if self.cfg['claraLogDir'] is not None:
      cmd += ' -l '+self.cfg['claraLogDir']+' '
    cmd += ' '+self.getJobName().replace('--00001','-%.5d'%hack)
    Job.setCmd(self,cmd)

class TrainJob(Job):
  def __init__(self,workflow,cfg):
    Job.__init__(self,workflow,cfg)
    self.addEnv('CLARA_HOME',cfg['clara'])
    self.addEnv('JAVA_OPTS','-Xmx8g -Xms6g')
    self.setRam('10GB')
    self.setCores(12)
    self.addTag('mode','ana')
    # TODO: choose time based on #events:
    self.setTime('24h')
    self.addInput('train.sh',os.path.dirname(os.path.realpath(__file__))+'/../scripts/train.sh')
    self.addInput('clara.yaml',cfg['trainYaml'])
  def addInputData(self,filenames):
    self.setDisk(ChefUtil.getTrainDiskReq(filenames))
    for x in filenames:
      Job.addInputData(self,x)
    outDir='%s/train/%s/'%(self.cfg['outDir'],self.getTag('run'))
    for x in filenames:
      basename=os.path.basename(x)
      for y in ChefUtil.getTrainIndices(self.cfg['trainYaml']):
        Job.addOutputData(self,'skim%d_%s'%(y,basename),outDir)
  def setCmd(self,hack):
    cmd = './train.sh -t 12 '
    if self.cfg['claraLogDir'] is not None:
      cmd += ' -l '+self.cfg['claraLogDir']+' '
    cmd += ' '+self.getJobName().replace('--00001','-%.5d'%hack)
    cmd += ' && ls -lhtr'
    Job.setCmd(self,cmd)

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

