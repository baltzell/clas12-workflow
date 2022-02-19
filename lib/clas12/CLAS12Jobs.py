import os,re,sys,logging

import ClaraYaml
import ChefUtil
import SwifJob
from RunFileUtil import RunFile
from CLAS12Job import CLAS12Job

_LOGGER=logging.getLogger(__name__)

_DEBUG=False

class JputJob(SwifJob.JputJob):
  def __init__(self,workflow,cfg):
    SwifJob.JputJob.__init__(self,workflow)
    self.project=cfg['project']
    self.os=cfg['node']
    self.cfg=cfg

class MergingJob(CLAS12Job):
  def __init__(self,workflow,cfg):
    CLAS12Job.__init__(self,workflow,cfg)
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
      CLAS12Job.addInputData(self,filenames[ii])
      basename=filenames[ii].split('/').pop()
      cmd+=' '+basename
    cmd+=' && ls $o && if (`stat -c%s $o` < 100) rm -f $o'
    cmd+=' && %s/bin/hipo-utils -test $o'%self.cfg['coatjava']
    cmd+=' || rm -f $o ; ls $o'
    CLAS12Job.setCmd(self,cmd)

class DecodeAndMergeJob(CLAS12Job):
  def __init__(self,workflow,cfg):
    CLAS12Job.__init__(self,workflow,cfg)
    self.setRam('3GB')
    self.addTag('mode','decmrg')
    self.addTag('coatjava',cfg['coatjava'])
  def addInputData(self,eviofiles):
    # FIXME:  this assume 2 GB EVIO file
    self.setDisk('%.0fGB'%(int(ChefUtil.DEFAULT_EVIO_BYTES*1.4)/1e9*len(eviofiles)+1))
    hours = len(eviofiles)+2
    if hours < 12:
      hours = 12
    self.setHours(hours)
    decodedfiles=[]
    for eviofile in eviofiles:
      CLAS12Job.addInputData(self,eviofile)
      runno=RunFile(eviofile).runNumber
      fileno=RunFile(eviofile).fileNumber
      basename=self.cfg['singlePattern']%(runno,fileno)
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
    if _DEBUG:
      cmd = cmd.replace('bin/decoder','bin/decoder -n 1000')
    CLAS12Job.setCmd(self,cmd)

class DecodingJob(CLAS12Job):
  def __init__(self,workflow,cfg):
    CLAS12Job.__init__(self,workflow,cfg)
    self.setRam('3GB')
    self.addTag('mode','decode')
    self.addTag('coatjava',cfg['coatjava'])
  def addInputData(self,filename):
    CLAS12Job.addInputData(self,filename)
    basename=self.cfg['singlePattern']%(int(self.getTag('run')),int(self.getTag('file')))
    if self.cfg['workDir'] is None or self.cfg['model'].find('decmrg')<0:
      outDir = '%s/%.6d/'%(self.cfg['decDir'],int(self.getTag('run')))
      CLAS12Job.addOutputData(self,basename,outDir)
    else:
      outDir = '%s/singles/%.6d/'%(self.cfg['workDir'],int(self.getTag('run')))
      CLAS12Job.addOutputData(self,basename,outDir,'staging')
  def setCmd(self):
    decoderOpts=ChefUtil.getDecoderOpts(self.getTag('run'),self.cfg)
    cmd =' set o=%s ; set i=%s'%(os.path.basename(self.outputData[0]),os.path.basename(self.inputData[0]))
    cmd+=' ; %s/bin/decoder %s -o $o $i'%(self.cfg['coatjava'],decoderOpts)
    cmd+=' && ls $o && if (`stat -c%s $o` < 100) rm -f $o'
    cmd+=' && %s/bin/hipo-utils -test $o'%self.cfg['coatjava']
    cmd+=' || rm -f $o ; ls $o'
    if _DEBUG:
      cmd = cmd.replace('bin/decoder','bin/decoder -n 1000')
    CLAS12Job.setCmd(self,cmd)

class ReconJob(CLAS12Job):
  THRD_MEM_REQ={0:0, 16:12, 20:16, 24:20, 32:28, 36:32, 40:36, 48:44}
  THRD_MEM_LIM={0:0, 16:10, 20:14, 24:18, 32:26, 36:30, 40:34, 48:42}
  HOURS_INC,BYTES_INC = None,None
  def __init__(self,workflow,cfg):
    CLAS12Job.__init__(self,workflow,cfg)
    self.addEnv('CLARA_HOME',cfg['clara'])
    # $COATJAVA has to be set for postprocessing to find bankdefs:
    if not cfg['nopostproc']:
      self.addEnv('COATJAVA',cfg['clara']+'/plugins/clas12')
    # only limit the memory for non-exclusive jobs:
    if ReconJob.THRD_MEM_LIM[cfg['threads']] > 0:
      self.addEnv('JAVA_OPTS','-Xmx%dg -Xms8g'%ReconJob.THRD_MEM_LIM[cfg['threads']])
    self.setRam(str(ReconJob.THRD_MEM_REQ[cfg['threads']])+'GB')
    self.setCores(self.cfg['threads'])
    self.addTag('mode','recon')
    self.setTime('24h')
    self.setDisk('20GB')
    self.nfiles = 0
  def setRequestIncrements(self,filename):
    if ReconJob.HOURS_INC is None:
      if self.cfg['threads'] == 0:
        ReconJob.HOURS_INC = ChefUtil.getReconSeconds(filename)/60/60/48
      else:
        ReconJob.HOURS_INC = ChefUtil.getReconSeconds(filename)/60/60/self.cfg['threads']
    tmp = ChefUtil.getReconFileBytes(self.cfg['reconYaml'],filename)
    tmp += ChefUtil.DEFAULT_DECODED_BYTES
    if ReconJob.BYTES_INC is None or tmp>ReconJob.BYTES_INC:
      ReconJob.BYTES_INC = tmp
  def addInputData(self,filename):
    CLAS12Job.addInputData(self,filename)
    basename=filename.split('/').pop()
    outDir='%s/%s/recon/%s/'%(self.cfg['outDir'],self.cfg['schema'],self.getTag('run'))
    CLAS12Job.addOutputData(self,'rec_'+basename,outDir)
    # here we choose request increment based on the first file:
    if self.cfg['hattawy'] or ReconJob.HOURS_INC is None or ReconJob.BYTES_INC is None:
      self.setRequestIncrements(filename)
    # and now update the resource requests when every file is added:
    self.nfiles += 1
    self.setRequests(ReconJob.BYTES_INC*self.nfiles,ReconJob.HOURS_INC*self.nfiles)
  def setCmd(self,hack):
    cmd = os.path.dirname(os.path.realpath(__file__))+'/scripts/clara.sh'
    cmd += ' -t %s -y %s'%(str(self.getCores()),self.cfg['reconYaml'])
    if _DEBUG:
      cmd += ' -n 5000'
    if self.cfg['claraLogDir'] is not None:
      cmd += ' -l '+self.cfg['claraLogDir']+' '
    cmd += ' '+self.getJobName().replace('--00001','-%.5d'%hack)
    if not self.cfg['nopostproc'] or self.cfg['recharge']:
      for x in self.outputData:
        x=os.path.basename(x)
        cmd += ' && set o='+x
        # postprocessing must run from the same coatjava as clara for bankdefs:
        if self.cfg['recharge']:
          cmd += ' && ( ls -l && $CLARA_HOME/plugins/clas12/bin/rebuild-scalers -o rs.hipo $o'
          cmd += ' && rm -f $o && mv -f rs.hipo $o'
          cmd += ' && $COATJAVA/bin/hipo-utils -test $o || rm -f $o'
          cmd += ' && ls $o )'
        if not self.cfg['nopostproc']:
          opts = '-q 1'
          if not self.cfg['noheldel']:
            opts += ' -d 1'
          if self.cfg['helflip']:
            opts += ' -f 1'
          cmd += ' && ( ls -l && $CLARA_HOME/plugins/clas12/bin/postprocess %s -o pp.hipo $o'%(opts)
          cmd += ' && rm -f $o && mv -f pp.hipo $o'
          cmd += ' && $COATJAVA/bin/hipo-utils -test $o || rm -f $o'
          cmd += ' && ls $o )'
    CLAS12Job.setCmd(self,cmd)

class TrainJob(CLAS12Job):
  HOURS_INC,BYTES_INC = None,None
  def __init__(self,workflow,cfg):
    CLAS12Job.__init__(self,workflow,cfg)
    self.addEnv('CLARA_HOME',cfg['clara'])
    self.addEnv('JAVA_OPTS','-Xmx8g -Xms6g')
    self.setRam('10GB')
    self.setCores(12)
    self.addTag('mode','ana')
    # TODO: choose time based on #events:
    self.setTime('24h')
    self.nfiles = 0
  def setRequestIncrements(self,filename):
    TrainJob.HOURS_INC = 0.5
    TrainJob.BYTES_INC = ChefUtil.getTrainDiskBytes(self.cfg['reconYaml'],filename)
  def addInputData(self,filenames):
    for x in filenames:
      if not x.endswith('.hipo'):
        _LOGGER.critical('Non-HIPO file detected for a train job: '+x)
        sys.exit(99)
      CLAS12Job.addInputData(self,x)
    if self.cfg['workDir'] is None:
      outDir=self.cfg['outDir']
    else:
      outDir=self.cfg['workDir']
    outDir='%s/%s/train/%s/'%(outDir,self.cfg['schema'],self.getTag('run'))
    for x in filenames:
      basename=os.path.basename(x)
      for y in ClaraYaml.getTrainIndices(self.cfg['trainYaml']):
        CLAS12Job.addOutputData(self,'skim%d_%s'%(y,basename),outDir)
    if TrainJob.HOURS_INC is None or TrainJob.BYTES_INC is None:
      self.setRequestIncrements(filenames[0])
    self.nfiles += len(filenames)
    self.setRequests(TrainJob.BYTES_INC*self.nfiles,TrainJob.HOURS_INC*self.nfiles)
  def setCmd(self,hack):
    cmd = os.path.dirname(os.path.realpath(__file__))+'/scripts/train.sh'
    cmd += ' -t 12 -y '+self.cfg['trainYaml']
    if self.cfg['claraLogDir'] is not None:
      cmd += ' -l '+self.cfg['claraLogDir']+' '
    cmd += ' '+self.getJobName().replace('--00001','-%.5d'%hack)
    cmd += ' && ls -lhtr'
    CLAS12Job.setCmd(self,cmd)

class TrainMrgJob(CLAS12Job):
  def __init__(self,workflow,cfg):
    CLAS12Job.__init__(self,workflow,cfg)
    self.addEnv('COATJAVA',cfg['coatjava'])
    # FIXME: use `module load`, but need to know what version or wait until stable
    lib=os.path.dirname(os.path.realpath(__file__)).rstrip('clas12')
    self.addEnv('PYTHONPATH',lib+'/util:'+lib+'/clas12:'+lib+'/ccdb')
    self.setRam('1000MB')
    self.addTag('mode','anamrg')
    self.setTime('36h')
  def setCmd(self):
    # FIXME: write outputs to local disk and use Auger staging
    if self.cfg['workDir'] is None:
      inDir = self.cfg['outDir']
    else:
      inDir = self.cfg['workDir']
    outDir = '%s/%s/train'%(self.cfg['trainDir'],self.cfg['schema'])
    self.addOutputData(outDir,outDir,auger=False)
    for trainName in list(ClaraYaml.getTrainNames(self.cfg['trainYaml']).values()):
      ChefUtil.mkdir(outDir+'/'+trainName)
    cmd = os.path.dirname(os.path.realpath(__file__))+'/../../scripts/hipo-merge-trains.py'
    cmd+=' -i %s/%s/train/%.6d'%(inDir,self.cfg['schema'],int(self.getTag('run')))
    cmd+=' -o '+outDir
    cmd+=' -y '+self.cfg['trainYaml']
    cmd+=' && ls -ltR %s && ls -lt %s'%(inDir,outDir)
    CLAS12Job.setCmd(self,cmd)

class TrainCleanupJob(CLAS12Job):
  def __init__(self,workflow,cfg):
    CLAS12Job.__init__(self,workflow,cfg)
    self.setRam('100MB')
    self.setTime('2h')
    self.addTag('mode','anaclean')
  def setCmd(self):
    if self.cfg['workDir'] is None:
      delDir = self.cfg['outDir']
    else:
      delDir = self.cfg['workDir']
    cmd='rm -rf %s/%s/train/%.6d'%(delDir,self.cfg['schema'],int(self.getTag('run')))
    CLAS12Job.setCmd(self,cmd)


if __name__ == '__main__':

  job=ReconJob('wflow')
  job.setTrack('debug')
  job.setCmd('./clara.sh -t %d -l /volatile/clas12/users/baltzell/clara-test/nostage %s'%(16,job.getJobName()))
  job.addInput('clara.yaml','/volatile/clas12/users/baltzell/clara-test/data.yaml')
  job.addInput('clas_006501.evio.00000.hipo','/cache/clas12/rg-b/production/decoded/6b.1.1/006501/clas_006501.evio.00000.hipo')
  job.addOutput('rec_clas_006501.evio.00000.hipo','/volatile/clas12/users/baltzell/clara-test/nostage')

  from SwifWorkflow import SwifWorkflow
  wflow=SwifWorkflow('wflow')
  wflow.addJob(job)
  print((wflow.getJson()))

