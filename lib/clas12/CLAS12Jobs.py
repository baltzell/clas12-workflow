import os,re,sys,json,logging

import ChefUtil,ChefConfig
from RunFileUtil import RunFile
from SwifJob import SwifJob

_LOGGER=logging.getLogger(__name__)

class Job(SwifJob):
  def __init__(self,workflow,cfg):
    SwifJob.__init__(self,workflow)
    self.addEnv('CCDB_CONNECTION','mysql://clas12reader@clasdb-farm.jlab.org/clas12')
    self.addEnv('RCDB_CONNECTION','mysql://rcdb@clasdb-farm.jlab.org/rcdb')
    self.addEnv('MALLOC_ARENA_MAX','2')
    self.os=cfg['node']
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
  def doReadme(self,directory):
    # put it on /cache if it's /mss:
    if directory.startswith('/mss/'):
      directory=directory.replace('/mss/','/cache/',1)
    # if the last dir is just a number, go up one:
    cfgdir=directory.strip('/').split('/')
    if len(cfgdir)<1: return
    if re.match('^\d+$',cfgdir[len(cfgdir)-1]) is not None: cfgdir.pop()
    cfgdir='/'+('/'.join(cfgdir))
    cfgfile=cfgdir+'/REAMDE.json'
    if os.path.isfile(cfgfile):
      # check for conflict with pre-existing config file:
      with open(cfgfile,'r') as f:
        if self.cfg != ChefConfig.ChefConfig(json.load(f)):
          _LOGGER.critical('Configuration conflicts with '+cfgfile)
          sys.exit()
    elif os.access(cfgdir,os.W_OK):
      # write new config file:
      with open(cfgfile,'w') as f:
        f.write(self.cfg.getReadme())
        f.close()
  def addOutputData(self,basename,directory,tag=None):
    ChefUtil.mkdir(directory,tag)
    self.doReadme(directory)
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
    self.setDisk('%.0fGB'%(int(ChefUtil.DEFAULT_EVIO_BYTES*1.4)/1e9*len(eviofiles)+1))
    self.setTime('%.0fh'%(len(eviofiles)))
    decodedfiles=[]
    for eviofile in eviofiles:
      Job.addInputData(self,eviofile)
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
    decoderOpts=ChefUtil.getDecoderOpts(self.getTag('run'),self.cfg)
    cmd =' set o=%s ; set i=%s'%(os.path.basename(self.outputData[0]),os.path.basename(self.inputData[0]))
    cmd+=' ; %s/bin/decoder %s -o $o $i'%(self.cfg['coatjava'],decoderOpts)
    cmd+=' && ls $o && if (`stat -c%s $o` < 100) rm -f $o'
    cmd+=' && %s/bin/hipo-utils -test $o'%self.cfg['coatjava']
    cmd+=' || rm -f $o ; ls $o'
    Job.setCmd(self,cmd)

class ReconJob(Job):
  THRD_MEM_REQ={0:0,   16:12, 20:14, 24:16, 32:16}
  THRD_MEM_LIM={0:256, 16:10, 20:12, 24:14, 32:14}
  def __init__(self,workflow,cfg):
    Job.__init__(self,workflow,cfg)
    if cfg['postproc']:
      self.addEnv('COATJAVA',cfg['coatjava'])
    self.addEnv('CLARA_HOME',cfg['clara'])
    self.addEnv('JAVA_OPTS','-Xmx%dg -Xms8g'%ReconJob.THRD_MEM_LIM[cfg['threads']])
    self.setRam(str(ReconJob.THRD_MEM_REQ[cfg['threads']])+'GB')
    self.setCores(self.cfg['threads'])
    self.addTag('mode','recon')
    # TODO: choose time based on #events:
    self.setTime('24h')
    self.setDisk('20GB')
    self.addInput('clara.sh',os.path.dirname(os.path.realpath(__file__))+'/../scripts/clara.sh')
    self.addInput('clara.yaml',cfg['reconYaml'])
  def addInputData(self,filename):
    self.setDisk(ChefUtil.getReconDiskReq(self.cfg['reconYaml'],filename))
    Job.addInputData(self,filename)
    basename=filename.split('/').pop()
    outDir='%s/%s/recon/%s/'%(self.cfg['outDir'],self.cfg['schema'],self.getTag('run'))
    Job.addOutputData(self,'rec_'+basename,outDir)
  def setCmd(self,hack):
    cmd = './clara.sh -t '+str(self.getCores())
    if self.cfg['claraLogDir'] is not None:
      cmd += ' -l '+self.cfg['claraLogDir']+' '
    cmd += ' '+self.getJobName().replace('--00001','-%.5d'%hack)
    if self.cfg['postproc']:
      for x in self.outputData:
        x=os.path.basename(x)
        cmd += ' && ls -l && echo %s/bin/postprocess -d 1 -q 1 -o pp.hipo %s'%(self.cfg['coatjava'],x)
        cmd += ' && %s/bin/postprocess -d 1 -q 1 -o pp.hipo %s'%(self.cfg['coatjava'],x)
        cmd += ' && rm -f %s && mv -f pp.hipo %s'%(x,x)
        cmd += ' && %s/bin/hipo-utils -test %s || rm -f %s'%(self.cfg['coatjava'],x,x)
        cmd += ' && ls %s'%(x)
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
    self.setDisk(ChefUtil.getTrainDiskReq(self.cfg['reconYaml'],filenames))
    for x in filenames:
      Job.addInputData(self,x)
    outDir='%s/%s/train/%s/'%(self.cfg['outDir'],self.cfg['schema'],self.getTag('run'))
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

class TrainMrgJob(Job):
  def __init__(self,workflow,cfg):
    Job.__init__(self,workflow,cfg)
    self.addEnv('COATJAVA',cfg['coatjava'])
    # FIXME: use `module load`, but need to know what version or wait until stable
    lib=os.path.dirname(os.path.realpath(__file__)).rstrip('clas12')
    self.addEnv('PYTHONPATH',lib+'/util:'+lib+'/clas12')
    self.setRam('700MB')
    self.addTag('mode','anamrg')
    self.setTime('12h')
  def setRun(self,run):
    self.run=int(run)
  def setCmd(self):
    # FIXME: write outputs to local disk and use Auger staging
    cmd=os.path.dirname(os.path.realpath(__file__))+'/../../scripts/hipo-merge-trains.py'
    cmd+=' -i %s/%s/train/%.6d'%(self.cfg['outDir'],self.cfg['schema'],self.run)
    cmd+=' -o %s/%s/train'%(self.cfg['outDir'],self.cfg['schema'])
    Job.setCmd(self,cmd)

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
  print(wflow.getJson())

