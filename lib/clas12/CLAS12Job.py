import logging,os,re,json,sys

import ChefUtil
from RunFileUtil import RunFile
from SwifJob import SwifJob

_LOGGER=logging.getLogger(__name__)

class CLAS12Job(SwifJob):

  def __init__(self,workflow,cfg):
    SwifJob.__init__(self,workflow)
    self.abbreviations.update({'decode':'d','dec':'d','recon':'r','clean':'c','merge':'m','mrg':'m','ana':'a'})
    self.addEnv('CLARA_HOME',cfg['clara'])
    if cfg['graalvm']:
      self.addEnv('JAVA_HOME','/group/clas12/packages/graalvm/22.2.0_java11')
    else:
      self.addEnv('JAVA_HOME','${CLARA_HOME}/jre/linux-64/jre')
    self.addEnv('PATH','${JAVA_HOME}/bin:${PATH}')
    if cfg['ccdbsqlite'] is None:
      self.addEnv('CCDB_CONNECTION','mysql://clas12reader@clasdb-farm.jlab.org/clas12')
    elif cfg['ccdbsqlite'].startswith('/cvmfs'):
      # SWIF2 only allows staging from /volatile and /cache
      # So here we drop the staging for now:
      self.addEnv('CCDB_CONNECTION','sqlite:///'+cfg['ccdbsqlite'])
    else:
      self.addEnv('CCDB_CONNECTION','sqlite:///clas12.sqlite')
      self.addInput('clas12.sqlite',cfg['ccdbsqlite'])
    self.addEnv('RCDB_CONNECTION','mysql://rcdb@clasdb-farm.jlab.org/rcdb')
    self.addEnv('MALLOC_ARENA_MAX','2')
    self.account=cfg['project']
    self.os=cfg['node']
    self.cfg=cfg

  def setRun(self,run):
    self.addTag('run','%.6d'%int(run))

  def getRun(sef):
    return int(self.getTag('run'))

  def addInputData(self,filename,auger=True):
    if isinstance(filename,list):
      for x in filename: self.addInputData(x,auger)
    else:
      basename=filename.split('/').pop()
      self.inputData.append(filename)
      if auger: self.addInput(basename,filename)
      runno=RunFile(filename).runNumber
      fileno=RunFile(filename).fileNumber
      self.setRun(runno)
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
    cfgfile=cfgdir+'/README.json'
    if os.path.isfile(cfgfile):
      # check for conflict with pre-existing config file:
      with open(cfgfile,'r') as f:
        from ChefConfig import ChefConfig
        diff = self.cfg.diff(ChefConfig(json.load(f)))
        if len(diff) != 0:
          _LOGGER.critical('Configuration conflicts with '+cfgfile)
          _LOGGER.critical('Conflicts on:  '+','.join(diff))
          sys.exit()
    elif os.access(cfgdir,os.W_OK):
      # write new config file:
      with open(cfgfile,'w') as f:
        f.write(self.cfg.getReadme())
        f.close()

  def addOutputData(self,basename,directory,tag=None,auger=True):
    ChefUtil.mkdir(directory,tag)
    self.doReadme(directory)
    self.addTag('outDir',directory)
    self.outputData.append(directory+'/'+basename)
    if auger: self.addOutput(basename,directory+'/'+basename)

  def setRequests(self,diskbytes,hours):
    gb = int((diskbytes/1e9)+1)
    hr = int(hours+1)
    if hr > 72:
      _LOGGER.critical('Huge time requirement (%s hours), need more threads and/or fewer files.'%str(hr))
      sys.exit(2)
    if gb > 120:
      _LOGGER.critical('Huge disk requirement (%s GB), need smaller --trainSize.'%str(gb))
      sys.exit(2)
    if hr < 24:
      hr = 24
    if gb < 10:
      gb = 10
    self.setTime( '%dh'  % hr)
    self.setDisk( '%dGB' % gb)

