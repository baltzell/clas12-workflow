import logging,os,re,json,sys

import ChefUtil,ChefConfig
from RunFileUtil import RunFile
from SwifJob import SwifJob

_LOGGER=logging.getLogger(__name__)

class CLAS12Job(SwifJob):

  def __init__(self,workflow,cfg):
    SwifJob.__init__(self,workflow)
    self.abbreviations={'decode':'d','recon':'r','clean':'c','merge':'m','mrg':'m','ana':'a'}
    self.addEnv('CCDB_CONNECTION','mysql://clas12reader@clasdb-farm.jlab.org/clas12')
    self.addEnv('RCDB_CONNECTION','mysql://rcdb@clasdb-farm.jlab.org/rcdb')
    self.addEnv('MALLOC_ARENA_MAX','2')
    self.project=cfg['project']
    self.os=cfg['node']
    self.cfg=cfg

  def setRun(self,run):
    self.addTag('run','%.6d'%int(run))

  def getRun(sef):
    return int(self.getTag('run'))

  def addInputData(self,filename,auger=True):
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
        if self.cfg != ChefConfig.ChefConfig(json.load(f)):
          _LOGGER.critical('Configuration conflicts with '+cfgfile)
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

