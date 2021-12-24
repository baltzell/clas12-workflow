import re,os,sys,logging,getpass

import RunFileUtil
import ChefUtil
from SwifJob import SwifJob

_LOGGER=logging.getLogger(__name__)

class HPSJob(SwifJob):
  def __init__(self,workflow,cfg):
    SwifJob.__init__(self,workflow.name)
    self.cfg = cfg
    self.copyInputs = False
    self.project = 'hps'
    self.shell = '/bin/bash'
    if self.cfg['logDir'] is None:
      self.setLogDir('/farm_out/'+getpass.getuser()+'/'+workflow.name)
    else:
      self.setLogDir(self.cfg['logDir']+'/'+workflow.name)
    ChefUtil.mkdir(self.logDir)
    if 'java' in self.cfg and self.cfg['java'] is not None:
      self.addEnv('JAVA_HOME',self.cfg['java'])
      self.addEnv('PATH',self.cfg['java']+'/bin:${PATH}')
  def addOutput(self,local,remote):
    if remote.endswith('.lcio'):
      remote = remote[0:-5] + '.slcio'
    SwifJob.addOutput(self,local,remote)
    ChefUtil.mkdir(os.path.dirname(remote))
  def getRun(self,filename):
    ret = self.cfg['runno']
    if ret is None or ret<0:
      ret = RunFileUtil.RunFile(filename).runNumber
      if ret is None:
        _LOGGER.critical('Cannot determine run number from file name, must be specified by --runno')
        sys.exit(1)
    return ret

class EvioToLcioJob(HPSJob):
  def __init__(self,workflow,cfg):
    HPSJob.__init__(self,workflow,cfg)
    self.setRam('1300MB')
    self.setDisk('30GB')
    self.setTime('24h')
    if self.cfg['hours'] is not None:
      self.setTime('%dh'%self.cfg['hours'])
  def setCmd(self):
    inBasename = self.inputs[0]['local']
    runno = self.getRun(inBasename)
    cmd = 'set echo ; ls -lhtr ;'
    cmd += ' java -Xmx896m -Xms512m -cp %s org.hps.evio.EvioToLcio'%self.cfg['jar']
    steer = '-x %s'%self.cfg['steer']
    if not self.cfg['steerIsFile']:
      steer += ' -r'
    cmd += ' %s -d %s -e 1000 -DoutputFile=out %s'%(steer,self.cfg['detector'],inBasename)
    cmd += ' || rm -f %s %s && false' %(inBasename,'out.slcio')
    outPath = '%s/%.6d/%s%s.slcio'%(self.cfg['outDir'],runno,self.cfg['outPrefix'],inBasename)
    self.addOutput('out.slcio',outPath)
    self.addTag('run','%.6d'%runno)
    SwifJob.setCmd(self,cmd)

class HpsJavaJob(HPSJob):
  def __init__(self,workflow,cfg):
    HPSJob.__init__(self,workflow,cfg)
    self.setRam('1GB')
    self.setDisk('10GB')
    self.setTime('2h')
    if self.cfg['hours'] is not None:
      self.setTime('%dh'%self.cfg['hours'])
  def setCmd(self):
    inBasename = self.inputs[0]['local']
    runno = self.getRun(inBasename)
    cmd = 'set echo ; ls -lhtr ;'
    cmd += ' java -Xmx896m -Xms512m -jar %s %s'%(self.cfg['jar'],self.cfg['steer'])
    if self.cfg['runno'] is not None:
      cmd += ' -R %d'%runno
    if self.cfg['detector'] is not None:
      cmd += ' -d '+self.cfg['detector']
    if not self.cfg['steerIsFile']:
      cmd += ' -r'
    cmd += ' -i %s -DoutputFile=out'%(inBasename)
    cmd += ' || rm -f %s %s && false' %(inBasename,'out.slcio')
    outPath = '%s/%.6d/%s%s'%(self.cfg['outDir'],runno,self.cfg['outPrefix'],inBasename)
    if not self.cfg['noLCIO']:
      self.addOutput('out.slcio',outPath)
    for x in self.cfg['outFile']:
      sufflen = len(outPath.split('.').pop())
      self.addOutput(x,outPath[:-sufflen]+x.split('.').pop())
    self.addTag('run','%.6d'%runno)
    SwifJob.setCmd(self,cmd)

#
# 300K evio files in physrun2019 --> 900 TB
# FEE skims are about 30 MB per evio file --> 9 TB
# MULT skims are much smaller
# MUON is about 15% --> 30 TB
# FCUP is 5% 
#
class EvioTriggerFilterJob(HPSJob):
  def __init__(self,workflow,cfg):
    HPSJob.__init__(self,workflow,cfg)
    # FIXME:  software from Maurk's personal location
    self.exe='/home/holtrop/bin/HPS_Trigger_Filter'
    # 2019 version:
    #self.addEnv('LD_LIBRARY_PATH','/home/holtrop/lib:/apps/root/6.12.06/lib')
    # 2021 version:
    self.addEnv('LD_LIBRARY_PATH','/apps/gcc/9.3.0/lib:/apps/gcc/9.3.0/lib64:/home/holtrop/root/lib/root:/home/holtrop/lib:/apps/python/3.4.3/lib:/home/hps/lib')
    self.setRam('500MB')
    self.setDisk('3GB')
    self.setTime('4h')
    if self.cfg['hours'] is not None:
      self.setTime('%dh'%self.cfg['hours'])
  def setCmd(self):
    rf1 = RunFileUtil.RunFile(self.inputs[0]['remote'])
    rf2 = RunFileUtil.RunFile(self.inputs[len(self.inputs)-1]['remote'])
    outfile = self.cfg['mergePattern']%(rf1.runNumber,rf1.fileNumber,rf2.fileNumber)
    outdir = self.cfg['outDir']
    r = rf1.runNumber
    # FIXME:  this doesn't have proper error checking on exit codes
    cmd = 'set echo ; ls -lhtr ; exe=%s ; '%self.exe
    triggers = {'rndm':'random','muon':'muon','fee':'FEE','mult2':16,'mult3':17,'moll':'moller_all'}
    for abb in self.cfg['trigger']:
      trigger = triggers[abb]
      basename = outfile.replace('hps_','hps_%s_'%abb)
      cmd += '$exe -T %s -o out_%s.evio ./*.evio* ;'%(trigger,abb)
      cmd += '[ -e out_%s.evio ] && /site/bin/swif outfile out_%s.evio file:%s;' % (abb,abb,'%s/%s/%.6d/%s'%(outdir,abb,r,basename))
      self.outputData.append('%s/%s/%.6d/%s'%(outdir,abb,r,basename))
    cmd += 'ls -lhtr ; '
    self.addTag('run','%.6d'%rf1.runNumber)
    self.makeOutputDirs()
    SwifJob.setCmd(self,cmd)

class EvioMergeJob(HPSJob):
  def __init__(self,workflow,cfg):
    HPSJob.__init__(self,workflow,cfg)
    self.exe='/home/holtrop/bin/evioconcat'
    self.addEnv('LD_LIBRARY_PATH','/apps/gcc/9.3.0/lib:/apps/gcc/9.3.0/lib64:/home/holtrop/root/lib/root:/home/holtrop/lib:/apps/python/3.4.3/lib:/home/hps/lib')
    self.setRam('500MB')
    self.setDisk('25GB')
    self.setTime('4h')
  def setCmd(self):
    rf1 = RunFileUtil.RunFile(self.inputs[0]['remote'])
    rf2 = RunFileUtil.RunFile(self.inputs[len(self.inputs)-1]['remote'])
    prefix = re.match('^(hps[_a-z]+)',self.inputs[0]['remote'].split('/').pop()).group(1)
    outfile = self.cfg['mergePattern']%(rf1.runNumber,rf1.fileNumber,rf2.fileNumber)
    cmd = 'set echo ; ls -lhtr ; %s -o out.evio ./*.evio* ; ls -lhtr'%self.exe
    self.addOutput('out.evio','%s/%.6d/%s'%(self.cfg['outDir'],rf1.runNumber,outfile.replace('hps_',prefix)))
    self.addTag('run','%.6d'%rf1.runNumber)
    SwifJob.setCmd(self,cmd)

