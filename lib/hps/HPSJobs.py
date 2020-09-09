import os,logging,getpass

import RunFileUtil
import ChefUtil
from SwifJob import SwifJob

class HPSJob(SwifJob):
  def __init__(self,workflow,cfg):
    SwifJob.__init__(self,workflow.name)
    self.cfg=cfg
    self.copyInputs=False
    self.project='hps'
    if self.cfg['logDir'] is None:
      self.setLogDir('/farm_out/'+getpass.getuser()+'/'+workflow.name)
    else:
      self.setLogDir(self.cfg['logDir']+'/'+workflow.name)
    ChefUtil.mkdir(self.logDir)
  def addOutput(self,local,remote):
    if remote.endswith('.lcio'):
      remote = remote[0:-5] + '.slcio'
    SwifJob.addOutput(self,local,remote)
    ChefUtil.mkdir(os.path.dirname(remote))
  def getRun(self,filename):
    ret = self.cfg['runno']
    if ret is None:
      ret = RunFileUtil.RunFile(filename).runNumber
    return ret

class EvioToLcioJob(HPSJob):
  def __init__(self,workflow,cfg):
    HPSJob.__init__(self,workflow,cfg)
    self.setTime('60h')
    self.setRam('1300MB')
    self.setDisk('30GB')
  def setCmd(self):
    inBasename = self.inputs[0]['local']
    runno = self.getRun(inBasename)
    if runno is None:
      logging.critical('Cannot determine run number from filename, and not provided by user.')
    cmd = 'set echo ; ls -lhtr ;'
    cmd = ' java -Xmx896m -Xms512m -cp %s org.hps.evio.EvioToLcio'%self.cfg['jar']
    cmd += ' -x %s -r -d %s -e 1000 -DoutputFile=out %s'%(self.cfg['steer'],self.cfg['detector'],inBasename)
    cmd += ' || rm -f %s %s && false' %(inBasename,'out.slcio')
    outPath = '%s/%.6d/%s%s.slcio'%(self.cfg['outDir'],runno,self.cfg['outPrefix'],inBasename)
    self.addOutput('out.slcio',outPath)
    self.addTag('run','%.6d'%runno)
    SwifJob.setCmd(self,cmd)

class HpsJavaJob(HPSJob):
  def __init__(self,workflow,cfg):
    HPSJob.__init__(self,workflow,cfg)
    self.setTime('2h')
    self.setRam('1GB')
    self.setDisk('10GB')
  def setCmd(self):
    inBasename = self.inputs[0]['local']
    runno = self.getRun(inBasename)
    cmd = 'set echo ; ls -lhtr ;'
    cmd = ' java -Xmx896m -Xms512m -jar %s %s'%(self.cfg['jar'],self.cfg['steer'])
    if runno is not None:
      cmd += ' -R %d'%runno
    if self.cfg['detector'] is not None:
      cmd += ' -d '+self.cfg['detector']
    cmd += ' -r -i %s -DoutputFile=out'%(inBasename)
    cmd += ' || rm -f %s %s && false' %(inBasename,'out.slcio')
    outPath = '%s/%.6d/%s%s'%(self.cfg['outDir'],runno,self.cfg['outPrefix'],inBasename)
    self.addOutput('out.slcio',outPath)
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
    self.setTime('4h')
    self.setRam('500MB')
    self.setDisk('10GB')
  def setCmd(self):
    rf1 = RunFileUtil.RunFile(self.inputs[0]['remote'])
    rf2 = RunFileUtil.RunFile(self.inputs[len(self.inputs)-1]['remote'])
    outfile = cfg['mergePattern']%(rf1.runNumber,rf1.fileNumber,rf2.fileNumber)
    cmd ='setenv LD_LIBRARY_PATH /home/holtrop/lib:/apps/root/6.12.06/lib ; '
    cmd+='set echo ; ls -lhtr ; '
    if 'fcup' in self.cfg['trigger']:
      cmd+='/home/holtrop/bin/HPS_Trigger_Filter -T fcup -o out_fcup.evio ./*.evio* ;'
      job.addOutput('out_fcup.evio',  '%s/fcup/%.6d/%s'%(cfg['outDir'],rf1.runNumber,outfile.replace('hps_','hps_fcup_')))
    if 'muon' in self.cfg['trigger']:
      cmd+='/home/holtrop/bin/HPS_Trigger_Filter -T muon -E -o out_muon.evio ./*.evio* ;'
      job.addOutput('out_muon.evio',  '%s/muon/%.6d/%s'%(cfg['outDir'],rf1.runNumber,outfile.replace('hps_','hps_muon_')))
    if 'fee' in self.cfg['trigger']:
      cmd+='/home/holtrop/bin/HPS_Trigger_Filter -T FEE -o out_fee.evio ./*.evio* ;'
      job.addOutput('out_fee.evio',  '%s/fee/%.6d/%s'%(cfg['outDir'],rf1.runNumber,outfile.replace('hps_','hps_fee_')))
    if 'mult2' in self.cfg['trigger']:
      cmd+='/home/holtrop/bin/HPS_Trigger_Filter -T 16 -o out_mult2.evio ./*.evio* ;'
      job.addOutput('out_mult2.evio','%s/mult2/%.6d/%s'%(cfg['outDir'],rf1.runNumber,outfile.replace('hps_','hps_mult2_')))
    if 'mult3' in self.cfg['trigger']:
      cmd+='/home/holtrop/bin/HPS_Trigger_Filter -T 17 -o out_mult3.evio ./*.evio* ;'
      job.addOutput('out_mult3.evio','%s/mult3/%.6d/%s'%(cfg['outDir'],rf1.runNumber,outfile.replace('hps_','hps_mult3_')))
    cmd+='ls -lhtr ; '
    SwifJob.setCmd(self,cmd)
    job.addTag('run','%.6d'%rf1.runNumber)

