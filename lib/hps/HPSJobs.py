import os,re,sys,logging,getpass

import RunFileUtil
import ChefUtil
from SwifJob import SwifJob
from SwifWorkflow import SwifWorkflow

logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[ %(name)-15s ] %(message)s')
logger=logging.getLogger(__name__)

RunFileUtil.setFileRegex('.*hps[_A-Za-z]*_(\d+)\.evio\.(\d+)')

# 300K evio files in physrun2019
# FEE skims are about 30 MB per evio file --> 9 TB
# MULT skims are much smaller
class EvioTriggerFilterJob(SwifJob):
  def __init__(self,workflow,cfg):
    SwifJob.__init__(self,workflow)
    self.cfg=cfg
    self.copyInputs=False
    self.setTime('4h')
    self.setRam('500MB')
    self.setDisk('10GB')
    self.project='hallb-pro'
    self.setLogDir('/farm_out/'+getpass.getuser()+'/trigskim')
    ChefUtil.mkdir(self.logDir)
  def addOutput(self,local,remote):
    SwifJob.addOutput(self,local,remote)
    ChefUtil.mkdir(os.path.dirname(remote))
  def setCmd(self):
    cmd ='source /apps/root/6.12.06/bin/thisroot.csh ; '
    cmd+='setenv LD_LIBRARY_PATH ${LD_LIBRARY_PATH}:/home/holtrop/lib ; '
    cmd+='set echo ; ls -lhtr ; '
    cmd+='/home/holtrop/bin/HPS_Trigger_Filter -T FEE -o out_fee.evio ./*.evio* ;'
    cmd+='/home/holtrop/bin/HPS_Trigger_Filter -T 16 -o out_mult2.evio ./*.evio* ;'
    cmd+='/home/holtrop/bin/HPS_Trigger_Filter -T 17 -o out_mult3.evio ./*.evio* ;'
    cmd+='ls -lhtr ; '
    SwifJob.setCmd(self,cmd)
    rf1 = RunFileUtil.RunFile(self.inputs[0]['remote'])
    rf2 = RunFileUtil.RunFile(self.inputs[len(self.inputs)-1]['remote'])
    outfile = cfg['mergePattern']%(rf1.runNumber,rf1.fileNumber,rf2.fileNumber)
    job.addOutput('out_fee.evio',  '%s/fee/10:1/%.6d/%s'%(cfg['outDir'],rf1.runNumber,outfile.replace('hps_','hps_fee_')))
    job.addOutput('out_mult2.evio','%s/mult2/10:1/%.6d/%s'%(cfg['outDir'],rf1.runNumber,outfile.replace('hps_','hps_mult2_')))
    job.addOutput('out_mult3.evio','%s/mult3/10:1/%.6d/%s'%(cfg['outDir'],rf1.runNumber,outfile.replace('hps_','hps_mult3_')))

if __name__ == '__main__':

  # future command-line arguments, if we want to generalize this:
  cfg={}
  cfg['runs'] = '10004-10740'
  cfg['inputs'] = '/home/baltzell/hps-2019-mss-prod.txt'
  cfg['outDir'] = '/volatile/hallb/hps/baltzell/test'
  cfg['mergePattern'] = 'hps_%.6d.evio.%.5d-%.5d.hipo'
  cfg['mergeSize'] = 100

  workflow = SwifWorkflow('trigskim')
  workflow.addRuns(ChefUtil.getRunList(cfg['runs']))
  workflow.findFiles(cfg['inputs'])
  workflow.setPhaseSize(0)

  phase=0

  for inputs in workflow.getGroups():
    if len(inputs)<100:
      continue
    phase+=1
    inps=[]
    for ii,inp in enumerate(inputs):
      inps.append(inp)
      if len(inps)>=cfg['mergeSize'] or ii>=len(inputs)-1:
        job=EvioTriggerFilterJob(workflow.name,cfg)
        job.setPhase(phase)
        for inp in inps: job.addInput(os.path.basename(inp),inp)
        job.setCmd()
        workflow.addJob(job)
        inps=[]

  print workflow.getJson()

