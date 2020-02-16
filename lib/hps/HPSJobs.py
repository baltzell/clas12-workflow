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
#
CFG={}
CFG['runs'] = '10004-10740'
CFG['inputs'] = '/home/baltzell/hps-2019-mss-prod.txt'
CFG['outDir'] = '/volatile/hallb/hps/production/physrun2019/evio/fee/10:1'
CFG['mergePattern'] = 'hps_%.6d.evio.%.5d-%.5d.hipo'
CFG['mergeSize'] = 10

class TriggerFilterJob(SwifJob):
  def __init__(self,workflow):
    SwifJob.__init__(self,workflow)
    self.setTime('2h')
    self.setRam('500MB')
    self.setDisk('%dGB'%(CFG['mergeSize']*2+10))
    self.project='hallb-pro'
    cmd ='source /apps/root/6.12.06/bin/thisroot.csh ; '
    cmd+='setenv LD_LIBRARY_PATH ${LD_LIBRARY_PATH}:/home/holtrop/lib ; '
    cmd+='ls -lhtr ; '
    cmd+='/home/holtrop/bin/HPS_Trigger_Filter -T FEE -m 10000000 -o out ./*.evio* ;'
    cmd+='ls -lhtr'
    self.setCmd(cmd)

if __name__ == '__main__':

  workflow = SwifWorkflow('hpsfee')
  workflow.addRuns(ChefUtil.getRunList(CFG['runs']))
  workflow.findFiles(CFG['inputs'])

  phase=0

  for inputs in workflow.getGroups():
    if len(inputs)<100:
      continue
    phase+=1
    inps=[]
    for ii,inp in enumerate(inputs):
      inps.append(inp)
      if len(inps)>=CFG['mergeSize'] or ii>=len(inputs)-1:
        job=TriggerFilterJob(workflow.name)
        job.setPhase(phase)
        for inp in inps:
          job.addInput(os.path.basename(inp),inp)
        rf1 = RunFileUtil.RunFile(inps[0])
        rf2 = RunFileUtil.RunFile(inps[len(inps)-1])
        outfile = CFG['mergePattern']%(rf1.runNumber,rf1.fileNumber,rf2.fileNumber)
        outdir = CFG['outDir']+'/%.6d/'%rf1.runNumber
        job.addOutput('out_0.evio',outdir+outfile)
        job.setLogDir('/farm_out/'+getpass.getuser()+'/hpsfee')
        workflow.addJob(job)
        ChefUtil.mkdir(outdir)
        inps=[]

  print workflow.getJson()

