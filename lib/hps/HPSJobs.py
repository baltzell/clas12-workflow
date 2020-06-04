import os,re,sys,logging,getpass,argparse

import RunFileUtil
import ChefUtil
from SwifJob import SwifJob
from SwifWorkflow import SwifWorkflow

logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[ %(name)-15s ] %(message)s')
logger=logging.getLogger(__name__)

RunFileUtil.setFileRegex('.*hps[_A-Za-z]*_(\d+)\.evio\.(\d+)')

#
# 300K evio files in physrun2019 --> 900 TB
# FEE skims are about 30 MB per evio file --> 9 TB
# MULT skims are much smaller
# MUON is about 15% --> 30 TB
# FCUP is 5% 
#
class EvioTriggerFilterJob(SwifJob):
  def __init__(self,workflow,cfg):
    SwifJob.__init__(self,workflow.name)
    self.cfg=cfg
    self.copyInputs=False
    self.setTime('4h')
    self.setRam('500MB')
    self.setDisk('10GB')
    self.project='hallb-pro'
    self.setLogDir('/farm_out/'+getpass.getuser()+'/'+workflow.name)
    ChefUtil.mkdir(self.logDir)
  def addOutput(self,local,remote):
    SwifJob.addOutput(self,local,remote)
    ChefUtil.mkdir(os.path.dirname(remote))
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

if __name__ == '__main__':

  cli=argparse.ArgumentParser(description='Generate a CLAS12 SWIF workflow.',
     epilog='(*) = required option for all models, from command-line or config file')

  cli.add_argument('--trigger',metavar='NAME',action='append',default=[],choices['fee','mult2','mult3','muon','fcup'])
  cli.add_argument('--tag',    metavar='NAME',help='(*) e.g. pass1v0, automatically prefixed with runGroup and suffixed by model to define workflow name',  type=str, default=None,required=True)
  cli.add_argument('--outDir', metavar='PATH',help='final data location', type=str,default=None,required=True)
  cli.add_argument('--runs',   metavar='RUNS/PATH',help='(*) run numbers (e.g. "4013" or "4013,4015" or "3980,4000-4999"), or a file containing a list of run numbers.  This option is repeatable.', action='append', default=[], type=str,required=True)
  cli.add_argument('--mergeSize',metavar='#',help='number of files to merge', default=10, type=int)
  args=cli.parse_args(sys.argv[1:])

  cfg={}
  cfg['runs']   = args.runs
  cfg['outDir'] = args.outDir
  cfg['mergeSize'] = args.mergeSize
  cfg['trigger'] = args.trigger
  cfg['inputs'] = '/home/hps/users/baltzell/hps-2019-mss-prod.txt'
  cfg['mergePattern'] = 'hps_%.6d.evio.%.5d-%.5d'

  workflow = SwifWorkflow('trigskim-'+args.tag)
  workflow.addRuns(ChefUtil.getRunList(cfg['runs']))
  workflow.findFiles(cfg['inputs'])
  workflow.setPhaseSize(0)

  runsPerPhase=31
  phase,runsInThisPhase=0,0

  for inputs in workflow.getGroups():
    if len(inputs)<100:
      continue
    runsInThisPhase += 1
    if runsInThisPhase > runsPerPhase:
      runsInThisPhase = 0
      phase += 1
    inps=[]
    for ii,inp in enumerate(inputs):
      inps.append(inp)
      if len(inps)>=cfg['mergeSize'] or ii>=len(inputs)-1:
        job=EvioTriggerFilterJob(workflow,cfg)
        job.setPhase(phase)
        for inp in inps:
          job.addInput(os.path.basename(inp),inp)
        job.setCmd()
        exists=False
        for out in job.outputs:
          if out['remote'].startswith('file:'):
            x=out['remote'][5:]
          elif out['remote'].startswith('mss:'):
            x=out['remote'][4:]
          if os.path.exists(x):
            inps.pop()
            exists=True
            break
        if not exists:
          workflow.addJob(job)
        inps=[]

  print(workflow.getJson())

