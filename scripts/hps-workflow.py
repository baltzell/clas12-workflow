#!/usr/bin/env python
import os,sys,logging,argparse

import RunFileUtil,HPSJobs,ChefUtil
from SwifWorkflow import SwifWorkflow

logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[ %(name)-15s ] %(message)s')
logger=logging.getLogger(__name__)

cli=argparse.ArgumentParser(description='Generate a HPS SWIF workflow.')#,
#     epilog='(*) = required option for all models, from command-line or config file')

cli.add_argument('--tag',      metavar='NAME',help='workflow name',  type=str, default=None,required=True)
cli.add_argument('--evio2lcio',help='run evio2lcio',action='store_true',default=False)
cli.add_argument('--trigger',  metavar='NAME',help='trigger type, repeatable',action='append',default=[],choices=['fee','mult2','mult3','muon','fcup'])
cli.add_argument('--outDir',   metavar='PATH',help='final data location', type=str,default=None,required=True)
cli.add_argument('--logDir',   metavar='PATH',help='log directory', type=str,default=None,required=False)
cli.add_argument('--runs',     metavar='RUNS/FILE',help='(*) run numbers (e.g. "4013" or "4013,4015" or "3980,4000-4999"), or a file containing a list of run numbers.  This option is repeatable.', action='append', default=[], type=str,required=True)
cli.add_argument('--inputs',   metavar='DIR/FILE',help='directory to search recursively for input files, or file containing list of input files, repeatable',type=str,default=None)
cli.add_argument('--mergeSize',metavar='#',help='number of files to merge', default=0, type=int)
cli.add_argument('--jar',      metavar='PATH',help='path to hps-java-bin.jar',type=str,default=None)
cli.add_argument('--steer',    metavar='PATH',help='steering resource "path"',type=str,default='/org/hps/steering/recon/PhysicsRun2019FullRecon.lcsim')
cli.add_argument('--detector', metavar='NAME',help='detector name',type=str,default=None)#'HPS-PhysicsRun2019-v2-4pt5')
cli.add_argument('--outPrefix',metavar='NAME',help='output file prefix',type=str,default='')
cli.add_argument('--runno',    metavar='#',help='override run number from input filename',type=int,default=None)
cli.add_argument('--hours',    metavar='#',help='job time request in hours (default = (2)24 for (non-)evio2lcio jobs)',type=int,default=None)
cli.add_argument('--submit',   help='submit and run workflow automatically',default=False,action='store_true')
args=cli.parse_args(sys.argv[1:])

if len(args.trigger)>0:
  if args.evio2lcio or args.jar:
    cli.warning('ignoring --evio2lcio/jar/steer for trigger job')
else:
  if args.jar is None:
    cli.error('requires --jar')
  if args.steer is None:
    cli.error('requires --steer')
  if not os.path.isfile(args.jar):
    cli.error('missing jar:  '+args.jar)
  if args.mergeSize != 0:
    cli.error('only mergeSize=0 supported for trigger yet')
  if not args.evio2lcio:
    if not args.outPrefix:
      cli.error('--outPrefix is required for evio2lcio')
    if not args.detector:
      cli.error('--detector is required for evio2lcio')

RunFileUtil.setFileRegex('.*hps[_A-Za-z]*[23]?_(\d+)\.evio\.(\d+).*')

cfg={}
cfg['logDir'] = args.logDir
cfg['runs']   = args.runs
cfg['outDir'] = args.outDir
cfg['mergeSize'] = args.mergeSize
cfg['trigger'] = args.trigger
cfg['jar'] = args.jar
cfg['steer'] = args.steer
cfg['detector']= args.detector
cfg['outPrefix'] = args.outPrefix
cfg['runno'] = args.runno

if args.inputs is None:
  cfg['inputs'] = '/home/hps/users/baltzell/hps-2019-mss-prod.txt'
else:
  cfg['inputs'] = args.inputs
cfg['mergePattern'] = 'hps_%.6d.evio.%.5d-%.5d'

workflow = SwifWorkflow(args.tag)
workflow.addRuns(ChefUtil.getRunList(cfg['runs']))
workflow.findFiles(cfg['inputs'])
workflow.setPhaseSize(0)

runsPerPhase=31
phase,runsInThisPhase=0,0

for inputs in workflow.getGroups():
#    if len(args.trgger)>0 and len(inputs)<100:
#      continue
  runsInThisPhase += 1
  if len(args.trigger)>0 and runsInThisPhase > runsPerPhase:
    runsInThisPhase = 0
    phase += 1
  inps=[]
  for ii,inp in enumerate(inputs):
    inps.append(inp)
    if len(inps)>=cfg['mergeSize'] or ii>=len(inputs)-1:
      if len(args.trigger)>0:
        job=HPSJobs.EvioTriggerFilterJob(workflow,cfg)
      elif args.evio2lcio:
        job=HPSJobs.EvioToLcioJob(workflow,cfg)
      else:
        job=HPSJobs.HpsJavaJob(workflow,cfg)
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

logger.info('Created workflow with %d jobs based on %d runs with %d total input files and %d phases'%\
  (len(workflow.jobs),len(workflow.getRunList()),workflow.getFileCount(),workflow.phase+1))

if os.path.exists(workflow.name+'.json'):
  logger.critical('File already exists:  '+workflow.name+'.json')
  sys.exit()

logger.info('Writing workflow to ./'+workflow.name+'.json')
with open(workflow.name+'.json','w') as out:
  out.write(workflow.getJson())

if args.submit:
  logger.info('Submitting %s.json with %d jobs ...\n'%(workflow.name,len(workflow.jobs)))
  workflow.submitJson()


