#!/usr/bin/env python
import re,os,sys,logging,argparse

import RunFileUtil,HPSJobs,ChefUtil,JarUtil
from SwifWorkflow import SwifWorkflow

MERGEPATTERN='hps_%.6d.evio.%.5d-%.5d'
FILEREGEX='.*hps[_A-Za-z]*_(\d+)\.evio\.(\d+).*'
RECONSTEER='/org/hps/steering/recon/PhysicsRun2019FullRecon.lcsim'
TRIGGERS=['fee','mult2','mult3','muon','rndm','moll']
JAVAS=['11.0.2','14.0.2']

logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[ %(name)-15s ] %(message)s')
logger=logging.getLogger(__name__)

cli=argparse.ArgumentParser(description='Generate a HPS SWIF workflow.',epilog='(*) = required')
subclis=cli.add_subparsers(dest='command')

cli.add_argument('--tag',      metavar='NAME',help='(*) workflow name',  type=str, required=True)
cli.add_argument('--outDir',   metavar='PATH',help='(*) final data location', type=str, required=True)
cli.add_argument('--logDir',   metavar='PATH',help='log directory', type=str, default=None, required=False)
cli.add_argument('--runs',     metavar='RUNS/FILE',help='(*) run numbers (e.g. "4013" or "4013,4015" or "3980,4000-4999"), or a file containing a list of run numbers, repeatable', action='append', default=[], type=str, required=True)
cli.add_argument('--inputs',   metavar='DIR/FILE',help='(*) directory to search recursively for input files, or file containing list of input files, repeatable',default=[], type=str, action='append', required=True)
cli.add_argument('--hours',    metavar='#',help='job time request in hours (default = 2/4/24 for user/skim/evio2lcio jobs)', type=int, default=None)
cli.add_argument('--submit',   help='submit and run workflow automatically', default=False, action='store_true')
cli.add_argument('--fileRegex',metavar='REGEX',help='input filename format for matching run and file numbers (default = %s)'%FILEREGEX, type=str, default=FILEREGEX)

cli_evioskim = subclis.add_parser('evioskim',epilog='(*) = required')
cli_evioskim.add_argument('--trigger',  metavar='NAME',help='(*) trigger type, repeatable ('+'/'.join(TRIGGERS)+')',action='append',default=[],choices=TRIGGERS,required=True)
#cli_evioskim.add_argument('--mergeSize',metavar='#',help='number of files to merge', default=0, type=int, choices=[0,2,3,4,5,10,20,30,50,100])
#cli_evioskim.add_argument('--workDir',  metavar='PATH',help='(*) temporary location before merging', type=str, required=True)

cli_evio2lcio = subclis.add_parser('evio2lcio',epilog='(*) = required')
cli_evio2lcio.add_argument('--jar',      metavar='PATH',help='(*) path to hps-java-bin.jar',type=str,required=True)
cli_evio2lcio.add_argument('--detector', metavar='NAME',help='(*) detector name',type=str,required=True)
cli_evio2lcio.add_argument('--steer',    metavar='RESOURCE',help='steering resource (default=%s)'%RECONSTEER,type=str,default=RECONSTEER)
cli_evio2lcio.add_argument('--outPrefix',metavar='NAME',help='output file prefix',type=str,default='')
cli_evio2lcio.add_argument('--runno',    metavar='#',help='override run numbers from input filenames',type=int,default=-1)
cli_evio2lcio.add_argument('--java',     metavar='#.#.#',help='override system java version (choices=%s)'%','.join(JAVAS),type=str,default=None,choices=JAVAS)

cli_lcio = subclis.add_parser('lcio',epilog='(*) = required')
cli_lcio.add_argument('--jar',      metavar='PATH',help='(*) path to hps-java-bin.jar',type=str,required=True)
cli_lcio.add_argument('--steer',    metavar='RESOURCE',help='(*) steering resource (e.g. %s)'%RECONSTEER,type=str,required=True)
cli_lcio.add_argument('--detector', metavar='NAME',help='detector name',type=str,default=None)
cli_lcio.add_argument('--outPrefix',metavar='NAME',help='output file prefix',type=str,required=True)
cli_lcio.add_argument('--runno',    metavar='#',help='pass run number to java command via -R #, and use filename if # not specified',type=int,default=None,nargs='?',const='-1')
cli_lcio.add_argument('--java',     metavar='#.#.#',help='override system java version (choices=%s)'%','.join(JAVAS),type=str,default=None,choices=JAVAS)
cli_lcio.add_argument('--outFile',  metavar='FILENAME',help='add an additional output file, repeatable', default=[], action='append', type=str)
cli_lcio.add_argument('--noLCIO',   help='disable LCIO output file', default=False, action='store_true')

args=cli.parse_args(sys.argv[1:])

cfg = vars(args)
cfg['mergePattern'] = MERGEPATTERN

if args.command=='lcio':
  if (args.runno is None and args.detector is not None) or (args.runno is not None and args.detector is None):
    cli.error('lcio jobs require both --runno/--detector or neither')

if 'jar' in cfg:
  cfg['jar'] = os.path.abspath(cfg['jar'])
  if not os.path.isfile(cfg['jar']):
    cli.error('missing jar file:  '+cfg['jar'])

if cfg.get('java') is not None:
  cfg['java']='/group/clas12/packages/jdk/'+cfg['java']

if cfg.get('outFile') is not None:

  if cfg.get('noLCIO') is False and len(cfg.get('outFile'))==0:
    cli.error('You specified --nolcio and no --outFile.  That means no outputs.')

  suffixes = ['.slcio']

  for x in cfg.get('outFile'):

    if x.find('.') <=0:
      cli.error('This --outFile does not contain a .suffix:  '+x)

    suffix = x.split('.').pop()

    if suffix in suffixes:
      cli.error('Multiple --outFiles with the same suffix is not supported:  '+suffix)

    suffixes.append(suffix)

if cfg.get('steer') is not None:

  if os.path.isfile(cfg['steer']):

    cfg['steer'] = os.path.abspath(cfg['steer'])
    cfg['steerIsFile'] = True
    logger.warning('interpreting --steer as a file:  '+cfg['steer'])

  elif JarUtil.contains(cfg.get('jar'),cfg['steer']):
    cfg['steerIsFile'] = False
    logger.warning('interpreting --steer as a jar resource:  '+cfg['steer'])

  else:
    cli.error('--steer not found as a file nor jar resource:  '+cfg['steer'])

if cfg.get('detector') is not None:
  if not JarUtil.contains(cfg.get('jar'),cfg['detector']):
    cli.error('detector not found in jar:  '+cfg['detector'])

RunFileUtil.setFileRegex(FILEREGEX)

workflow = SwifWorkflow(args.tag)
workflow.addRuns(ChefUtil.getRunList(cfg['runs']))
workflow.findFiles(cfg['inputs'])
workflow.setPhaseSize(0)

if args.command == 'evioskim':

  # just a loop over runs:
  for inputs in workflow.getGroups():

    # accumulate jobs for merging their outputs:
    jobs = []

    # just a loop over all files in the run:
    for ii,inp in enumerate(inputs):

      # one filter job for each input file:
      job = HPSJobs.EvioTriggerFilterJob(workflow,cfg)
      job.addInput(os.path.basename(inp),inp)
      job.setCmd()
      workflow.addJob(job)
      jobs.append(job)

      # decide whether to accumulate more jobs before merging:
      #if len(jobs) < cfg['mergeSize'] and ii < len(inputs)-1:
      continue

      # organize by prefix:
      inps = {}
      while len(jobs) > 0:
        job = jobs.pop(0)
        for o in job.getOutputPaths():
          prefix = re.match('^(hps[_a-z]+)',o.split('/').pop()).group(1)
          if prefix not in inps:
            inps[prefix] = {'jobs':[],'files':[]}
          inps[prefix]['jobs'].append(job)
          inps[prefix]['files'].append(o)

      # one merge job for each prefix:
      for prefix in inps.keys():
        job = HPSJobs.EvioMergeJob(workflow,cfg)
        for inp in inps[prefix]['files']:
          job.addInput(os.path.basename(inp),inp)
        for j in inps[prefix]['jobs']:
          job.antecedents.append(j.getJobName())
        job.setCmd()
        if not job.outputExists():
          workflow.addJob(job)

else:

  for inputs in workflow.getGroups():

    for inp in inputs:

      # choose the job type:
      if args.command == 'evio2lcio':
        job=HPSJobs.EvioToLcioJob(workflow,cfg)
      else:
        job=HPSJobs.HpsJavaJob(workflow,cfg)

      job.addInput(os.path.basename(inp),inp)
      job.setCmd()

      if not job.outputExists():
        workflow.addJob(job)

logger.info('Created workflow with %d jobs based on %d runs with %d total input files and %d phases'%\
  (len(workflow.jobs),len(workflow.getRunList()),workflow.getFileCount(),workflow.phase+1))

if len(workflow.jobs)<1:
  logger.critical('Found zero jobs to create')
  sys.exit(1)

if os.path.exists(workflow.name+'.json'):
  logger.critical('File already exists:  '+workflow.name+'.json')
  sys.exit(1)

logger.info('Writing workflow to ./'+workflow.name+'.json')
with open(workflow.name+'.json','w') as out:
  out.write(workflow.getJson())

if args.submit:
  logger.info('Submitting %s.json with %d jobs ...\n'%(workflow.name,len(workflow.jobs)))
  workflow.submitJson()

