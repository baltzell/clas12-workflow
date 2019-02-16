import os
import sys
import argparse
from RunFileUtil import setFileRegex,getFileRegex

RUNGROUPS=['rga','rgb','rgk','rgm','rgl','rgd','rge','test']
TASKS=['decode','recon']
PROJECT='clas12'
TRACK='reconstruction'

SINGLEPATT='clas_%.6d.evio.%.5d.hipo'
MERGEPATT='clas_%.6d.evio.%.5d-%.5d.hipo'

def getRunList(args):
  runs=[]
  for run in args.run:
    for run in run.split(','):
      if run.find('-')<0:
        try:
          runs.append(int(run))
        except:
          print '\nERROR: Run numbers must be integers:  '+run+'\n'
          return None
      else:
        if run.count('-') != 1:
          print '\nERROR:  Invalid run range: '+run+'\n'
          return None
        try:
          start,end=run.split('-')
          start=int(start)
          end=int(end)
          for run in range(start,end+1):
            runs.append(run)
        except:
          print '\nERROR: Run numbers must be integers:  '+run+'\n'
          return None
  for fileName in args.runFile:
    if not os.access(fileName,os.R_OK):
      print '\nERROR:  File is not readable:  '+fileName+'\n'
      return None
    for line in open(fileName,'r').readlines():
      run=line.strip().split()[0]
      try:
        runs.append(int(run))
      except:
        print '\nERROR: Run numbers must be integers:  %s (%s)\n'%(fileName,line)
        return None
  return runs

def getConfig(args):

  df='\n(default=%(default)s)'

  cli=argparse.ArgumentParser(description='Generate a CLAS12 SWIF workflow.')

  cli.add_argument('--workflow',metavar='NAME',help='workflow suffix (automatically prefixed with runGroup and task)',  type=str, required=True)
  cli.add_argument('--runGroup',metavar='NAME',help='run group name', type=str, choices=RUNGROUPS, required=True)
  cli.add_argument('--task',    metavar='NAME',help='task name'+df,   type=str, choices=TASKS, default='decode')

  cli.add_argument('--inputs', metavar='PATH',help='name of file containing a list of input files, or a directory to be searched recursively for input files (repeatable)',action='append',type=str,required=True)
  cli.add_argument('--workDir',metavar='PATH',help='temporary data location',         type=str,required=True)
  cli.add_argument('--outDir', metavar='PATH',help='final data location',             type=str,required=True)

  cli.add_argument('--run',    metavar='RUN(s)',help='run numbers (e.g. 4013 or 4013,4015 or 3980,4000-4999) (repeatable)', action='append', default=[], type=str)
  cli.add_argument('--runFile',metavar='PATH',help='file containing a list of run numbers (repeatable)', action='append', default=[], type=str)

  cli.add_argument('--coatjava',metavar='PATH',help='coatjava install location'+df, type=str,default='/group/clas12/packages/coatjava-6b.0.0')

  cli.add_argument('--phaseSize', metavar='#',help='number of files per phase'+df, type=int, default=1200)
  cli.add_argument('--mergeSize', metavar='#',help='number of files per merge'+df, type=int, default=10)

  cli.add_argument('--torus',    metavar='#.#',help='override RCDB torus scale'+df,   type=float, default=None)
  cli.add_argument('--solenoid', metavar='#.#',help='override RCDB solenoid scale'+df,type=float, default=None)

  cli.add_argument('--fileRegex',metavar='REGEX',help='input filename format'+df,     type=str, default=getFileRegex())

  cli.add_argument('--model', help='workflow model (0=ThreePhase, 1=Rolling, 2=SinglesOnly)'+df, type=int, choices=[0,1,2,3], default=1)

  cli.add_argument('--multiRun', help='allow multiple runs per phase'+df, action='store_true', default=False)

#  cli.add_argument('--submit', help='submit and run jobs immediately', action='store_true', default=False)
#  cli.add_argument('--project', metavar='NAME',help='scicomp batch project name'+df, type=str, default=PROJECT)
#  cli.add_argument('--track',   metavar='NAME',help='scicomp batch track name'+df,   type=str, default=TRACK)

  args = cli.parse_args(args)

  args.submit = False

  if args.model != 2:
    if args.phaseSize % args.mergeSize != 0:
      cli.error('\n--phaseSize must be a multiple of --mergeSize for merging workflows.')

  if args.fileRegex is not None and args.fileRegex != getFileRegex():
    if args.model==2:
      setFileRegex(args.fileRegex)
    else:
      cli.error('--fileRegex is only allowed with SinglesOnly (--model 2)')

  if args.multiRun is True:
    if args.model!=2:
      cli.error('--multiRun is only allowed with SinglesOnly (--model 2)')


  runs = getRunList(args)

  if runs is None or len(runs)<1:
    cli.error('\nFound no runs.  See --run or --runfile.')

  cfg={}
  cfg['dryRun']      = not args.submit
  cfg['runs']        = runs
  cfg['coatjava']    = args.coatjava
  cfg['inputs']      = args.inputs
  cfg['workDir']     = args.workDir
  cfg['outDir']      = args.outDir
  cfg['project']     = PROJECT
  cfg['track']       = TRACK
  cfg['phaseSize']   = args.phaseSize
  cfg['mergeSize']   = args.mergeSize
  cfg['model']       = args.model
  cfg['torus']       = args.torus
  cfg['solenoid']    = args.solenoid
  cfg['runGroup']    = args.runGroup
  cfg['multiRun']    = args.multiRun
  cfg['task']        = args.task
  cfg['workflow']    = args.workflow
  cfg['mergePattern']  = MERGEPATT
  cfg['singlePattern'] = SINGLEPATT

  return cli,cfg

if __name__ == '__main__':
  cli,cfg=getConfig(sys.argv[1:])
  print cfg

