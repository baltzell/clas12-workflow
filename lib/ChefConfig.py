import os
import sys
import argparse

RUNGROUPS=['rga','rgb','rgk','test']

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
  for fileName in args.runfile:
    if not os.access(fileName,os.R_OK):
      print '\nERROR:  File is not readable:  '+fileName+'\n'
      return None
    for line in open(fileName,'r').readlines():
      try:
        runs.append(int(line))
      except:
        print '\nERROR: Run numbers must be integers:  %s (%s)\n'%(fileName,line)
        return None
  return runs

def getConfig(args):

  df='\n(default=%(default)s)'

  cli=argparse.ArgumentParser(description='Generate a CLAS12 SWIF decoding+merging workflow.')

  cli.add_argument('--runGroup',metavar='NAME',help='run group name (REQUIRED)', type=str, required=True)
  cli.add_argument('--task',    metavar='NAME',help='task name (REQUIRED)',      type=str, required=True)

  cli.add_argument('--submit', help='submit and run jobs immediately', action='store_true', default=False)

  cli.add_argument('--run',    metavar='RUN(s)',help='run numbers (e.g. 4013 or 4013,4015 or 4000-4999)', action='append', default=[], type=str)
  cli.add_argument('--runfile',metavar='PATH',help='file of run numbers', action='append', default=[], type=str)

  cli.add_argument('--mssList', metavar='PATH',help='file or directory of input files'+df,type=str,default='/mss/clas12/rg-a/data')
  cli.add_argument('--workDir', metavar='PATH',help='temporary data location'+df,         type=str,default='/volatile/clas12/clas12/data/tmp')
  cli.add_argument('--outDir',  metavar='PATH',help='final location of merged files'+df,  type=str,default='/volatile/clas12/clas12/data/tmp')
  cli.add_argument('--coatjava',metavar='PATH',help='coatjava install location'+df,       type=str,default='/group/clas12/packages/coatjava-6.0.0')

  cli.add_argument('--phaseSize', metavar='#',help='number of files per phase'+df, type=int, default=1200)
  cli.add_argument('--mergeSize', metavar='#',help='number of files per merge'+df, type=int, default=10)

  cli.add_argument('--torus',    metavar='#.#',help='override RCDB torus scale'+df,type=float, default=None)
  cli.add_argument('--solenoid', metavar='#.#',help='override RCDB solenoid scale'+df,type=float, default=None)

  cli.add_argument('--project', metavar='NAME',help='scicomp batch project name'+df,type=str, default='clas12')
  cli.add_argument('--track',   metavar='NAME',help='scicomp batch track name'+df,  type=str, default='reconstruction')
  cli.add_argument('--mergePatt',metavar='PATTERN',help='merged filename format'+df, type=str, default='clas_%.6d.evio.%.5d-%.5d.hipo')
  cli.add_argument('--evioRegex',metavar='REGEX',  help='evio filename regex'+df,    type=str, default='.*clas[A-Za-z]*_(\d+)\.evio\.(\d+)')

  cli.add_argument('--model', help='workflow model (0=ThreePhase, 1=Rolling, 2=SinglesOnly)'+df, type=int, choices=[0,1,2,3], default=1)

  args = cli.parse_args(args)

  runs = getRunList(args)

  if runs is None: sys.exit()

  if args.task.count(' ')>0:
    sys.exit('ERROR:  task cannot contain spaces')
  if args.runGroup not in RUNGROUPS:
    sys.exit('ERROR:  valid run groups are: '+', '.join(RUNGROUPS))

  cfg={}
  cfg['dryRun']      = not args.submit
  cfg['runs']        = runs
  cfg['coatjava']    = args.coatjava
  cfg['mssList']     = args.mssList
  cfg['workDir']     = args.workDir
  cfg['outDir']      = args.outDir
  cfg['project']     = args.project
  cfg['track']       = args.track
  cfg['phaseSize']   = args.phaseSize
  cfg['mergeSize']   = args.mergeSize
  cfg['mergePattern']= args.mergePatt
  cfg['evioRegex']   = args.evioRegex
  cfg['model']       = args.model
  cfg['torus']       = args.torus
  cfg['solenoid']    = args.solenoid
  cfg['runGroup']    = args.runGroup
  cfg['task']        = args.task
  cfg['workflow']    = args.runGroup+'_'+args.task
  return cli,cfg

if __name__ == '__main__':
  cli,cfg=getConfig(sys.argv[1:])
  print cfg

