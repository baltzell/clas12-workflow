import os
import sys
import json
import argparse
import traceback
from RunFileUtil import setFileRegex
from RunFileUtil import getFileRegex
from RunFileUtil import getRunList

RUNGROUPS=['rga','rgb','rgk','rgm','rgl','rgd','rge','test']
TRACKS=['reconstruction','debug']
TASKS=['decode','recon']
MODELS=[0,1,2,3]

class ChefConfig:

  def __init__(self,args):

    self._setDefaults()

    self._setCli()

    if self.args.defaults:
      sys.exit(str(self))

    if self.args.config is not None:
      self._readConfigFile(self.args.config)

    self._parseCliArgs()

    self._checkConfig()

    self.cfg['runs'] = getRunList(vars(self.args))
    if self.cfg['runs'] is None or len(self.cfg['runs'])==0:
      self.cli.error('\nFound no runs.')

    if self.args.show:
      sys.exit(str(self))

  def _setDefaults(self):
    self.cfg={}
    self.cfg['project']       = 'clas12'
    self.cfg['track']         = 'reconstruction'
    self.cfg['task']          = 'decode'
    self.cfg['runGroup']      = None
    self.cfg['coatjava']      = '/group/clas12/packages/coatjava-6b.0.0'
    self.cfg['workflow']      = None
    self.cfg['inputs']        = []
    self.cfg['workDir']       = None
    self.cfg['outDir']        = None
    self.cfg['phaseSize']     = 2000
    self.cfg['mergeSize']     = 10
    self.cfg['model']         = 1
    self.cfg['torus']         = None
    self.cfg['solenoid']      = None
    self.cfg['multiRun']      = False
    self.cfg['mergePattern']  = 'clas_%.6d.evio.%.5d-%.5d.hipo'
    self.cfg['singlePattern'] = 'clas_%.6d.evio.%.5d.hipo'
    self.cfg['fileRegex']     = getFileRegex()
    self.cfg['submit']        = False

  def _setCli(self):

    self.cli=argparse.ArgumentParser(description='Generate a CLAS12 SWIF workflow.')

    self.cli.add_argument('-d', '--defaults',help='print default config and exit', action='store_true', default=False)
    self.cli.add_argument('-s', '--show',    help='print config and exit', action='store_true', default=False)
    self.cli.add_argument('--config',metavar='PATH',help='load config file (contents superceded by command line arguments)', type=str,default=None)

    self.cli.add_argument('--workflow',metavar='NAME',help='workflow name suffix/tag, e.g. v0, automatically prefixed with runGroup and task',  type=str, default=None)
    self.cli.add_argument('--runGroup',metavar='NAME',help='run group name', type=str, choices=RUNGROUPS, default=None)
    self.cli.add_argument('--task',    metavar='NAME',help='task name', type=str, choices=TASKS, default=None)

    self.cli.add_argument('--inputs', metavar='PATH',help='name of file containing a list of input files, or a directory to be searched recursively for input files (repeatable)',action='append',type=str,default=[])
    self.cli.add_argument('--run',    metavar='RUN(s)',help='run numbers (e.g. 4013 or 4013,4015 or 3980,4000-4999) (repeatable, not allowed in config file)', action='append', default=[], type=str)
    self.cli.add_argument('--runFile',metavar='PATH',help='file containing a list of run numbers (repeatable, not allowed in config file)', action='append', default=[], type=str)

    self.cli.add_argument('--outDir', metavar='PATH',help='final data location', type=str,default=None)
    self.cli.add_argument('--workDir',metavar='PATH',help='temporary data location (for merging workflows only)', type=str,default=None)

    self.cli.add_argument('--coatjava',metavar='PATH',help='coatjava install location', type=str,default=None)

    self.cli.add_argument('--phaseSize', metavar='#',help='number of files per phase', type=int, default=None)
    self.cli.add_argument('--mergeSize', metavar='#',help='number of files per merge', type=int, default=None)

    self.cli.add_argument('--torus',    metavar='#.#',help='override RCDB torus scale',   type=float, default=None)
    self.cli.add_argument('--solenoid', metavar='#.#',help='override RCDB solenoid scale',type=float, default=None)

    self.cli.add_argument('--fileRegex',metavar='REGEX',help='input filename format (for matching run and file numbers)', type=str, default=None)

    self.cli.add_argument('--model', help='workflow model (0=ThreePhase, 1=Rolling, 2=SinglesOnly)', type=int, choices=MODELS,default=None)

    self.cli.add_argument('--multiRun', help='allow multiple runs per phase (non-merging workflow only)', action='store_true', default=None)

    #  self.cli.add_argument('--submit', help='submit and run jobs immediately', action='store_true', default=False)
    #  self.cli.add_argument('--track',   metavar='NAME',help='scicomp batch track name',   type=str, default=None)

    self.args = self.cli.parse_args(args)

  def _readConfigFile(self,filename):

    if not os.access(filename,os.R_OK):
      sys.exit('Config file is not readable:  '+filename)

    try:
      cfg = json.load(open(filename,'r'))
    except:
      print traceback.format_exc()
      sys.exit('FATAL ERROR: Config file '+filename+' has invalid JSON format.')

    for key,val in cfg.iteritems():
      if key not in self.cfg:
        sys.exit('FATAL ERROR:  Config file contains invalid key:  '+key)
      if key == 'model' and val not in MODELS:
        sys.exit('Config file contans invalid model:  '+val)
      if key == 'task' and val not in TASKS:
        sys.exit('Config file contains invalid task:  '+val)
      if key == 'runGroup' and val not in RUNGROUPS:
        sys.exit('Config file contains invalid runGroup:  '+val)
      if key == 'track' and val not in TRACKS:
        sys.exit('Config file contains invalid track:  '+val)
      self.cfg[key]=val

  def _parseCliArgs(self):
    for key,val in vars(self.args).iteritems():
      if key in self.cfg:
        if val is None:
          continue
        if type(val) is list and len(val)==0:
          continue
        self.cfg[key]=val

  def _checkConfig(self):

    if len(self.args.run)==0 and len(self.args.runFile)==0:
      self.cli.error('At least one of --run or --runFile must be specified on the command line.')

    if len(self.cfg['inputs'])==0:
      self.cli.error('"inputs" must be defined.')

    if self.cfg['outDir'] is None:
      self.cli.error('"outDir" must be defined.')

    # non-merging workflows:
    if self.cfg['model']==2:

      if self.cfg['workDir'] is not None:
        print 'WARNING:  ignoring "workDir" for non-merging workflow.'

      if self.cfg['fileRegex'] != getFileRegex():
        setFileRegex(self.cfg['fileRegex'])

    # merging workflow have some additional constraints:
    else:

      if self.cfg['workDir'] is None:
        self.cli.error('"workDir" must be defined for merging workflows.')

      if self.cfg['phaseSize']%self.cfg['mergeSize']!=0:
        self.cli.error('"phaseSize" must be a multiple of "mergeSize".')

      if self.cfg['fileRegex'] != getFileRegex():
        self.cli.error('Non-default "fileRegex" is not allowed in merging workflows.')

      if self.cfg['multiRun']:
        self.cli.error('"multiRun" is not allowed in merging workflows.')

    return self.cfg

  def __str__(self):
    return json.dumps(self.cfg,indent=2,separators=(',',': '),sort_keys=True)

  def __dict__(self):
    return self.cfg

if __name__ == '__main__':
  cc=ChefConfig(sys.argv[1:])
  print str(cc)

