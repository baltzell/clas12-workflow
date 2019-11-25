import os,sys,json,copy,logging,getpass,argparse,traceback
import ChefUtil
import RunFileUtil
import CLAS12Workflows

_LOGGER=logging.getLogger(__name__)

MODELS=['dec','decmrg','rec','ana','decrec','decmrgrec','recana','decrecana','decmrgrecana']

CHOICES={
    'runGroup': ['rga','rgb','rgk','rgm','rgl','rgd','rge','test'],
    'model'   : MODELS,
    'threads' : [16, 20, 24, 32]
}

CFG={
    'project'       : 'clas12',
    'runGroup'      : None,
    'coatjava'      : None,
    'clara'         : None,
    'tag'           : None,
    'inputs'        : [],
    'runs'          : [],
    'workDir'       : None,
    'outDir'        : None,
    'decDir'        : None,
    'logDir'        : '/farm_out/'+getpass.getuser(),
    'phaseSize'     : -1,
    'mergeSize'     : 5,
    'model'         : None,
    'torus'         : None,
    'solenoid'      : None,
    'mergePattern'  : 'clas_%.6d.evio.%.5d-%.5d.hipo',
    'singlePattern' : 'clas_%.6d.evio.%.5d.hipo',
    'fileRegex'     : RunFileUtil.getFileRegex(),
    'submit'        : False,
    'reconYaml'     : None,
    'trainYaml'     : None,
    'trainSize'     : 30,
    'claraLogDir'   : None,
    'threads'       : 16
}

class ChefConfig:

  def __init__(self,args):

    self._workflow=None

    self.cfg = copy.deepcopy(CFG)

    self.cli = self.getCli()

    self.args = self.cli.parse_args(args)

    if self.args.defaults:
      print(str(self))
      sys.exit()

    if self.args.config is not None:
      self._loadConfigFile(self.args.config)

    self._loadCliArgs()

    self._verifyConfig()

    if self.args.show:
      print(str(self))
      sys.exit()

  def get(self,key):
    return self.cfg[key]

  def getWorkflow(self):
    if self._workflow is None:
      name='%s-%s-%s'%(self.cfg['runGroup'],self.cfg['model'],self.cfg['tag'])
      if self.cfg['phaseSize'] >= 0:
        self._workflow = CLAS12Workflows.RollingRuns(name,self.cfg)
      else:
        self._workflow = CLAS12Workflows.MinimalDependency(name,self.cfg)
    if self._workflow.getFileCount()<1:
      _LOGGER.critical('Found no applicable input files.  Check "inputs" and "run".')
      sys.exit()
    return self._workflow

  def getCli(self):

    cli=argparse.ArgumentParser(description='Generate a CLAS12 SWIF workflow.',
        epilog='(*) = required option for all models, from command-line or config file')

    cli.add_argument('--runGroup',metavar='NAME',help='(*) run group name', type=str, choices=CHOICES['runGroup'], default=None)
    cli.add_argument('--tag',     metavar='NAME',help='(*) workflow name suffix/tag, e.g. v0, automatically prefixed with runGroup and task to define workflow name',  type=str, default=None)
    cli.add_argument('--model', metavar='NAME', help='(*) workflow model ('+'/'.join(MODELS)+')', type=str, choices=CHOICES['model'],default=None)

    cli.add_argument('--inputs', metavar='PATH',help='(*) name of file containing a list of input files, or a directory to be searched recursively for input files, or a shell glob of either.  This option is repeatable.',action='append',type=str,default=[])
    cli.add_argument('--runs',   metavar='RUN/PATH',help='(*) run numbers (e.g. "4013" or "4013,4015" or "3980,4000-4999"), or a file containing a list of run numbers.  This option is repeatable.', action='append', default=[], type=str)

    cli.add_argument('--outDir', metavar='PATH',help='final data location', type=str,default=None)
    cli.add_argument('--decDir', metavar='PATH',help='overrides outDir for decoding', type=str,default=None)
    cli.add_argument('--workDir',metavar='PATH',help='temporary data location (for merging and phased workflows only)', type=str,default=None)
    cli.add_argument('--logDir',metavar='PATH',help='log location (otherwise the SLURM default)', type=str,default=None)

    cli.add_argument('--coatjava',metavar='PATH',help='coatjava install location', type=str,default=None)
    cli.add_argument('--clara',metavar='PATH',help='clara install location', type=str,default=None)

    cli.add_argument('--threads', metavar='#',help='number of Clara threads', type=int, default=None, choices=CHOICES['threads'])
    cli.add_argument('--reconYaml',metavar='PATH',help='recon yaml file', type=str,default=None)
    cli.add_argument('--trainYaml',metavar='PATH',help='train yaml file', type=str,default=None)
    cli.add_argument('--claraLogDir',metavar='PATH',help='location for clara log files', type=str,default=None)

    cli.add_argument('--phaseSize', metavar='#',help='number of files per phase (negative is unphased)', type=int, default=None)
    cli.add_argument('--mergeSize', metavar='#',help='number of files per merge', type=int, default=None)
    cli.add_argument('--trainSize', metavar='#',help='number of files per train', type=int, default=None)

    cli.add_argument('--torus',    metavar='#.#',help='override RCDB torus scale',   type=float, default=None)
    cli.add_argument('--solenoid', metavar='#.#',help='override RCDB solenoid scale',type=float, default=None)

    cli.add_argument('--fileRegex',metavar='REGEX',help='input filename format (for matching run and file numbers)', type=str, default=None)

    cli.add_argument('--config',metavar='PATH',help='load config file (overriden by command line arguments)', type=str,default=None)
    cli.add_argument('--defaults',help='print default config file and exit', action='store_true', default=False)
    cli.add_argument('--show',    help='print config file and exit', action='store_true', default=False)

    cli.add_argument('--submit', help='submit and run jobs immediately', action='store_true', default=False)

    cli.add_argument('--version',action='version',version='0.4')

    return cli

  def _loadConfigFile(self,filename):

    if not os.access(filename,os.R_OK):
      sys.exit('FATAL ERROR:  Config file is not readable:  '+filename)

    try:
      cfg = json.load(open(filename,'r'))
    except:
      print(traceback.format_exc())
      sys.exit('FATAL ERROR: Config file '+filename+' has invalid JSON format.')

    for key,val in cfg.items():
      if key not in self.cfg:
        sys.exit('FATAL ERROR:  Config file contains invalid key:  '+key)
      if key in CHOICES and val not in CHOICES[key]:
        sys.exit('FATAL ERROR:  Config file\'s "%s" must be one of %s'%(key,str(CHOICES[key])))
      self.cfg[key]=val

  def _loadCliArgs(self):
    for key,val in vars(self.args).items():
      if key in self.cfg:
        if val is None:
          continue
        if type(val) is list and len(val)==0:
          continue
        self.cfg[key]=val

  def _verifyConfig(self):

    if self.cfg['model'] is None:
      self.cli.error('"model" must be defined.')

    if self.cfg['runGroup'] is None:
      self.cli.error('"runGroup" must be defined.')

    if self.cfg['tag'] is None:
      self.cli.error('"tag" must be specified.')

    if self.cfg['runs'] is None or len(self.cfg['runs'])<1:
      self.cli.error('"runs" must be defined.')

    if len(self.cfg['inputs'])==0:
      self.cli.error('"inputs" must be specified.')

    # cleanup directory definitions:
    for xx in ['decDir','outDir','workDir','logDir']:
      if self.cfg[xx] is not None:
        if self.cfg[xx]=='None' or self.cfg[xx]=='NULL' or self.cfg[xx]=='null':
          self.cfg[xx]=None
        elif not self.cfg[xx].startswith('/'):
          self.cli.error('"'+xx+'" must be an absolute path, not '+self.cfg[xx])

    # for decoding workflows, assign decDir to outDir if it doesn't exist:
    if self.cfg['model'].find('dec')>=0: 
      if self.cfg['decDir'] is None:
        if self.cfg['outDir'] is None:
          self.cli.error('One of "outDir" or "decDir" must be defined for decoding workflows.')
        else:
          self.cfg['decDir']=self.cfg['outDir']+'/decoded'
          _LOGGER.warning('Using --outDir/decoded for decoding outputs ('+self.cfg['outDir']+')')

    # for non-decoding workflows, require outDir:
    if self.cfg['model']!='dec' and self.cfg['model']!='decmrg': 
      if self.cfg['outDir'] is None:
        self.cli.error('"outDir" must be specified for this workflow.')

    # merging+phased workflows have additional constraints:
    if self.cfg['phaseSize']>=0 and self.cfg['model'].find('mrg')>=0: 

      if self.cfg['workDir'] is None:
        self.cli.error('"workDir" must be defined for phased, merging workflows.')

      if self.cfg['phaseSize']>0 and self.cfg['phaseSize']%self.cfg['mergeSize']!=0:
        self.cli.error('"phaseSize" must be a multiple of "mergeSize".')

      if self.cfg['fileRegex'] != RunFileUtil.getFileRegex():
        self.cli.error('Non-default "fileRegex" is not allowed in merging workflows.')

    else:
      if self.cfg['workDir'] is not None:
        _LOGGER.warning('Ignoring --workDir for non-merging, non-phased workflow.')
        self.cfg['workDir']=None

    # set user-defined regex for input files:
    if self.cfg['fileRegex'] != RunFileUtil.getFileRegex():
      RunFileUtil.setFileRegex(self.cfg['fileRegex'])

    # check for clara:
    if self.cfg['model'].find('rec')>=0 or self.cfg['model'].find('ana')>=0:
      if self.cfg['clara'] is None:
        self.cli.error('"clara" must be defined for model='+str(self.cfg['model']))
      if not os.path.exists(self.cfg['clara']):
        self.cli.error('"clara" does not exist: '+self.cfg['clara'])

    # check for coatjava
    if self.cfg['model'].find('dec')>=0 or self.cfg['model'].find('mrg')>=0: 
      if self.cfg['coatjava'] is None:
        if self.cfg['clara'] is not None:
          _LOGGER.warning('Using coatjava from clara: '+self.cfg['clara'])
          self.cfg['coatjava']=self.cfg['clara']+'/plugins/clas12'
        else:
          self.cli.error('"coatjava" must be defined for model='+str(self.cfg['model']))
      if not os.path.exists(self.cfg['coatjava']):
        self.cli.error('"coatjava" does not exist: '+self.cfg['coatjava'])

    # check yaml files:
    if self.cfg['model'].find('ana')>=0: 
      if self.cfg['trainYaml'] is None:
        self.cli.error('"trainYaml" must be defined for model='+str(self.cfg['model']))
      elif not os.path.exists(self.cfg['trainYaml']):
        self.cli.error('"trainYaml" does not exist:  '+self.cfg['trainYaml'])
    if self.cfg['model'].find('rec')>=0: 
      if self.cfg['reconYaml'] is None:
        self.cli.error('"reconYaml" must be defined for model='+str(self.cfg['model']))
      elif not os.path.exists(self.cfg['reconYaml']):
        self.cli.error('"reconYaml" does not exist:  '+self.cfg['reconYaml'])

    # parse run list:
    self.cfg['runs'] = ChefUtil.getRunList(self.cfg['runs'])
    if self.cfg['runs'] is None or len(self.cfg['runs'])==0:
      self.cli.error('\nFound no runs.  Check --inputs and --runs.')


  def __str__(self):
    return json.dumps(self.cfg,indent=2,separators=(',',': '),sort_keys=True)

  def __dict__(self):
    return self.cfg

if __name__ == '__main__':
  cc=ChefConfig(sys.argv[1:])
  print(str(cc))

