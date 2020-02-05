import os,sys,glob,json,copy,logging,getpass,argparse,traceback,collections
import ChefUtil
import RunFileUtil
import CLAS12Workflows

_LOGGER=logging.getLogger(__name__)
_TOPDIR = os.path.dirname(os.path.realpath(__file__)).rstrip('lib/clas12')

CHOICES={
    'model'   : ['dec','decmrg','rec','ana','decrec','decmrgrec','recana','decrecana','decmrgrecana'],
    'runGroup': ['rga','rgb','rgc','rgd','rge','rgf','rgk','rgm','rgl','test'],
    'threads' : [16, 20, 24, 32],
    'node'    : ['general','centos77','centos72','farm19','farm18','farm16','farm14','farm13','qcd12s','amd','xeon']
}

CFG=collections.OrderedDict()
CFG['runGroup']     = None
CFG['tag']          = None
CFG['node']         = 'general'
CFG['model']        = None
CFG['reconYaml']    = None
CFG['project']      = 'clas12'
CFG['trainYaml']    = None
CFG['coatjava']     = None
CFG['clara']        = None
CFG['inputs']       = []
CFG['runs']         = []
CFG['workDir']      = None
CFG['outDir']       = None
CFG['decDir']       = None
CFG['phaseSize']    = -1
CFG['mergeSize']    = 5
CFG['trainSize']    = 30
CFG['threads']      = 16
CFG['torus']        = None
CFG['solenoid']     = None
CFG['postproc']     = False
CFG['claraLogDir']  = None
CFG['logDir']       = '/farm_out/'+getpass.getuser()
CFG['submit']       = False
CFG['fileRegex']    = RunFileUtil.getFileRegex()
CFG['mergePattern'] = 'clas_%.6d.evio.%.5d-%.5d.hipo'
CFG['singlePattern']= 'clas_%.6d.evio.%.5d.hipo'
CFG['ignored']      = {}

# override default project for priority accounts:
if getpass.getuser() in ['clas12','clas12-1','clas12-2','clas12-3','clas12-4','clas12-5','hps']:
  CFG['project']='hallb-pro'

class ChefConfig(collections.OrderedDict):

  def __str__(self):
    return json.dumps(self,indent=2,separators=(',',': '),sort_keys=False)

  def __init__(self,args):
    if isinstance(args,dict):
      collections.OrderedDict.__init__(self,args)
    else:
      collections.OrderedDict.__init__(self,copy.deepcopy(CFG))
      self._workflow=None
      self.cli = self.getCli()
      self.args = self.cli.parse_args(args)
      if self.args.defaults:
        print(str(self))
        sys.exit()
      if self.args.config is not None:
        self._loadConfigFile(self.args.config)
      self._loadCliArgs()
      self._verifyConfig()
      self._storeYamls()
      if self.args.show:
        c=copy.deepcopy(collections.OrderedDict(self))
        c.pop('ignored')
        print(json.dumps(c,indent=2,separators=(',',': '),sort_keys=False))
        sys.exit()

  def __eq__(self,cfg):
    # equality is based only on things that would change the output data
    for k in ['clara','coatjava','reconYaml','trainYaml','outDir','mergeSize']:
      if self[k] != None and cfg[k] != None:
        if self[k] != cfg[k]:
          return False
    return True

  def __ne__(self,cfg):
    return not self.__eq__(cfg)

  def getReadme(self):
    c=copy.deepcopy(collections.OrderedDict(self))
    c.pop('inputs')
    c.pop('runs')
    return json.dumps(c,indent=2,separators=(',',': '),sort_keys=False)

  def append(self,cfg):
    for k,v in cfg.items():
      if k not in self or self[k] is None:
        self[k]=v

  def _storeYamls(self):
    for x in ['reconYaml','trainYaml']:
      if self[x] is not None:
        with open(self[x],'r') as f:
          self['ignored'][x]=[y.strip('\n') for y in f.readlines()]

  def _checkYamls(self):
    for x in ['reconYaml','trainYaml']:
      if self[x] is None:
        continue
      if not os.path.isfile(self[x]):
        yamlprefix = '%s/yamls/%s_'%(_TOPDIR,x.replace('Yaml',''))
        if os.path.isfile(yamlprefix+self[x]+'.yaml'):
          self[x] = yamlprefix+self[x]+'.yaml'
          _LOGGER.warning('Using stock yaml: '+self[x])
        else:
          _LOGGER.critical('Nonexistent yaml: '+self[x])
          sys.exit()
      if x=='reconYaml':
        good=False
        with open(self[x],'r') as f:
          for line in f.readlines():
            if line.strip().find('schema_dir: ')==0:
              cols=line.strip().split()
              if len(cols)<2:
                _LOGGER.critical('Undefined schema_dir in '+self[x])
                sys.exit()
              elif os.path.isdir(cols[1].strip('"')):
                good=True
              else:
                _LOGGER.critical('Invalid schema_dir in '+self[x]+':')
                _LOGGER.critical('  '+cols[1].strip('"'))
                sys.exit()
        if not good:
          _LOGGER.warning('No schema_dir defined in '+self[x])

  def compactModel(self):
    x=self['model']
    for y in ['dec','mrg','rec','ana']:
      x=x.replace(y,y[0])
    return x

  def getWorkflow(self):
    if self._workflow is None:
      name='%s-%s-%s'%(self['runGroup'],self.compactModel(),self['tag'])
      if self['phaseSize'] >= 0:
        self._workflow = CLAS12Workflows.RollingRuns(name,self)
      else:
        self._workflow = CLAS12Workflows.MinimalDependency(name,self)
    if self._workflow.getFileCount()<1:
      _LOGGER.critical('Found no applicable input files.  Check "inputs" and "run".')
      sys.exit()
    return self._workflow

  def getCli(self):

    stockReconYamls=[os.path.basename(x).lstrip('recon_').rstrip('.yaml') for x in glob.glob(_TOPDIR+'/yamls/recon_*.yaml')]
    stockTrainYamls=[os.path.basename(x).lstrip('train_').rstrip('.yaml') for x in glob.glob(_TOPDIR+'/yamls/train_*.yaml')]

    cli=argparse.ArgumentParser(description='Generate a CLAS12 SWIF workflow.',
        epilog='(*) = required option for all models, from command-line or config file')

    cli.add_argument('--runGroup',metavar='NAME',help='(*) run group name', type=str, choices=CHOICES['runGroup'], default=None)
    cli.add_argument('--tag',     metavar='NAME',help='(*) e.g. pass1v0, automatically prefixed with runGroup and suffixed by model to define workflow name',  type=str, default=None)
    cli.add_argument('--model', metavar='NAME', help='(*) workflow model ('+'/'.join(CHOICES['model'])+')', type=str, choices=CHOICES['model'],default=None)

    cli.add_argument('--inputs', metavar='PATH',help='(*) name of file containing a list of input files, or a directory to be searched recursively for input files, or a (quoted) shell glob of either.  This option is repeatable.',action='append',type=str,default=[])
    cli.add_argument('--runs',   metavar='RUN/PATH',help='(*) run numbers (e.g. "4013" or "4013,4015" or "3980,4000-4999"), or a file containing a list of run numbers.  This option is repeatable.', action='append', default=[], type=str)

    cli.add_argument('--outDir', metavar='PATH',help='final data location', type=str,default=None)
    cli.add_argument('--decDir', metavar='PATH',help='overrides outDir for decoding', type=str,default=None)
    cli.add_argument('--workDir',metavar='PATH',help='temporary data location for single decoded/train files before merging', type=str,default=None)
    cli.add_argument('--logDir',metavar='PATH',help='log location (otherwise the SLURM default)', type=str,default=None)

    cli.add_argument('--coatjava',metavar='PATH',help='coatjava install location', type=str,default=None)
    cli.add_argument('--clara',metavar='PATH',help='clara install location', type=str,default=None)

    cli.add_argument('--threads', metavar='#',help='number of Clara threads', type=int, default=None, choices=CHOICES['threads'])
    cli.add_argument('--reconYaml',metavar='PATH',help='recon yaml file (stock options = %s)'%('/'.join(stockReconYamls)), type=str,default=None)
    cli.add_argument('--trainYaml',metavar='PATH',help='train yaml file (stock options = %s)'%('/'.join(stockTrainYamls)), type=str,default=None)
    cli.add_argument('--claraLogDir',metavar='PATH',help='location for clara log files', type=str,default=None)

    cli.add_argument('--phaseSize', metavar='#',help='number of files per phase (negative is unphased)', type=int, default=None)
    cli.add_argument('--mergeSize', metavar='#',help='number of decoded files per merge', type=int, default=None)
    cli.add_argument('--trainSize', metavar='#',help='number of files per train', type=int, default=None)

    cli.add_argument('--postproc', help='enable post-processing of helicity and beam charge', action='store_true', default=None)

    cli.add_argument('--torus',    metavar='#.#',help='override RCDB torus scale',   type=float, default=None)
    cli.add_argument('--solenoid', metavar='#.#',help='override RCDB solenoid scale',type=float, default=None)

    cli.add_argument('--fileRegex',metavar='REGEX',help='input filename format (for matching run and file numbers)', type=str, default=None)

    cli.add_argument('--config',metavar='PATH',help='load config file (overriden by command line arguments)', type=str,default=None)
    cli.add_argument('--defaults',help='print default config file and exit', action='store_true', default=False)
    cli.add_argument('--show',    help='print config file and exit', action='store_true', default=False)

    cli.add_argument('--node', metavar='NAME',help='batch farm node type (os/feature)', type=str, default=None, choices=CHOICES['node'])

    cli.add_argument('--submit', help='submit and run jobs immediately', action='store_true', default=False)

    cli.add_argument('--version',action='version',version='clas12-workflow/0.9')

    return cli

  def _loadConfigFile(self,filename):

    if not os.access(filename,os.R_OK):
      _LOGGER.critical('Config file is not readable:  '+filename)
      sys.exit()

    try:
      cfg = json.load(open(filename,'r'))
    except:
      print(traceback.format_exc())
      _LOGGER.critical('Config file has invalid JSON format:  '+filename)
      sys.exit()

    for key,val in cfg.items():
      if key not in self:
        _LOGGER.critical('Config file contains invalid key:  '+key)
        sys.exit()
      if key in CHOICES and val not in CHOICES[key]:
        _LOGGER.critical('Config file\'s "%s" must be one of %s'%(key,str(CHOICES[key])))
        sys.exit()
      self[key]=val

  def _loadCliArgs(self):
    for key,val in vars(self.args).items():
      if key in self:
        if val is None:
          continue
        if type(val) is list and len(val)==0:
          continue
        self[key]=val

  def _verifyConfig(self):

    if self['model'] is None:
      self.cli.error('"model" must be defined.')

    if self['runGroup'] is None:
      self.cli.error('"runGroup" must be defined.')

    if self['tag'] is None:
      self.cli.error('"tag" must be specified.')

    if self['runs'] is None or len(self['runs'])<1:
      self.cli.error('"runs" must be defined.')

    if len(self['inputs'])==0:
      self.cli.error('"inputs" must be specified.')

    # cleanup directory definitions:
    for xx in ['decDir','outDir','workDir','logDir']:
      if self[xx] is not None:
        if self[xx]=='None' or self[xx]=='NULL' or self[xx]=='null':
          self[xx]=None
        elif not self[xx].startswith('/'):
          self.cli.error('"'+xx+'" must be an absolute path, not '+self[xx])

    # for decoding workflows, assign decDir to outDir if it doesn't exist:
    if self['model'].find('dec')>=0:
      if self['decDir'] is None:
        if self['outDir'] is None:
          self.cli.error('One of "outDir" or "decDir" must be defined for decoding workflows.')
        else:
          self['decDir']=self['outDir']+'/decoded'
          _LOGGER.warning('Using --outDir/decoded for decoding outputs ('+self['outDir']+')')

    # for non-decoding workflows, require outDir:
    if self['model']!='dec' and self['model']!='decmrg':
      if self['outDir'] is None:
        self.cli.error('"outDir" must be specified for this workflow.')

    # merging+phased workflows have additional constraints:
    if self['phaseSize']>=0 and self['model'].find('mrg')>=0:

      if self['workDir'] is None:
        self.cli.error('"workDir" must be defined for phased, merging workflows.')

      if self['phaseSize']>0 and self['phaseSize']%self['mergeSize']!=0:
        self.cli.error('"phaseSize" must be a multiple of "mergeSize".')

      if self['fileRegex'] != RunFileUtil.getFileRegex():
        self.cli.error('Non-default "fileRegex" is not allowed in merging workflows.')

    else:
      if self['workDir'] is not None and self['model'].find('ana')<0:
        _LOGGER.warning('Ignoring --workDir for non-merging, non-phased, trainless workflow.')
        self['workDir']=None

    # no temporary files on /cache or mss
    if self['workDir'] is not None:
      if self['workDir'].find('/cache')==0 or self['workDir'].find('/mss')==0:
        self.cli.error('--workDir cannot be on /cache or /mss.')
    if self['model'].find('ana')>=0:
      if self['outDir'].find('/cache')==0 or self['outDir'].find('/mss')==0:
        if self['workDir'] is None:
          self.cli.error('--workDir is required for trains if --outDir is on /cache or /mss')

    # set user-defined regex for input files:
    if self['fileRegex'] != RunFileUtil.getFileRegex():
      RunFileUtil.setFileRegex(self['fileRegex'])

    # check for clara:
    if self['model'].find('rec')>=0 or self['model'].find('ana')>=0:
      if self['clara'] is None:
        self.cli.error('"clara" must be defined for model='+str(self['model']))
      if not os.path.exists(self['clara']):
        self.cli.error('"clara" does not exist: '+self['clara'])

    # check for coatjava
    if self['model'].find('dec')>=0 or self['model'].find('mrg')>=0 or self['postproc'] or self['model'].find('ana')>=0:
      if self['coatjava'] is None:
        if self['clara'] is not None:
          _LOGGER.warning('Using coatjava from clara: '+self['clara'])
          self['coatjava']=self['clara']+'/plugins/clas12'
        else:
          self.cli.error('"coatjava" must be defined for model='+str(self['model']))
      if not os.path.exists(self['coatjava']):
        self.cli.error('"coatjava" does not exist: '+self['coatjava'])

    # check yaml files:
    if self['model'].find('ana')>=0:
      if self['trainYaml'] is None:
        self.cli.error('"trainYaml" must be defined for model='+str(self['model']))
      if self['reconYaml'] is None:
        self.cli.error('"reconYaml" must be defined for model='+str(self['model']))
    if self['model'].find('rec')>=0 and self['reconYaml'] is None:
      self.cli.error('"reconYaml" must be defined for model='+str(self['model']))
    if self['reconYaml'] is not None:
      self['schema']=ChefUtil.getSchemaName(self['reconYaml'])
    self._checkYamls()

    # parse run list:
    self['runs'] = ChefUtil.getRunList(self['runs'])
    if self['runs'] is None or len(self['runs'])==0:
      self.cli.error('\nFound no runs.  Check --inputs and --runs.')

    # check post-processing:
    if self['postproc']:
      if self['model'].find('rec')<0:
        self['postproc']=False
        self.cli.warning('Ignoring "postproc" for non-rec workflow.')
      else:
        # check for suffiecient coatjava version:
        # FIXME: remove this check eventually
        cjv=ChefUtil.getCoatjavaVersion(self['coatjava'])
        if cjv is None:
          self.cli.error('Could not determine coatjava version.')
        if cjv[0]<6:
          self.cli.error('Post-processing requires coatjava>6b.4.1')
        elif cjv[0]==6:
          if cjv[1]<4:
            self.cli.error('Post-processing requires coatjava>6b.4.1')
          elif cjv[1]==4 and cjv[2]<1:
            self.cli.error('Post-processing requires coatjava>6b.4.1')
        # not ready for 120 Hz:
        for run in self['runs']:
          if run>11000 and cjv[0]==6 and cjv[1]<5:
            self.cli.critical('Post-processing 120 Hz helicity requires coatjava>6b.5.0.')

if __name__ == '__main__':
  cc=ChefConfig(sys.argv[1:])
  print(str(cc))

