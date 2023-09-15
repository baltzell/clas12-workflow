import os,re,sys,stat,glob,json,copy,logging,getpass,argparse,traceback,collections
import ChefUtil
import CoatjavaVersion
import RunFileUtil
import CLAS12Workflows
import ClaraYaml
from CLAS12Jobs import ReconJob

_LOGGER=logging.getLogger(__name__)
_TOPDIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__))+'/../../')
_JSONFORMAT={'indent':2,'separators':(',',': '),'sort_keys':False}
_VALIDREMOTES=['/mss','/volatile','/cache']

CHOICES={
'model'   : ['dec','decmrg','rec','ana','decrec','decmrgrec','recana','decrecana','decmrgrecana','his'],
'runGroup': ['era','rga','rgb','rgc','rgd','rge','rgf','rgk','rgm','rgl','test'],
'node'    : ['general','centos77','farm19','farm18','farm16','farm14','farm13','amd','xeon'],
'threads' : list(ReconJob.THRD_MEM_REQ.keys()),
}

STOCK_TRAIN_YAMLS={}
for x in sorted(glob.glob(_TOPDIR+'/yamls/train_*.yaml')):
  STOCK_TRAIN_YAMLS[os.path.basename(x)[6:][:-5]] = x

CFG=json.load(open(_TOPDIR+'/lib/clas12/defaults.json','r'))

if CFG['logDir'] is None:
  CFG['logDir'] = '/farm_out/'+getpass.getuser()

# override default project for priority accounts:
if getpass.getuser() in ['clas12','clas12-1','clas12-2','clas12-3','clas12-4','clas12-5','clas12-6','clas12-7','clas12-8','clas12-9','hps']:
  CFG['project']='hallb-pro'

def compactModel(model):
  x=model
  for y in ['dec','mrg','rec','his','ana']:
    x=x.replace(y,y[0])
  return x

def fullModel(model):
  x=''
  for y in ['dec','mrg','rec','his','ana']:
    if y[0] in model:
      x+=y
  return x

class ChefConfig(collections.OrderedDict):

  def __str__(self):
    return json.dumps(self,**_JSONFORMAT)

  def __init__(self,args):
    if isinstance(args,dict):
      collections.OrderedDict.__init__(self,args)
    else:
      collections.OrderedDict.__init__(self,copy.deepcopy(CFG))
      self._workflow=None
      self.cli = self.getCli()
      self.args = self.cli.parse_args(args)
      if self.args.defaults:
        print((str(self)))
        sys.exit()
      if self.args.config is not None:
        self._loadConfigFile(self.args.config)
      self._loadCliArgs()
      self._verifyConfig()
      self._storeYamls()
      if self.args.lowpriority:
        self['project']='clas12'
      if self.args.show:
        c=copy.deepcopy(collections.OrderedDict(self))
        c.pop('ignored')
        print((json.dumps(c,**_JSONFORMAT)))
        sys.exit(0)
      if self['model'].find('rec') >= 0:
        cjv=CoatjavaVersion.CoatjavaVersion(self['clara'])
        if self['denoise']:
          if cjv < '9.0.0':
            self['denoise'] = 1.9
          else:
            self['denoise'] = 2.0
      if self['helflip']:
        _LOGGER.info('--helflip is currently not allowed')
        sys.exit(1)

  def diff(self,cfg):
    ret = []
    for k in ['clara','coatjava','reconYaml','trainYaml','mergeSize','nopostproc','helflip','recharge','ccdbsqlite']:
      if self.get(k) != None and cfg.get(k) != None:
        if k=='clara' or k=='coatjava':
          if os.path.normpath(self[k]) != os.path.normpath(cfg[k]):
            ret.append(k)
        elif self[k] != cfg[k]:
          ret.append(k)
    return ret

  def __eq__(self,cfg):
    return len(self.diff(cfg)) == 0

  def __ne__(self,cfg):
    return not self.__eq__(cfg)

  def getReadme(self):
    c=copy.deepcopy(collections.OrderedDict(self))
    c.pop('inputs')
    c.pop('runs')
    return json.dumps(c,**_JSONFORMAT)

  def append(self,cfg):
    for k,v in list(cfg.items()):
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
      elif x is 'trainYaml' and self[x] in STOCK_TRAIN_YAMLS:
        _LOGGER.info('Using stock train yaml: '+self[x])
        self[x] = STOCK_TRAIN_YAMLS[self[x]]
      else:
        if not os.path.isfile(self[x]):
          _LOGGER.critical('Nonexistent user yaml: '+self[x])
          sys.exit(1)
        self[x] = os.path.abspath(self[x])
        # set it to read-only:
        try:
          os.chmod(self[x], stat.S_IRUSR|stat.S_IRGRP|stat.S_IROTH)
        except OSError as e:
          _LOGGER.warning('You do not own your YAML, someone else can change it later(!): '+self[x])
          pass
      if x=='reconYaml':
        good=False
        with open(self[x],'r') as f:
          for line in f.readlines():
            if line.strip().find('schema_dir: ')==0:
              cols=line.strip().split()
              if len(cols)<2:
                _LOGGER.critical('Undefined schema_dir in '+self[x])
                sys.exit(1)
              elif os.path.isdir(cols[1].strip('"')):
                good=True
              else:
                _LOGGER.critical('Invalid schema_dir in '+self[x]+':')
                _LOGGER.critical('  '+cols[1].strip('"'))
                sys.exit(1)
        if not good:
          _LOGGER.warning('No schema_dir defined in '+self[x])

  def getWorkflow(self):
    if self._workflow is None:
      name='%s-%s-%s'%(self['runGroup'],compactModel(self['model']),self['tag'])
      if self['phaseSize']>0:
        self._workflow = CLAS12Workflows.RollingRuns(name,self)
      else:
        self._workflow = CLAS12Workflows.MinimalDependency(name,self)
    if self._workflow.getFileCount()<1:
      _LOGGER.critical('Found no applicable input files.  Check "inputs" and "run".')
      sys.exit(1)
    return self._workflow

  def getCli(self):

    cli=argparse.ArgumentParser(description='Generate a CLAS12 SWIF workflow.',
        epilog='(*) = required option for all models, from command-line or config file')

    cli.add_argument('--runGroup',metavar='NAME',help='(*) run group name', type=str, choices=CHOICES['runGroup'], default=None)
    cli.add_argument('--tag',     metavar='NAME',help='(*) e.g. pass1v0, automatically prefixed with runGroup and suffixed by model to define workflow name',  type=str, default=None)
    cli.add_argument('--model', metavar='NAME', help='(*) workflow model ('+'/'.join(CHOICES['model'])+')', type=str, choices=CHOICES['model'],default=None)

    cli.add_argument('--inputs', metavar='PATH',help='(*) name of file containing a list of input files, or a directory to be searched recursively for input files, or a (quoted) shell glob of either.  This option is repeatable.',action='append',type=str,default=[])
    cli.add_argument('--runs',   metavar='RUN/PATH',help='(*) run numbers (e.g. "4013" or "4013,4015" or "3980,4000-4999"), or a file containing a list of run numbers.  This option is repeatable.', action='append', default=[], type=str)

    cli.add_argument('--outDir', metavar='PATH',help='final data location', type=str,default=None)
    cli.add_argument('--decDir', metavar='PATH',help='overrides outDir for decoding', type=str,default=None)
    cli.add_argument('--trainDir', metavar='PATH',help='overrides outDir for trains', type=str,default=None)
    cli.add_argument('--workDir',metavar='PATH',help='temporary data location for single decoded/train files before merging', type=str,default=None)
    if getpass.getuser().find('clas12-')<0 and getpass.getuser().find('hps')<0:
      cli.add_argument('--logDir',metavar='PATH',help='log location (otherwise the SLURM default)', type=str,default=None)

    cli.add_argument('--coatjava',metavar='VERSION/PATH',help='coatjava version number (or install location)', type=str,default=None)
    cli.add_argument('--clara',metavar='PATH',help='clara install location (unnecessary if coatjava is specified as a VERSION)', type=str,default=None)

    cli.add_argument('--threads', metavar='#',help='number of Clara threads', type=int, default=None, choices=CHOICES['threads'])
    cli.add_argument('--reconYaml',metavar='PATH',help='absolute path to recon yaml file', type=str,default=None)
    cli.add_argument('--trainYaml',metavar='PATH',help='absolute path to train yaml file (or a stock option: %s)'%('/'.join(STOCK_TRAIN_YAMLS.keys())), type=str,default=None)

    cli.add_argument('--phaseSize', metavar='#',help='number of files (or runs if less than 100) per phase, while negative is unphased', type=int, default=None)
    cli.add_argument('--mergeSize', metavar='#',help='number of decoded files per merge', type=int, default=None)
    cli.add_argument('--trainSize', metavar='#',help='number of files per train job', type=int, default=None)

#    if getpass.getuser().find('clas12-')<0:
    cli.add_argument('--reconSize', metavar='#',help='number of files per recon job', type=int, default=None)

    cli.add_argument('--denoise', help='enable DC denoising', default=False, action='store_true')
    cli.add_argument('--nopostproc', help='disable post-processing of helicity and beam charge', action='store_true', default=None)
    cli.add_argument('--recharge', help='rebuild RUN::scaler during post-processing', action='store_true', default=None)
    cli.add_argument('--helflip',  help='flip offline helicity (ONLY for data decoded prior to 6.5.11)', action='store_true', default=None)
    cli.add_argument('--noheldel', help='disable delayed-helicity correction', action='store_true', default=None)

    cli.add_argument('--timelinePhysics', help='for \'his\' model, produce physics timelines instead of detector timelines', default=False, action='store_true')

    cli.add_argument('--ccdbsqlite',metavar='PATH',help='path to CCDB sqlite file (default = mysql database)', type=str, default=None)

    cli.add_argument('--torus',    metavar='#.#',help='override RCDB torus scale',   type=float, default=None)
    cli.add_argument('--solenoid', metavar='#.#',help='override RCDB solenoid scale',type=float, default=None)

    cli.add_argument('--fileRegex',metavar='REGEX',help='input filename format for matching run and file numbers, default="%s"'%CFG['fileRegex'], type=str, default=None)
    cli.add_argument('--forties', help='set --fileRegex for files in the 40s', default=False, action='store_true')
    cli.add_argument('--graalvm', help='use GraalVM instead of JVM', default=False, action='store_true')

    cli.add_argument('--lowpriority',help='run with non-priority fairshare', default=False, action='store_true')
    cli.add_argument('--node', metavar='NAME',help='batch farm node type (os/feature)', type=str, default=None, choices=CHOICES['node'])

    cli.add_argument('--config',metavar='PATH',help='load config file (overriden by command line arguments)', type=str,default=None)
    cli.add_argument('--defaults',help='print default config file and exit', action='store_true', default=False)
    cli.add_argument('--show',    help='print config file and exit', action='store_true', default=False)
    cli.add_argument('--submit', help='submit and run jobs immediately', action='store_true', default=False)
    cli.add_argument('--hattawy', help='rigorous, slow disk request calculation', action='store_true', default=False)
    cli.add_argument('--version',action='version',version='clas12-workflow/0.99')

    return cli

  def _loadConfigFile(self,filename):

    if not os.access(filename,os.R_OK):
      _LOGGER.critical('Config file is not readable:  '+filename)
      sys.exit(1)

    try:
      cfg = json.load(open(filename,'r'))
      # strip out deprecated option:
      cfg.pop('postproc',None)
    except:
      print((traceback.format_exc()))
      _LOGGER.critical('Config file has invalid JSON format:  '+filename)
      sys.exit(1)

    int_keys=[]
    for key,val in list(CFG.items()):
      try:
        int(val)
        int_keys.append(key)
      except:
        pass

    for key,val in list(cfg.items()):
      if key not in self:
        _LOGGER.critical('Config file contains invalid key:  '+key)
        sys.exit(1)
      if key in CHOICES and val not in CHOICES[key]:
        _LOGGER.critical('Config file\'s "%s" must be one of %s'%(key,str(CHOICES[key])))
        sys.exit(1)
      if key in int_keys:
        try:
          int(val)
        except:
          _LOGGER.critical('Config file\'s "'+key+'" must be an integer: '+val)
          sys.exit(1)
      self[key]=val

  def _loadCliArgs(self):
    for key,val in list(vars(self.args).items()):
      if key in self:
        # FIXME:  None is a special default case, ignore it:
        if val is None:
          continue
        if type(val) is list and len(val)==0:
          continue
        # FIXME: booleans with default=False and store_true are a special default case, ignore it:
        if type(val) is bool and val is False and key in self:
          continue
        self[key]=val

  def _verifyConfig(self):

    if self['tag'] is None:
      self.cli.error('"tag" must be specified.')

    if self['forties']:
      self['fileRegex'] = '.*clas[_A-Za-z]*_(\d+)\.evio\.(0004\d+)'

    if re.match('^[a-zA-Z0-9._]+$',self['tag']) is None:
      self.cli.error('only letters, numbers, periods, and underscords are allowed in "--tag"')

    if self['model'] is None:
      self.cli.error('"model" must be defined.')

    if self['runGroup'] is None:
      self.cli.error('"runGroup" must be defined.')

    if self['runs'] is None or len(self['runs'])<1:
      self.cli.error('"runs" must be defined.')

    if len(self['inputs'])==0:
      self.cli.error('"inputs" must be specified.')
    else:
      for x in self['inputs']:
        if x.startswith('/cache'):
          _LOGGER.warning('Some of your --inputs start with /cache, which is almost never a good idea.')
          _LOGGER.warning('Are you sure you REALLY want to do that, rather than /mss?')
          break

    # print ignoring decoding-specific parameters:
    if self['model'].find('dec')<0:
      for x in 'mergeSize','decDir','torus','solenoid':
        if self[x] != CFG[x]:
          _LOGGER.warning('Ignoring custom --%s option since not decoding.'%x)

    # print ignoring train-specific parameters:
    if self['model'].find('ana')<0:
      for x in 'trainSize','trainDir','trainYaml':
        if self[x] != CFG[x]:
          _LOGGER.warning('Ignoring custom --%s option since not running trains.'%x)

    # print ignoring recon-specific parameters:
    if self['model'].find('rec')<0:
      for x in 'threads','reconYaml','nopostproc','helflip','recharge','noheldel':
        if self[x] != CFG[x]:
          _LOGGER.warning('Ignoring custom --%s option since not running recon.'%x)
      self['reconYaml'] = CFG['reconYaml']
    elif self['nopostproc']:
      for x in 'helflip','recharge','noheldel':
        if self[x] != CFG[x]:
          _LOGGER.warning('Ignoring custom --%s option since postprocessing is disabled.')

    # print ignoring work dir:
    if self['workDir'] is not None:
      if self['model'].find('ana')<0 and self['model'].find('mrg')<0:
        _LOGGER.warning('Ignoring --workDir for non-decoding-merging, trainless workflow.')
        self['workDir']=None

    # cleanup directory definitions:
    for xx in ['decDir','outDir','workDir','logDir','trainDir']:
      if self[xx] is not None:
        if self[xx]=='None' or self[xx]=='NULL' or self[xx]=='null':
          self[xx]=None
        elif not self[xx].startswith('/'):
          self.cli.error('"'+xx+'" must be an absolute path, not '+self[xx])
        elif xx is not 'logDir':
          if '/'+self[xx].strip('/').split('/').pop(0) not in _VALIDREMOTES:
            self.cli.error('"'+xx+'" must start with one of: '+','.join(_VALIDREMOTES))

    # for decoding workflows, assign decDir to outDir if it doesn't exist:
    if self['model'].find('dec')>=0:
      if self['decDir'] is None:
        if self['outDir'] is None:
          self.cli.error('One of "outDir" or "decDir" must be defined for decoding workflows.')
        else:
          self['decDir']=self['outDir']+'/decoded'
          _LOGGER.warning('Using --outDir for decoding outputs ('+self['outDir']+')')

    # for train workflows, assign trainDir to outDir if it doesn't exist:
    if self['model'].find('ana')>=0:
      if self['trainDir'] is None:
        if self['outDir'] is None:
          self.cli.error('One of "outDir" or "trainDir" must be defined for ana train workflows.')
        else:
          self['trainDir']=self['outDir']
          _LOGGER.info('Using --outDir for train outputs ('+self['outDir']+')')

    # for non-decoding workflows, require outDir:
    if self['model']!='dec' and self['model']!='decmrg':
      if self['outDir'] is None:
        self.cli.error('"outDir" must be specified for this workflow.')

    if self['reconSize']<1:
      self.cli.error('Invalid reconSize:  '+str(self['reconSize']))

    # before switchingn to run-phasing, phaseSize of 0 meant 1 run per phase,
    # swap it here to keep that meaning the same:
    if self['phaseSize']==0:
      self['phaseSize']=1
    if self['phaseSize']>1 and self['phaseSize']<3000:
      _LOGGER.warning('Increasing user-requested positive "phaseSize" to 3000')

    # print workflow dependency model info:
    if self['phaseSize']<0:
      _LOGGER.info('Using only job-job dependencies, no phases.')
    elif self['phaseSize']<CLAS12Workflows.RUN_PHASING_CUTOFF:
      _LOGGER.info('Using '+str(self['phaseSize'])+' *runs* per phase.')
    else:
      _LOGGER.info('Using '+str(self['phaseSize'])+' *files* per phase.')

    # merging+phased workflows have additional constraints:
    if self['model'].find('mrg')>=0 and self['fileRegex']!=RunFileUtil.getFileRegex():
      self.cli.error('Non-default "fileRegex" is not allowed in merging workflows.')

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

    # check sqlite file:
    if self['ccdbsqlite'] is not None:
      self['ccdbsqlite'] = os.path.abspath(self['ccdbsqlite'])
      if not os.path.isfile(self['ccdbsqlite']):
        self.cli.error('--ccdbsqlite file does not exist:  '+self['ccdbsqlite'])

    # let user specify version number instead of path:
    if self['coatjava'] is not None and not self['coatjava'].startswith('/'):
      _LOGGER.info('Interpreting --coatjava as a version number:  '+self['coatjava'])
      claras=CoatjavaVersion.getCoatjavaVersions()
      if self['coatjava'] in claras:
        path = claras[self['coatjava']]['path']
        if self['clara'] is None:
          _LOGGER.info('Assuming the CLARA install containing --coatjava:  '+path)
          self['clara']=os.path.normpath(path)
        self['coatjava']=os.path.normpath(path+'/plugins/clas12')
      else:
        self.cli.error('Coatjava version not found: '+self['coatjava']+'\n'+
            'Avaliable coatjava versions:  '+' '.join(reversed(sorted(claras.keys()))))

    # use coatjava from clara if coatjava isn't defined:
    if self['coatjava'] is None:
      if self['clara'] is not None:
        _LOGGER.info('Using COATJAVA from CLARA installation:  '+self['clara'])
        self['coatjava']=self['clara']+'/plugins/clas12'
      else:
        self.cli.error('You must define at least one of --coatjava or --clara.')

    # check for coatjava:
    if not os.path.exists(self['coatjava']):
      self.cli.error('COATJAVA does not exist: '+self['coatjava'])
    else:
      n = len(glob.glob(self['coatjava']+'/lib/clas/coat-libs*.jar'))
      if n < 1:
        self.cli.error('COATJAVA has insufficient libraries:  '+self['coatjava'])
      elif n > 1:
        self.cli.error('COATJAVA has too many library versions:  '+self['coatjava'])

    # check for clara:
    if self['model'].find('rec')>=0 or self['model'].find('ana')>=0:
      if self['clara'] is None:
        self.cli.error('--clara must be defined for model='+str(self['model']))
      if not os.path.exists(self['clara']):
        self.cli.error('CLARA does not exist:  '+self['clara'])

    # check yaml files:
    self._checkYamls()
    if self['model'].find('ana')>=0 and self['trainYaml'] is None:
        self.cli.error('"trainYaml" must be defined for model='+str(self['model']))
    if self['model'].find('rec')>=0 and self['reconYaml'] is None:
      self.cli.error('"reconYaml" must be defined for model='+str(self['model']))
    if self['reconYaml'] is not None:
      self['schema']=ClaraYaml.getSchemaName(self['reconYaml'])
      if not ClaraYaml.checkIntegrity(self['reconYaml'],self['clara'],self['ccdbsqlite']):
        self.cli.error('"reconYaml" has bugs')
    if self['trainYaml'] is not None:
      if not ClaraYaml.checkIntegrity(self['trainYaml'],self['clara']):
        self.cli.error('"trainYaml" has bugs')

    # reduce #files in train jobs if huge schema:
    if self['trainSize'] == CFG['trainSize']:
      if self['schema']=='mon' or self['schema']=='calib':
        self['trainSize']=10

    # parse run list:
    self['runs'] = ChefUtil.getRunList(self['runs'])
    if self['runs'] is None or len(self['runs'])==0:
      self.cli.error('\nFound no runs.  Check --inputs and --runs.')

    # check post-processing:
    if self['model'].find('rec')>=0 and ( not self['nopostproc'] or self['recharge'] ):
      cjv=CoatjavaVersion.CoatjavaVersion(self['clara'])
      if not self['nopostproc']:
        if cjv < '6b.4.1':
          self.cli.error('Post-processing requires 6b.4.1 or later')
        if self['helflip'] and cjv < '6.5.11':
          self.cli.error('Post-processing helflip requires 6.5.11 or later')
        if self['noheldel'] and not self['nopostproc'] and cjv < '7.1.0':
          self.cli.error('Post-processing with --noheldel requires 7.1.0 or later')
        for run in self['runs']:
          if run>11000 and cjv < '6b.5.0':
            self.cli.error('Post-processing 120 Hz helicity requires coatjava>6b.5.0.')
      if self['recharge'] and cjv < '6.5.6':
        self.cli.critical('Rebuilding beam charge requires coatjava>6.5.5')
      if self['helflip']:
        _LOGGER.warning('--helflip should only be used on data decoded prior to 6.5.11')

    if not self['denoise'] and self['model'].find('rec')>=0:
      _LOGGER.warning('Denoising is disabled, is that what you really want?')

    if self['model'].find('his')>=0:
      self['graalvm'] = True

if __name__ == '__main__':
  pass
  #logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[ %(name)-15s ] %(message)s')
  #cc=ChefConfig(sys.argv[1:])
  #print((str(cc)))

