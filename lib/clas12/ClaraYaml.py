import re,sys,yaml,glob,datetime,logging,argparse

import ccdb
import JarUtil

_CCDBURI = 'mysql://clas12reader@clasdb.jlab.org/clas12'
_LOGGER = logging.getLogger(__name__)

#
# Checks recon YAMLs for whether:
#
# every service has a configuration section
# every service has a CCDB timestamp/variation
# timestamps have correct format
# all classes exist in jars
# all variations exist in CCDB
# there's spaces in any names
#

def checkIntegrity(yamlfile,clara,ccdb_sqlite_file=None):
  return ClaraYaml(yamlfile,clara,ccdb_sqlite_file).checkIntegrity()

def getSchemaName(yamlfile):
  return ClaraYaml(yamlfile,None).getSchemaName()

def getTrainNames(yamlfile):
  return ClaraYaml(yamlfile,None).getTrainNames()

def getTrainIndices(yamlfile):
  return ClaraYaml(yamlfile,None).getTrainIndices()

class ClaraYaml:

  def __init__(self,yamlfile,clara,ccdb_sqlite_file=None):
    self.filename = yamlfile
    self.clara = clara
    if ccdb_sqlite_file is None:
      self.ccdb_connection = _CCDBURI
    else:
      self.ccdb_connection = 'sqlite:///'+ccdb_sqlite_file
    self.ccdb = None
    self.jars = None
    self.names = []
    self.check_ccdb = True
    with open(yamlfile,'r') as f:
      self.yaml = yaml.safe_load(f)
    if 'services' in self.yaml:
      for x in self.yaml['services']:
        if 'class' in x:
          if x['class'].find('org.jlab.jnp.grapes') == 0:
            self.check_ccdb = False

  def getJars(self):
    jars = {}
    for jar in glob.glob(self.clara + '/plugins/clas12/lib/*/*.jar'):
      jars[jar] = JarUtil.JarContents(jar)
    for jar in glob.glob(self.clara + '/plugins/grapes/lib/core/grapes*.jar'):
      jars[jar] = JarUtil.JarContents(jar)
    for jar in glob.glob(self.clara + '/lib/jclara-*.jar'):
      jars[jar] = JarUtil.JarContents(jar)
    return jars

  def findClass(self,name):
    if self.jars is None:
      self.jars = self.getJars()
    for jar,contents in self.jars.items():
      if contents.contains(name):
        return True
    return False

  def checkIntegrity(self):
    _LOGGER.info('Checking YAML file:  '+self.filename)
    if not self.checkGroups():
      return False
    if not self.checkAscii(self.filename):
      return False
    if 'services' not in self.yaml:
      _LOGGER.critical('\'services\' not in YAML: '+self.filename)
      return False
    if 'configuration' not in self.yaml:
      _LOGGER.critical('\'configuration\' not in YAML: '+self.filename)
      return False
    for service in self.yaml['services']:
      if not self.checkService(service):
        return False
    for x in ['reader','writer']:
      if x in self.yaml['io-services']:
        if not self.checkService(self.yaml['io-services'][x]):
          return False
    if not self.checkConfiguration(self.yaml):
      return False
    for i in self.getTrainIndices():
      if i<1 or i>32:
        _LOGGER.critical('Train id not in valid 1-32 range:  '+str(i))
        return False
    return True

  def getSchemaName(self):
    ret = None
    if 'configuration' in self.yaml:
      c = self.yaml['configuration']
      if 'io-services' in c:
        c = c['io-services']
        if 'writer' in c:
          c = c['writer']
          if 'schema_dir' in c:
            s = c['schema_dir'].strip().strip('/').split('/').pop().strip('"')
            if s == 'monitoring': s = 'mon'
            if s == 'calibration' : s = 'calib'
            ret = s
    return ret

  def getTrainIndices(self):
    ret = []
    if 'configuration' in self.yaml:
      c = self.yaml['configuration']
      if 'services' in c:
        for name,val in c['services'].items():
          if 'id' in val:
            ret.append(int(val['id']))
    return set(ret)

  def getTrainNames(self):
    ret = {}
    for x in self.getTrainIndices():
      ret[x] = None
    if 'configuration' in self.yaml:
      c = self.yaml['configuration']
      if 'custom-names' in c:
        for key,val in c['custom-names'].items():
          try:
            key = int(key)
          except:
            _LOGGER.error('Non-integer wagon id in custom-names in train YAML: '+key)
            sys.exit(42)
          if key not in ret:
            _LOGGER.error('Invalid wagon id in custom-names in train YAML:  '+str(key))
            sys.exit(42)
          ret[key] = val.strip()
    # must be all-or-none:
    if None in list(ret.values()):
      for x,y in list(ret.items()):
        if y is not None:
          _LOGGER.error('Missing custom-name in train yaml:  '+str(ret))
          sys.exit(42)
      for x,y in list(ret.items()):
        ret[x]='skim%d'%int(x)
    return ret

  def checkGroups(self):
    groups={'io-services':['reader','writer'],'services':[],'configuration':['global','io-services','custom-names','services'],'mime-types':[]}
    for group in self.yaml:
      if group not in groups:
        _LOGGER.error('Unknown YAML section:  '+group)
        return False
      if len(groups[group]) > 0:
        for x in self.yaml[group]:
          if x not in groups[group]:
            _LOGGER.error('Unknown YAML section:  '+group+':'+x)
            return False
    return True

  def checkService(self,service):
    if 'class' not in service:
      if 'name' in service:
        _LOGGER.critical('\'class\' missing for '+service['name']+' in YAML: '+self.filename)
      else:
        _LOGGER.critical('\'class\' missing for unnamed service in YAML: '+self.filename)
      return False
    if 'name' not in service:
      if 'class' in service:
        _LOGGER.critical('\'name\' missing for '+service['class']+' in YAML: '+self.filename)
      else:
        _LOGGER.critical('\'name\' missing for unclassed service in YAML: '+self.filename)
      return False
    if service['name'].find(' ')>0:
      _LOGGER.critical('Space found in \''+service['name']+'\' in YAML: '+self.filename)
      return False
    if not self.findClass(service['class']):
      _LOGGER.critical('Could not find class '+service['class']+' specified in YAML: '+self.filename)
      return False
    self.names.append(service['name'])
    return True

  def checkVariation(self,variation):
    if self.ccdb is None:
      self.ccdb = ccdb.AlchemyProvider()
    self.ccdb.connect(self.ccdb_connection)
    for v in self.ccdb.get_variations():
      if variation == v.name.strip():
        return True
    self.ccdb.disconnect()
    _LOGGER.critical('Could not find CCDB variation '+variation+' as specified in YAML: '+self.filename)
    return False

  def get(self,key):
    ret = {}
    if 'global' in self.yaml['configuration']:
      if key in self.yaml['configuration']['global']:
        ret['global'] = self.yaml['configuration']['global']['key']
    if 'services' in self.yaml['configuration']:
      for name,val in self.yaml['configuration']['services'].items():
        if key in val:
          ret[name] = val['key']
    return ret

  def checkTimestamp(self,timestamp):
    # check the basic format is valid:
    m = re.match('\d\d/\d\d/\d\d\d\d$',timestamp)
    if m is None:
      m = re.match('\d\d/\d\d/\d\d\d\d-\d\d:\d\d:\d\d$',timestamp)
    if m is None:
      _LOGGER.critical('Invalid timestamp in YAML: '+timestamp)
      _LOGGER.critical('Expected format is MM/DD/YYYY or MM/DD/YYYY-HH:MM:SS')
      return False
    # check it's really a possible timestamp:
    try:
      if timestamp.find('-') < 0:
        t = datetime.datetime.strptime(timestamp,'%m/%d/%Y')
      else:
        t = datetime.datetime.strptime(timestamp,'%m/%d/%Y-%H:%M:%S')
    except ValueError:
      _LOGGER.critical('Invalid timestamp in YAML: '+timestamp)
      _LOGGER.critical('Expected format is MM/DD/YYYY or MM/DD/YYYY-HH:MM:SS')
      return False
    # warn of possible day/month swap:
    if t.day < 13:
      _LOGGER.warning('Possible day/month swap in timestamp in YAML:  '+timestamp)
      _LOGGER.warning('Expected format is MM/DD/YYYY or MM/DD/YYYY-HH:MM:SS')
    return True

  def checkConfiguration(self,cfg):
    #if 'io-services' not in cfg:
    #  return False
    services = cfg['services']
    cfg = cfg['configuration']
    timestamp,variation = None,None
    if 'global' in cfg:
      if 'timestamp' in cfg['global']:
        timestamp = cfg['global']['timestamp']
        if not self.checkTimestamp(timestamp):
          return False
      if 'variation' in cfg['global']:
        variation = cfg['global']['variation']
        if not self.checkVariation(variation):
          return False
    if 'services' not in cfg:
      _LOGGER.warning('Configuration section does not contain a services subsetion in YAML: '+self.filename)
      return True
    for name,val in cfg['services'].items():
      if name not in self.names:
        _LOGGER.critical('Could not find '+name+' in service list in YAML: '+self.filename)
        return False
      if 'variation' in val:
        if not self.checkVariation(val['variation']):
          return False
      elif variation is None and self.check_ccdb and name != 'SWAPS':
        _LOGGER.warning('No CCDB variation specified for '+name+' in YAML: '+self.filename)
      if 'timestamp' in val:
        if not self.checkTimestamp(val['timestamp']):
          return False
      elif timestamp is None and self.check_ccdb:
        _LOGGER.warning('No CCDB timestamp specified for '+name+' in YAML: '+self.filename)
    if timestamp is None and self.check_ccdb:
      for service in services:
        if service['name'] not in cfg['services'].keys():
          _LOGGER.warning('No CCDB timestamp specified for '+service['name']+' in YAML: '+self.filename)
    return True

  def checkAscii(self,filename):
    with open(filename,'r') as f:
      lineno = 0
      for line in f.readlines():
        lineno += 1
        try:
          line.encode('ascii')
        except:
          _LOGGER.critical('Non-ASCII characters (line %d: %s )found in YAML: %s'%(lineno,line.strip(),self.filename))
          return False
    return True

if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[ %(name)-15s ] %(message)s')
  logger = logging.getLogger(__name__)
  cli = argparse.ArgumentParser('Check integrity of a CLARA YAML file.')
  cli.add_argument('--clara',help='path to clara installation',metavar='PATH',type=str,default='/group/clas12/packages/clara/5.0.2_7.1.0')
  cli.add_argument('--ccdbsqlite',help='CCDB sqlite file to use',metavar='FILE',type=str,default=None)
  cli.add_argument('yaml',help='YAML file to check',type=str)
  args = cli.parse_args(sys.argv[1:])
  if checkIntegrity(args.yaml,args.clara,args.ccdbsqlite):
    logger.info('YAML checks passed.')
  else:
    logger.critical('YAML checks failed.')
    sys.exit(1)

