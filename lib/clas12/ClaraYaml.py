import os,re,sys,yaml,glob,logging

import ccdb
import JarUtil

_LOGGER=logging.getLogger(__name__)

_CCDBURI = 'mysql://clas12reader@clasdb.jlab.org/clas12'

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
# TODO: add full train YAML integrity checking
#

def checkIntegrity(yamlfile,clara):
  return ClaraYaml(yamlfile,clara).checkIntegrity()

def getSchemaName(yamlfile):
  return ClaraYaml(yamlfile,None).getSchemaName()

def getTrainNames(yamlfile):
  return ClaraYaml(yamlfile,None).getTrainNames()

def getTrainIndices(yamlfile):
  return ClaraYaml(yamlfile,None).getTrainIndices()

class ClaraYaml:

  def __init__(self,yamlfile,clara):
    self.filename = yamlfile
    self.clara = clara
    self.ccdb = None
    self.jars = None
    self.names = []
    with open(yamlfile,'r') as f:
      self.yaml = yaml.safe_load(f)

  def getJars(self):
    jars = {}
    for jar in glob.glob(self.clara + '/plugins/clas12/lib/*/*.jar'):
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
    if not self.checkConfiguration(self.yaml['configuration']):
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
      _LOGGER.critical('Could not find class '+service['class']+' in YAML: '+self.filename)
      return False
    self.names.append(service['name'])
    return True

  def checkVariation(self,variation):
    if self.ccdb is None:
      self.ccdb = ccdb.AlchemyProvider()
    self.ccdb.connect(_CCDBURI)
    for v in self.ccdb.get_variations():
      if variation == v.name.strip():
        return True
    self.ccdb.disconnect()
    _LOGGER.critical('Could not find variation '+variation+' in CCDB in YAML: '+self.filename)
    return False

  def checkTimestamp(self,timestamp):
    m = re.match('\d\d/\d\d/\d\d\d\d$',timestamp)
    if m is None:
      m = re.match('\d\d/\d\d/\d\d\d\d-\d\d:\d\d:\d\d$',timestamp)
    if m is None:
      _LOGGER.critical('Invalid timestamp format '+timestamp+' in YAML: '+self.filename)
      return False
    return True

  def checkConfiguration(self,cfg):
    if 'io-services' not in cfg:
      return False
    if 'services' not in cfg:
      return False
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
    for name,val in cfg['services'].items():
      if name not in self.names:
        _LOGGER.critical('Could not find '+name+' in service list in YAML: '+self.filename)
        return False
      if 'variation' in val:
        if not self.checkVariation(val['variation']):
          return False
      elif variation is None:
        _LOGGER.warning('No CCDB variation specified for '+name+' in YAML: '+self.filename)
      if 'timestamp' in val:
        if not self.checkTimestamp(val['timestamp']):
          return False
      elif timestamp is None:
        _LOGGER.warning('No CCDB timestamp specified for '+name+' in YAML: '+self.filename)
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
  usage = 'Usage:  ClaraYaml yamlfile $CLARA_HOME'
  if len(sys.argv) < 3:
    print(usage)
  elif not os.path.isdir(sys.argv[2]):
    print('Missing CLARA installation:  '+sys.argv[2])
    print(usage)
  elif not os.path.isfile(sys.argv[1]):
    print('Missing YAML file:  '+sys.argv[2])
    print(usage)
  else:
    cy = ClaraYaml(sys.argv[1],sys.argv[2])
    cy.checkIntegrity()

