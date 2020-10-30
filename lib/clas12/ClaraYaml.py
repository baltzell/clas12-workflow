import os,re,sys,yaml,glob,logging

import ccdb
import JarUtil

_LOGGER=logging.getLogger(__name__)

_CCDBURI = 'mysql://clas12reader@clasdb.jlab.org/clas12'

#
# Checks whether:
#
# every service has a configuration section
# every service has a CCDB timestamp/variation
# timestamps have correct format
# all classes exist in jars
# all variations exist in CCDB
# there's spaces in any names
#

def checkIntegrity(yamlfile,clara):
  cy = ClaraYaml(yamlfile,clara)
  return cy.checkIntegrity()

class ClaraYaml:

  def __init__(self,yamlfile,clara):
    self.filename = yamlfile
    self.clara = clara
    self.ccdb = ccdb.AlchemyProvider()
    self.jars = self.getJars()
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
    for jar,contents in self.jars.items():
      if contents.contains(name):
        return True
    return False

  def checkIntegrity(self):
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
    self.ccdb.connect(_CCDBURI)
    for v in self.ccdb.get_variations():
      if variation == v.name.strip():
        return True
    self.ccdb.disconnect()
    _LOGGER.critical('Could not find variation '+variation+' in CCDB in YAML: '+self.filename)
    return False

  def checkConfiguration(self,cfg):
    if 'io-services' not in cfg:
      return False
    if 'services' not in cfg:
      return False
    for name,val in cfg['services'].items():
      if name not in self.names:
        _LOGGER.critical('Could not find '+name+' in service list in YAML: '+self.filename)
        return False
      if 'variation' in val:
        if not self.checkVariation(val['variation']):
          return False
      else:
        _LOGGER.warning('No CCDB variation specified for '+name+' in YAML: '+self.filename)
      if 'timestamp' in val:
        m = re.match('\d\d/\d\d/\d\d\d\d$',val['timestamp'])
        if m is None:
          m = re.match('\d\d/\d\d/\d\d\d\d-\d\d:\d\d:\d\d$',val['timestamp'])
        if m is None:
          _LOGGER.critical('Invalid timestamp format '+val['timestamp']+' for '+name+' in YAML: '+self.filename)
          return False
      else:
        _LOGGER.warning('No CCDB timestamp specified for '+name+' in YAML: '+self.filename)
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

