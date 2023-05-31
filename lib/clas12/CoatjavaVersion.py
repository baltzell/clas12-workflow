import re,os,sys,glob,logging

_LOGGER=logging.getLogger(__name__)

CLAS12_PACKAGES_DIR='/group/clas12/packages/'
CLARA_VERSIONS=['5.0.2','4.3.12'] # ordered by preference
SEARCH_PATHS = ['plugins/clas12/lib/clas','lib/clas','coatjava/lib/clas']

class CoatjavaVersion():

  # in hindsight, string comparison of '#.#.#' would work fine
  # after stripping out the a/b/c in the version name !!

  def __init__(self,string):
    self.string=string.strip().rstrip('/')
    self.version=None
    if not self._find(self.string):
      if self._extract(os.path.basename(self.string)):
        if os.path.isdir(self.string):
          _LOGGER.warning('Couldn\'t find jar, relying on directory name for coatjava version: '+self.string)
      else:
        raise ValueError('Cannot determine coatjava version: '+self.string)

  def _extract(self,string):
    m=re.search('_(\d+)([abcd]*)\.(\d+)\.(\d+)\.(\d+)',string)
    if m is None:
      m=re.search('_(\d+)([abcd]*)\.(\d+)\.(\d+)',string)
    if m is None:
      m=re.search('(\d+)([abcd]*)\.(\d+)\.(\d+)\.(\d+)',string)
    if m is None:
      m=re.search('(\d+)([abcd]*)\.(\d+)\.(\d+)',string)
    if m is not None:
      self.major=int(m.group(1))
      self.minor=int(m.group(3))
      self.small=int(m.group(4))
      self.version=m.group().strip('_')
      return True
    return False

  def _find(self,path):
    for x in SEARCH_PATHS:
      g = glob.glob(path+'/'+x+'/coat-libs-*.jar')
      if len(g) > 1:
        raise ValueError('Multiple coatjavas installed at: '+path)
      if len(g) == 1:
        return self._extract(os.path.basename(g[0]))
    return False

  def __lt__(self,other):
    if not isinstance(other,CoatjavaVersion):
      other=CoatjavaVersion(other)
    if self.major != other.major:
      return self.major < other.major
    if self.minor != other.minor:
      return self.minor < other.minor
    return self.small < other.small

  def __gt__(self,other):
    if not isinstance(other,CoatjavaVersion):
      other=CoatjavaVersion(other)
    if self.major != other.major:
      return self.major > other.major
    if self.minor != other.minor:
      return self.minor > other.minor
    return self.small > other.small

  def __eq__(self,other):
    if not self<other and not self>other:
      return True
    return False

  def __ge__(self,other):
    if self>other or self==other:
      return True
    return False

  def __le__(self,other):
    if self<other or self==other:
      return True
    return False

  def __str__(self):
    return '%s (%s)'%(self.string,self.version)

def getCoatjavaVersions():
  cjvs={}
  for clara in CLARA_VERSIONS:
    for clara in glob.glob(CLAS12_PACKAGES_DIR+'/clara/'+clara+'_*'):
      if clara.find('nightly')>=0:
        continue
      if clara.count('_') > 1:
        continue
      clara=os.path.normpath(clara)
      if os.path.isdir(clara):
        try:
          cjv=CoatjavaVersion(clara)
          if cjv.version not in cjvs:
            cjvs[cjv.version]={'path':clara, 'version':cjv}
        except:
          pass
  return cjvs

if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[ %(name)-15s ] %(message)s')
  if len(sys.argv)>2:
    for ii in range(1,len(sys.argv)-1):
      print(('a: '+str(CoatjavaVersion(sys.argv[ii]))))
      print(('b: '+str(CoatjavaVersion(sys.argv[ii+1]))))
      print(('a>b: '+str(CoatjavaVersion(sys.argv[ii])>CoatjavaVersion(sys.argv[ii+1]))))
  elif len(sys.argv)>1:
    for xx in sys.argv[1:]:
      print((CoatjavaVersion(xx)))
  else:
    cjvs = [ y['version'] for x,y in getCoatjavaVersions().items() ]
    for cjv in sorted(cjvs):
      print(cjv)

