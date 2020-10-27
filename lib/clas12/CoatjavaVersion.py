import re,os,sys,glob,logging

_LOGGER=logging.getLogger(__name__)

CLAS12_PACKAGES_DIR='/group/clas12/packages/'
CLARA_VERSION='4.3.12'

class CoatjavaVersion():

  # in hindsight, string comparison of '#.#.#' would work fine
  # after stripping out the a/b/c in the version name !!

  def __init__(self,string):
    self.string=string.strip().rstrip('/')
    self.version=None
    self._parse(self.string)

  def _parse(self,path):
    if os.path.basename(path).endswith('nightly'):
      self.major=999
      self.minor=999
      self.small=999
      self.version='nightly'
    else:
      m=re.search('_(\d+)([abcd]*)\.(\d+)\.(\d+)',os.path.basename(path))
      if m is None:
        m=re.search('(\d+)([abcd]*)\.(\d+)\.(\d+)',os.path.basename(path))
      if m is not None:
        self.major=int(m.group(1))
        self.minor=int(m.group(3))
        self.small=int(m.group(4))
        self.version=m.group().strip('_')
    if self.version is None or not self.string.endswith(self.version):
      raise ValueError('Cannot determine coatjava version: '+path)

  def __lt__(self,other):
    if not isinstance(other,CoatjavaVersion):
      other=CoatjavaVersion(other)
    if self.major<other.major:
      return True
    if self.minor<other.minor:
      return True
    if self.small<other.small:
      return True
    return False

  def __gt__(self,other):
    if not isinstance(other,CoatjavaVersion):
      other=CoatjavaVersion(other)
    if self.major>other.major:
      return True
    if self.minor>other.minor:
      return True
    if self.small>other.small:
      return True
    return False

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
  for clara in glob.glob(CLAS12_PACKAGES_DIR+'/clara/'+CLARA_VERSION+'_*'):
    clara=os.path.normpath(clara)
    if os.path.isdir(clara):
      try:
        cjv=CoatjavaVersion(clara)
        cjvs[cjv.version]=clara
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
    for xx,yy in list(getCoatjavaVersions().items()):
      print((xx+' '+yy))

