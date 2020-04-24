import re,sys,logging

_LOGGER=logging.getLogger(__name__)

class CoatjavaVersion():

  # in hindsight, string comparison of #.#.# would work fine
  # after stripping out the a/b/c in the version name !!

  def __init__(self,string):
    self.string=string
    self.version=self._parse(string)
    if self.version is None:
      _LOGGER.critical('Cannot determine coatjava version: '+string)
      sys.exit(1)
    self.major=self.version[0]
    self.minor=self.version[1]
    self.small=self.version[2]

  def _parse(self,path):
    cj=path.split('/').pop().split('_').pop()
    m=re.match('.*\d+\.\d+\.\d+_(\d+)[abcd]*\.(\d+)\.(\d+).*',path)
    if m is not None:
      return [int(m.group(1)),int(m.group(2)),int(m.group(3))]
    m=re.match('.*(\d+)[abcd]*\.(\d+)\.(\d+).*',cj)
    if m is not None:
      return [int(m.group(1)),int(m.group(2)),int(m.group(3))]
    return None

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
    return '%s(%s)'%(self.string,self.version)

if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[ %(name)-15s ] %(message)s')
  if len(sys.argv)>2:
    for ii in range(1,len(sys.argv)-1):
      print('a: '+str(CoatjavaVersion(sys.argv[ii])))
      print('b: '+str(CoatjavaVersion(sys.argv[ii+1])))
      print('a>b: '+str(CoatjavaVersion(sys.argv[ii])>CoatjavaVersion(sys.argv[ii+1])))
  else:
    for xx in sys.argv[1:]:
      print(CoatjavaVersion(xx))

