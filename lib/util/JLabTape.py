import os
import sys
import hashlib
import zlib
import logging

_LOGGER = logging.getLogger(__name__)

if sys.version_info < (3,0):
  _LOGGER.critical('Requires python > 3')
  sys.exit(1)

# should optimize checksums for large files, meanwhile:
MAX_FILE_SIZE = 5e9

class TapeFile():

  def __init__(self, path):

    self.path = path
    self.size = None
    self.crc32 = None
    self.md5 = None

    if not os.path.isfile(path):
      raise ValueError('File does not exist:  '+path)

  def __eq__(self, other):

    if self.size != other.size:
      return False
    if self.crc32 is not None and other.crc32 is not None:
      if self.crc32 != other.crc32:
        print(self.crc32)
        print(other.crc32)
        print('b')
        return False
    if self.md5 is not None and other.md5 is not None:
      if self.md5 != other.md5:
        print('c')
        return False
    return True

  def __str__(self):
    return 'size=%d crc32=0x%s md5=0x%s'%(self.size,self.crc32,self.md5)

class TapeStub(TapeFile):

  def __init__(self, path):

    TapeFile.__init__(self, path)

    if not path.startswith('/mss/'):
      raise ValueError('File must start with /mss/:  '+path)

    with open(path,'r') as f:
      for line in f.readlines():
        key,val = line.strip().split('=')
        if key == 'size':
          self.size = int(val)
        elif key == 'md5':
          self.md5 = val
        elif key == 'crc32':
          self.crc32 = val


class CachedFile(TapeFile):

  def __init__(self, path, crc32=False, md5=False):

    TapeFile.__init__(self, path)

    if not self.path.startswith('/cache/'):
      raise ValueError('File must start with /cache/:  '+path)

    self.size = os.stat(self.path).st_size

    if self.size > MAX_FILE_SIZE:
      raise ValueError('/cache file is too large to checksum:  '+self.path)

    if md5:
      with open(self.path,'rb') as f:
        x = hashlib.md5()
        x.update(f.read())
        self.md5 = str(x.hexdigest())

    if crc32:
      x = 0
      with open(self.path,'rb') as f:
        for line in f.readlines():
          x = zlib.crc32(line, x)
      self.crc32 = '%x' % x

if __name__ == '__main__':

  logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[ %(name)-15s ] %(message)s')

  if len(sys.argv)>1:
    path = sys.argv[1]
  else:
    path = '/cache/clas/e2a/production/pass2/v1/1161/3He_full/HROOT/hroot_18311_08_v1.root'

  cache = CachedFile(path, crc32=True, md5=True)
  tape = TapeStub('/mss'+path[6:])

  print(cache)
  print(tape)
  print(cache == tape)

