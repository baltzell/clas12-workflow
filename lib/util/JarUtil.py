import subprocess,os,logging

_LOGGER=logging.getLogger(__name__)

_JAVA=None

def contains(filename,resource):
  return JarContents(filename).contains(resource)

class JarContents:
  def __init__(self,filename):
    self.data=[]
    if _JAVA is None:
      try:
        JAVA=subprocess.check_output(['java','-version'],stderr=open(os.devnull,'w'))
      except:
        _LOGGER.critical('java not found in $PATH')
        return
    if not os.path.isfile(filename):
      _LOGGER.critical('File does not exist:  '+filename)
    else:
      p = subprocess.Popen(['jar','tf',filename],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,universal_newlines=True)
      for line in iter(p.stdout.readline, ''):
        self.data.append(line.strip().strip('/'))
      p.wait()
  def __str__(self):
    return '\n'.join(self.data)
  def contains(self,path):
    if path.strip('/') not in self.data:
      if path.strip('/').replace('/','.') not in self.data:
        if path.strip('/').replace('.','/')+'.class' not in self.data:
          return False
    return True


if __name__ == '__main__':
  import sys
  logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[ %(name)-15s ] %(message)s')
  if len(sys.argv)<2:
    print('Usage:  JarUtil.py /path/to/jarfile [/path/to/jar/resource]')
    sys.exit(1)
  jc = JarContents(sys.argv[1])
  if len(sys.argv)>2:
    print(jc.contains(sys.argv[2]))
  else:
    print(jc)

