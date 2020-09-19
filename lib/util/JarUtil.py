import subprocess,os,logging

_LOGGER=logging.getLogger(__name__)

def contains(filename,resource):
  return JarContents(filename).contains(resource)

class JarContents:
  def __init__(self,filename):
    self.data=[]
    try:
      subprocess.check_output(['java','-version'],stderr=open(os.devnull,'w'))
    except:
      _LOGGER.critical('java not found in $PATH')
      return
    if not os.path.isfile(filename):
      _LOGGER.critical('File does not exist:  '+filename)
    else:
      p = subprocess.Popen(['jar','tf',filename],stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
      for line in iter(p.stdout.readline, ''):
        self.data.append(line.strip().strip('/'))
      p.wait()
  def __str__(self):
    return '\n'.join(self.data)
  def contains(self,path):
    return path.strip('/') in self.data

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

