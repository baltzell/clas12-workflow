import pwd,os,subprocess

RECACHE=False

class LogFinder:

  def __init__(self):
    self.files={}

  def getuser(self,filename):
    try:
      return pwd.getpwuid(os.stat(filename).st_uid).pw_name
    except:
      return None

  def getClaraTag(self,claralog):
    x=claralog.split('/').pop().split('_')
    if len(x)<4:
      return None
    return x[2]+'_'+x[3]

  def getFarmoutTag(self,farmoutlog):
    parts=farmoutlog.strip().split('/')
    user=parts[2]
    basename=parts.pop()
    if user is not None:
      x=basename.replace(user+'-','',1).split('-')
      if len(x)>1:
        return x[1]
    return None

  def cacheFarmoutLogs(self,user,cachefile):
    if os.path.isfile(cachefile):
      os.remove(cachefile)
    print 'Generating /farm_out file cache at '+cachefile+' ...'
    with open(cachefile,'w') as f:
      subprocess.call(['find','/farm_out/'+user],stdout=f)
    print 'Generated /farm_out cache.'

  def loadFarmoutLogs(self,user):
    self.files[user]={}
    cachefile='%s/claralogana_farmout_%s.txt'%(os.environ['HOME'],user)
    if not os.path.isfile(cachefile) or RECACHE:
      self.cacheFarmoutLogs(user,cachefile)
    print 'Loading farmout logs from '+cachefile+' ...'
    with open(cachefile,'r') as f:
      while True:
        line=f.readline()
        if not line:
          break
        filename=line.strip()
        if filename.find('decode')>=0:
          continue  
        tag=self.getFarmoutTag(filename)
        if tag is None or tag=='decode':
          continue
        if not tag in self.files[user]:
          self.files[user][tag]=[]
        self.files[user][tag].append(filename)
    print 'Done loading farmout logs.'

  def findFarmoutLog(self,hostname,claralog):
    files=[]
    user=self.getuser(claralog)
    if user not in self.files:
      self.loadFarmoutLogs(user)
    tag=self.getClaraTag(claralog)
    if tag in self.files[user]:
      for file in self.files[user][tag]:
        if file.find(hostname)>=0:
          files.append(file)
    return files

