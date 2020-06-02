import pwd,os,re,subprocess
from JobSpecs import JobSpecs

RECACHE=False

class LogFinder:
  
  _STATUSCACHE={}

  def __init__(self):
    self.files={}

  def loadStatusCache(self,user):
    if user not in LogFinder._STATUSCACHE:
      print('Loading slurmJobs cache for user='+user+' ...')
      keys=[]
      for line in subprocess.check_output(['slurmJobs','-u',user]).decode('UTF-8').split('\n'):
        cols=line.strip().split()
        if len(cols)>1:
          if cols[0]=='JOB_ID':
            keys=cols
            LogFinder._STATUSCACHE[user]={}
          else:
            augerid=int(cols[0])
            LogFinder._STATUSCACHE[user][augerid]={}
            cols[8]+=cols.pop(9)
            for ii in range(1,len(cols)):
              LogFinder._STATUSCACHE[user][augerid][keys[ii]]=cols[ii]

  def getStatus(self,augerid,user):
    self.loadStatusCache(user)
    if user in self._STATUSCACHE:
      if augerid in self._STATUSCACHE[user]:
        if 'STAT' in self._STATUSCACHE[user][augerid]:
          return self._STATUSCACHE[user][augerid]['STAT']
    return None

  def getFarmoutAugerId(self,filename):
    for flavor in JobSpecs._FLAVORS:
      x=re.match('.*-(\d+)-%s\d+'%flavor,filename)
      if x is not None:
        return int(x.group(1))
    return None

  def getClaraSlurmId(self,filename):
    x=filename.split('/')
    try:
      return int(x[len(x)-2])
    except:
      return None

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
    print('Generating /farm_out file cache at '+cachefile+' ...')
    with open(cachefile,'w') as f:
      subprocess.call(['find','/farm_out/'+user],stdout=f)
    print('Generated /farm_out cache.')

  def loadFarmoutLogs(self,user):
    self.files[user]={}
    cachefile='%s/claralogana_farmout_%s.txt'%(os.environ['HOME'],user)
    if not os.path.isfile(cachefile) or RECACHE:
      self.cacheFarmoutLogs(user,cachefile)
    print('Loading farmout logs from '+cachefile+' ...')
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
    print('Done loading farmout logs.')

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

