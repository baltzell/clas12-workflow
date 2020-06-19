import json,requests,datetime,re,sys

import Matcher

#'https://scicomp.jlab.org/scicomp/farmCjob?type=query&user=baltzell&from=2019-11-01&to=2019-11-10&states=SUCCESS'

#{
#    "coreCount": 16,
#    "name": "rga-rec-vRD-6v.3.4_R5038x6-00598",
#    "memoryReq": "12 GB",
#    "hostname": "farm140229",
#    "state": "SUCCESS",
#    "submit": "Oct 25, 2019 10:28:18 PM",
#    "project": "clas12",
#    "reqId": 2302848,
#    "finish": "Oct 26, 2019 9:17:45 AM",
#    "memoryUsed": "9.4 GB",
#    "id": "5948277",
#    "exitCode": 0,
#    "cputime":"42:15.219",
#    "walltime":"00:34:02"
#}

class SlurmStatus():

  _DATEFORMAT='^([a-zA-Z]+) (\d+), (\d+) (\d+):(\d+):(\d+) ([a-zA-Z]+)$'
  _DATEVARS=['submit','finish']
  _TIMEVARS=['cputime','walltime']
  _BYTEVARS=['memoryUsed','memoryReq']
  _STATES=['TIMEOUT','SUCCESS','FAILED','OVER_RLIMIT']
  _VARS     =['project','name','id','coreCount','hostname','memoryReq','memoryUsed','cputime','walltime','c/w/#','submit','finish','state','exitCode']
  _SHORTVARS=['proj',   'name','id','#',        'host',    'memReq',   'memUse',    'cpu',    'wall',    'c/w/#','submit','finish','state','ex'      ]
  _LEN      =[ 9,        30,    10,  2,          10,        6,          6,           5,        5,         5,      19,      19,      12,     3        ]

  def __init__(self,user,data):
    self.user=user
    self.data=data
    # convert all dates to datetime objects:
    for x in SlurmStatus._DATEVARS:
      if x in self.data:
        self.data[x]=self.convertDate(self.data[x])
    # convert all bytes:
    for x in SlurmStatus._BYTEVARS:
      if x in self.data:
        self.data[x]=self.getBytes(self.data[x])
    # convert all times to seconds:
    for x in SlurmStatus._TIMEVARS:
      if x in self.data:
        self.data[x+'-1']=self.data[x]
        self.data[x]=self.convertTime(self.data[x])
    # calculate cpu/wall ratio:
    if 'cputime' in self.data and 'walltime' in self.data:
      try:
        int(self.data['cputime'])
        int(self.data['walltime'])
        int(self.data['coreCount'])
        if self.data['walltime']>0:
          self.data['c/w/#']=float(self.data['cputime'])/self.data['walltime']/self.data['coreCount']
      except:
        pass

  def getHeader(self):
    ret=''
    ret+='%8s '%'user'
    for ii,yy in enumerate(SlurmStatus._SHORTVARS):
      ret+=('%-'+str(SlurmStatus._LEN[ii])+'s ')%yy
    ret+='\n'
    return ret

  def convertTime(self,string):
    # convert the various time formats reported by the JLab batch system into seconds:
    if string is None or string.strip()=='':
      return string
    # first check for HH:MM:SS:
    mm=re.match('^(\d+):(\d\d):(\d\d)$',string)
    d,h,m,s=0,0,0,0
    if mm is not None:
      h=int(mm.group(1))
      m=int(mm.group(2))
      s=int(mm.group(3))
    else:
      # else catch MM:SS.SS:
      mm=re.match('^(\d+):(\d+)\.(\d+)$',string)
      if mm is not None:
        # we don't care about subseconds, ignore them:
        m=int(mm.group(1))
        s=int(mm.group(2))
      else:
        # else catch multi-day format:
        mm=re.match('^(\d+)-(\d+):(\d+):(\d+)$',string)
        if mm is not None:
          d=int(mm.group(1))
          h=int(mm.group(2))
          m=int(mm.group(3))
          s=int(mm.group(4))
        else:
          # else stop, need to learn how to intpreret this time format:
          print(('Error converting time: >'+string+'<'))
          sys.exit()
    return s+60*m+60*60*h+24*60*60*d

  def convertDate(self,string):
    # datetime doesn't have non-zero-padded stuff,
    # so we have to do this manually ...
    ret=string
    m=re.match(SlurmStatus._DATEFORMAT,string)
    if m is not None:
      month=m.group(1)
      day=int(m.group(2))
      year=int(m.group(3))
      hour=int(m.group(4))
      minute=int(m.group(5))
      second=int(m.group(6))
      ampm=m.group(7)
      if ampm=='PM' and hour<12:
        hour+=12
      # put it back into something datetime can do:
      a='%s %.2d, %.2d %.2d:%.2d:%.2d'%(month,day,year,hour,minute,second)
      ret=datetime.datetime.strptime(a,'%b %d, %Y %H:%M:%S')
    return ret

  def getBytes(self,string):
    ret=string
    if string is not None:
      try:
        # looks like KB doesn't come with units ...
        ret=float(string)*1000
      except:
        x=string.strip().split()
        if len(x)==2:
          try:
            ret=float(x[0])
            scales={'GB':1e9,'MB':1e6,'KB':1e3}
            for scale in list(scales.keys()):
              if x[1].find(scale)>=0:
                ret*=scales[scale]
                break
          except:
            return ret
    return ret

  def getMemRatio(self):
    req,used=None,None
    if 'memoryUsed' in self.data:
      used=self.getBytes(self.data['memoryUsed'])
    if 'memoryReq' in self.data:
      req=self.getBytes(self.data['memoryReq'])
    return used/req

  def __str__(self):
    ret=''
    ret+='%8s '%self.user
    for ii,yy in enumerate(SlurmStatus._VARS):
      a='N/A'
      if yy in self.data:
        a=self.data[yy]
        if isinstance(a,datetime.datetime):
          a==datetime.datetime.strftime(a,'%Y/%m/%d-%H:%M:%S ')
        elif yy in SlurmStatus._BYTEVARS and isinstance(a,float):
          if a/1e6>=1000:
            a='%4.1fG'%(a/1e9)
          else:
            a='%4.0fM'%(a/1e6)
        elif yy=='name' and len(str(a))>30:
          prefix=a[0:14]
          suffix=a[len(a)-15:]
          a=prefix+'*'+suffix
        elif yy=='c/w/#':
          a='%.2f'%a
        elif yy=='cputime' or yy=='walltime':
          try:
            a='%.1f'%(float(a)/60/60)
          except:
            pass
      if yy in SlurmStatus._BYTEVARS or yy in ['cputime','walltime','c/w/#']:
        ret+=('%'+str(SlurmStatus._LEN[ii])+'s ')%str(a).strip()
      else:
        ret+=('%-'+str(SlurmStatus._LEN[ii])+'s ')%str(a).strip()
    ret+='\n'
    return ret

class SlurmQuery():

  _URL='https://scicomp.jlab.org/scicomp/farmCjob'

  def __init__(self,user,project=None):
    self.user=user
    self.project=project
    self.start=None
    self.end=None
    self.dayDelta=7
    self.states=[]
    self.data={}
    self.myData=[]
    self.statuses=[]
    self.matchAny=[]
    self.matchAll=[]
    self.minimumWallHours=None
    self.maximumWallHours=None
    self.states=SlurmStatus._STATES
    self.setDefaultTime()

  def setDefaultTime(self):
    now=datetime.datetime.now()
    # move it to tomorrow:
    self.end=now+datetime.timedelta(days=1)
    self.start=self.end+datetime.timedelta(days=-self.dayDelta)

  def setDayDelta(self,days):
    self.dayDelta=days
    self.start=self.end+datetime.timedelta(days=-self.dayDelta)

  def setDayEnd(self,day):
    self.end=day
    self.start=self.end+datetime.timedelta(days=-self.dayDelta)

  def pruneProjects(self):
    if self.data is None or self.project is None:
      return
    while True:
      pruned=False
      for ii,xx in enumerate(self.data):
        if 'project' not in xx:
          continue
        if self.project != xx['project']:
          self.data.pop(ii)
          pruned=True
          break
      if not pruned:
        break

  def pruneJobNames(self):
    if self.data is None:
      return
    while True:
      pruned=False
      for ii,xx in enumerate(self.data):
        if 'name' not in xx:
          continue
        if not Matcher.matchAny(xx['name'],self.matchAny) or not Matcher.matchAll(xx['name'],self.matchAll):
          self.data.pop(ii)
          pruned=True
          break
      if not pruned:
        break

  def get(self):
    self.data=None
    url=SlurmQuery._URL+'?type=query&user='+self.user
    url+='&from='+self.start.strftime('%Y-%m-%d')
    url+='&to='+self.end.strftime('%Y-%m-%d')
    url+='&states='+'+'.join(self.states)
    response=requests.get(url)
    if int(response.status_code)!=200:
      print('Server error.')
      #print url
      #print response.content
      return None
    try:
      self.data=json.loads(response.content)
    except:
      return None
    self.pruneProjects()
    self.pruneJobNames()
    for xx in self.data:
      self.myData.append(SlurmStatus(self.user,xx))
    return self.data

  def getJson(self):
    return json.dumps(self.get(),indent=2,separators=(',',': '))

  def __str__(self):
    ret=''
    self.get()
    if len(self.myData)>0:
      ret+=self.myData[0].getHeader()
      cpus,walls,cores,mused,mreqd=[],[],[],[],[]
      for xx in self.myData:
        if 'walltime' in xx.data:
          if self.minimumWallHours is not None:
            if xx.data['walltime'] < float(self.minimumWallHours)*60*60:
              continue
          if self.maximumWallHours is not None:
            if xx.data['walltime'] > float(self.maximumWallHours)*60*60:
              continue
        ret+=str(xx)
        try:
          cpu=float(xx.data['cputime'])
          wall=float(xx.data['walltime'])
          core=int(xx.data['coreCount'])
          used=int(xx.data['memoryUsed'])
          reqd=int(xx.data['memoryReq'])
          cpus.append(cpu)
          walls.append(wall)
          cores.append(core)
          mused.append(used)
          mreqd.append(reqd)
        except:
          pass
      wall=0
      for ii in range(len(cpus)):
        wall+=walls[ii]*cores[ii]
      if wall>0:
        ret += '\n'
        ret += 'Job Count       : %d\n'%len(cores)
        ret += 'CPU Count       : %d\n'%sum(cores)
        ret += 'CPU Days        : %.1f\n'%(sum(cpus)/60/60/24)
        ret += 'Wall Days       : %.1f\n'%(wall/60/60/24)
        ret += 'CPU/Wall        : %.3f\n'%(sum(cpus)/wall)
        ret += 'MemUsed/Req     : %.3f\n'%(float(sum(mused))/sum(mreqd))
        ret += 'MemReq/Slot(GB) : %.3f\n'%(float(sum(mreqd))/1e9/sum(cores))
        ret += 'AvgMemUsed(GB)  : %.3f\n'%(sum(mused)/1e9/len(mused))
    return ret

if __name__ == '__main__':
  sq=SlurmQuery('clas12')
  sq.setDayDelta(12)
  print(sq)

#    print ss.getMemRatio()
#    import pandas
#    return pandas.DataFrame(, columns=["time", "temperature", "quality"])
