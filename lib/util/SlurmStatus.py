import json,requests,datetime,re

import Matcher

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
#    "exitCode": 0
#}

class SlurmStatus():
  _DATEFORMAT='^([a-zA-Z]+) (\d+), (\d+) (\d+):(\d+):(\d+) ([a-zA-Z]+)$'
  _DATEVARS=['submit','finish']
  _BYTEVARS=['memoryUsed','memoryReq']
  _STATES=['timeout','success','failed','over_rlimit']
  _VARS=['project','name','id','coreCount','hostname','memoryReq','memoryUsed','state','exitCode','submit','finish']
  _LEN=[6,30,10,2,10,7,7,12,3,25,25]
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
  def getHeader(self):
    ret=''
    ret+='%10s'%'user'
    for ii,yy in enumerate(SlurmStatus._VARS):
      ret+=('%-'+str(SlurmStatus._LEN[ii])+'s ')%yy
    ret+='\n'
    return ret
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
            for scale in scales.keys():
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
    ret+='%10s '%self.user
    for ii,yy in enumerate(SlurmStatus._VARS):
      if yy in self.data:
        a=self.data[yy]
        if isinstance(a,datetime.datetime):
          ret+=datetime.datetime.strftime(a,'%Y/%m/%d-%H:%M:%S ')
        elif yy in SlurmStatus._BYTEVARS and isinstance(a,float):
          if a/1e6>=1000: ret+='%4.1f GB '%(a/1e9)
          else:          ret+='%4.0f MB '%(a/1e6)
        else:
          if len(str(a))>30:
            prefix=a[0:14]
            suffix=a[len(a)-15:]
            a=prefix+'*'+suffix
          ret+=('%-'+str(SlurmStatus._LEN[ii])+'s ')%str(a)
      else:
        ret+=('%'+str(SlurmStatus._LEN[ii])+'s ')%'N/A'
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
    self.setAllStates()
    self.setDefaultTime()
    self.data={}
    self.myData=[]
    self.statuses=[]
    self.matchAny=[]
    self.matchAll=[]
  def setAllStates(self):
    self.states=SlurmStatus._STATES
  def setDefaultTime(self):
    now=datetime.datetime.now()
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
#      ret+=self.myData[0].getHeader()
      for xx in self.myData:
        ret+=str(xx)
    return ret

if __name__ == '__main__':
  sq=SlurmQuery('clas12')
  sq.setDayDelta(12)
  print(sq)

#    print ss.getMemRatio()
#    import pandas
#    return pandas.DataFrame(, columns=["time", "temperature", "quality"])
