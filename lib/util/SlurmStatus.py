import json,requests,datetime
#import pandas

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
  _STATES=['timeout','success','failed','over_rlimit']
  _VARS=['name','id','coreCount','hostname','memoryReq','memoryUsed','state','exitCode']
  _LEN=[40,10,3,12,8,8,12,4]
  def __init__(self,data):
    self.data=data
  def getBytes(self,string):
    x=string.strip().split()
    if len(x)!=2: return None
    try: b=int(x[0])
    except: return None
    scales={'GB':1e9,'MB':1e6,'KB':1e3}
    for scale in scales.keys():
      if x[1].find(scale)>=0:
        b*=scales[scale]
        break
    return b
  def getMemRatio(self):
    req,used=None,None
    if 'memoryUsed' in self.data:
      used=self.getBytes(self.data['memoryUsed'])
    if 'memoryReq' in self.data:
      req=self.getBytes(self.data['memoryReq'])
    return used/req

class SlurmQuery():
  _URL='https://scicomp.jlab.org/scicomp/farmCjob'
  def __init__(self,user):
    self.user=user
    self.start=None
    self.end=None
    self.dayDelta=7
    self.states=[]
    self.setAllStates()
    self.setDefaultTime()
    self.data={}
    self.statuses=[]
  def setAllStates(self):
    self.states=SlurmStatus._STATES
  def setDefaultTime(self):
    now=datetime.datetime.now()
    self.end=now+datetime.timedelta(days=1)
    self.start=self.end+datetime.timedelta(days=-self.dayDelta)
  def setDayDelta(self,days):
    self.dayDelta=days
    self.start=self.end+datetime.timedelta(days=days)
  def setDayEnd(self,day):
    self.end=day
    self.start=self.end+datetime.timedelta(days=-self.dayDelta)
  def get(self):
    url=SlurmQuery._URL+'?type=query&user='+self.user
    url+='&from='+self.start.strftime('%Y-%m-%d')
    url+='&to='+self.end.strftime('%Y-%m-%d')
    url+='&states='+'+'.join(self.states)
    x=requests.get(url)
    self.data=json.loads(x.content)
    return self.data
  def getJson(self):
    self.get()
    return json.dumps(self.data,indent=2,separators=(',',': '))
  def getTable(self):
    self.get()
    for xx in self.data:
      for ii,yy in enumerate(SlurmStatus._VARS):
        if yy in xx:
          print ('%'+str(SlurmStatus._LEN[ii])+'s')%str(xx[yy]),
      print
      #print ss.getMemRatio()
#    self.get()
#    return pandas.DataFrame(, columns=["time", "temperature", "quality"])

if __name__ == '__main__':
  ss=SlurmQuery('clas12')
  ss.getTable()
#  print ss.getJson()


