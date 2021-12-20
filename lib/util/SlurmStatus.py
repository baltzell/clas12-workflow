import json,requests,datetime,re,sys

from JobSpecs import getNodeFlavor
from JobSpecs import JobSpecs
import Matcher

#NEW:
#https://scicomp.jlab.org/scicomp2/farmRecentJob?type=query&from=2021-10-30&to=2021-11-03&states=FAILED+COMPLETED+TIMEOUT+&user=clas12-2
#{
#"jobId":"43172035",
#"user":"clas12",
#"account":"hallb-pro",
#"jobName":"rga-a-test-5247-p0...",
#"partition":"production",
#"state":"COMPLETED",
#"core":1,
#"submit":"Oct 29, 2021 7:35:33 PM",
#"start":"Oct 29, 2021 7:37:56 PM",
#"end":"Oct 29, 2021 7:38:14 PM",
#"walltime":"00:00:18",
#"cputime":"00:13.285",
#"memory":"940",
#"exit":0,
#"nodeId":"farm13"
#}

#OLD:
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
  _VARS     =['account','jobName','id','core','hostname','memory','cputime','walltime','c/w/#','submit','end','state','exit']
  _SHORTVARS=['acct',   'name',   'id','#',   'host',    'mem',   'cpu',    'wall',    'c/w/#','submit','end','state','ex'  ]
  _LEN      =[ 9,        30,       10,  2,     10,        6,       5,        5,         5,      19,      19,   12,     3    ]

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

  _URL='https://scicomp.jlab.org/scicomp2/farmRecentJob'

  def __init__(self,user,project=None):
    self.user=user
    self.project=project
    self.start=None
    self.end=None
    self.dayDelta=7
    self.states=[]
    self.data=None
    self.myData=[]
    self.statuses=[]
    self.matchAny=[]
    self.matchAll=[]
    self.summary=None
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

  def _pruneProjects(self):
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

  def _pruneJobNames(self):
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
    if self.data is None:
      url=SlurmQuery._URL+'?type=query&user='+self.user
      url+='&from='+self.start.strftime('%Y-%m-%d')
      url+='&to='+self.end.strftime('%Y-%m-%d')
      url+='&states='+'+'.join(self.states)
      response=requests.get(url)
      if int(response.status_code)!=200:
        print('Server error.')
        return None
      try:
        self.data=json.loads(response.content.decode('UTF-8'))
      except:
        priint(response.content.decode('UTF-8'))
        return None
      self._pruneProjects()
      self._pruneJobNames()
      for xx in self.data:
        self.myData.append(SlurmStatus(self.user,xx))
    return self.data

  def getJson(self):
    return json.dumps(self.get(),indent=2,separators=(',',': '))

  def getHistos(self,varname):
    prefs={}
    prefs['cputime']={'scale':60*60,'title':'CPU Hours Per Core Per Job'}
    prefs['walltime']={'scale':60*60,'title':'Wall Hours Per Job'}
    prefs['cputime/walltime']={'scale':1,'title':'CPU/Wall Time Ratio'}
    _COLORS =[1,  2,    4,    3,    94,  51]
    _THREADS=[1,12, 20,   16,   24,   32]
    _FILLS  =[0,0,  3007, 3003, 3004, 3005]
    from ROOT import TH1D
    histos,mini,maxi,scale,title={},0,-99999,1,varname
    if varname in prefs:
      scale=prefs[varname]['scale']
      title=prefs[varname]['title']
    if varname.count('/')==1:
      varnames=varname.split('/')
    else:
      varnames=[varname]
    if len(self.myData)>0:
      for datum in self.myData:
        datum=datum.data
        if 'hostname' not in datum or 'coreCount' not in datum:
          continue
        if getNodeFlavor(datum['hostname']) is None:
          continue
        if False in [v in datum for v in varnames]:
          continue
        try:
          val=float(datum[varnames[0]])/scale
          if len(varnames)==2:
            val/=float(datum[varnames[1]])
          if varnames[0]=='cputime':
            val/=datum['coreCount']
          if float(val)>maxi:
            maxi=float(val)
        except:
          pass
      for datum in self.myData:
        datum=datum.data
        if 'hostname' not in datum or 'coreCount' not in datum:
          continue
        flavor=getNodeFlavor(datum['hostname'])
        if flavor is None:
          continue
        if False in [v in datum for v in varnames]:
          continue
        try:
          val=float(datum[varnames[0]])/scale
          if len(varnames)==2:
            val/=float(datum[varnames[1]])
          if varnames[0]=='cputime':
            val/=datum['coreCount']
          if flavor not in histos:
            histos[flavor]=TH1D(flavor+varname,';'+varname,100,mini,maxi)
            histos[flavor].GetXaxis().SetTitle(title)
            color=_COLORS[JobSpecs._FLAVORS.index(flavor)]
            histos[flavor].SetTitle('%sx%d'%(flavor,datum['coreCount']))
            histos[flavor].SetLineColor(color)
            fill=_FILLS[_THREADS.index(threads)]
            if fill>0:
              histos[flavor].SetFillStyle(fill)
              histos[flavor].SetFillColor(color)
          histos[flavor].Fill(float(val))
        except:
          pass
      maxi=0
      for h in histos.values():
        if h.GetBinContent(h.GetMaximumBin())>maxi:
          maxi=h.GetBinContent(h.GetMaximumBin())
      for h in histos.values():
        h.SetMaximum(maxi*1.1)
    return histos

  def getCanvas(self,varname):
    from ROOT import TCanvas
    import ROOTConfig
    histos=self.getHistos(varname)
    canvas=TCanvas(self.user+'_'+varname,self.user+':'+varname,700,500)
    dopt='H'
    for h in histos.values():
      h.Draw(dopt)
      dopt='SAMEH'
    canvas.BuildLegend(0.75,0.95-len(histos.values())*0.04,0.92,0.95)
    canvas.Update()
    return histos,canvas

  def size(self):
    self.get()
    return len(self.myData)

  def getSummary(self):
    self.get()
    if self.summary is None:
      dog=str(self)
    return self.summary

  def __str__(self):
    ret=''
    self.summary=''
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
      ret+=self.myData[0].getHeader()
      if wall>0:
        self.summary += '\n'
        self.summary += 'Job Count       : %d\n'%len(cores)
        self.summary += 'CPU Count       : %d\n'%sum(cores)
        self.summary += 'CPU Days        : %.1f\n'%(sum(cpus)/60/60/24)
        self.summary += 'Wall Days       : %.1f\n'%(wall/60/60/24)
        self.summary += 'CPU/Wall        : %.3f\n'%(sum(cpus)/wall)
        self.summary += 'MemUsed/Req     : %.3f\n'%(float(sum(mused))/sum(mreqd))
        self.summary += 'MemReq/Slot(GB) : %.3f\n'%(float(sum(mreqd))/1e9/sum(cores))
        self.summary += 'MemUsed/Job(GB) : %.3f\n'%(sum(mused)/1e9/len(mused))
    return ret

if __name__ == '__main__':
  sq=SlurmQuery('clas12')
  sq.setDayDelta(12)
  print(sq)

#    print ss.getMemRatio()
#    import pandas
#    return pandas.DataFrame(, columns=["time", "temperature", "quality"])
