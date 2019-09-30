import os,sys,re,datetime

from JobSpecs import JobSpecs
from JobErrors import ClaraErrors
from LogFinder import LogFinder

_MAXSIZEMB=10
_LOGTAGS=['Number','Threads','TOTAL','Total','Average','Start time','shutdown DPE']

class ClaraLog(JobSpecs):

  logFinder=LogFinder()

  def __init__(self,filename):
    JobSpecs.__init__(self)
    self.errors=ClaraErrors()
    self.filename=filename
    self.filesize=os.path.getsize(filename)
    self.host=self.getFarmoutHostname(filename)
    self.lastline=None
    if self.host is None:
      self.host=self.getClaraHostname(filename)
    if self.host is None:
      print 'Unfound host:  '+filename
      return
    for x in JobSpecs._FLAVORS:
      if self.host.find(x)==0:
        self.flavor=x
        break
    if os.path.getsize(filename)>_MAXSIZEMB*1e6:
      self.errors.setBit('HUGE')
    else:
      with open(filename,'r') as f:
        while True:
          line=f.readline()
          if not line:
            break
          if line.strip()!='':
            self.lastline=line.strip()
          self.parse(line)
    if not self.isComplete():
      self.errors.parse(self.lastline)
    #if self.errors.getBit('TRUNC'):
    self.attachFarmout()

  # Extract the hostname from a /farm_out logfile
  def getClaraHostname(self,logfilename):
    for flavor in JobSpecs._FLAVORS:
      # CLARA-generated names:
      m=re.match('.*/(%s)(\d+)_.*'%(flavor),logfilename)
      if m is not None:
        return m.group(1)+m.group(2)
    return None

  def attachFarmout(self):
    files=ClaraLog.logFinder.findFarmoutLog(self.host,self.filename)
    if len(files)<3:
      for file in files:
        if file.endswith('.err'):
          self.slurmerrors.parse(file)
          break

  def stringToTimestamp(self,string):
    fmt='\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d'
    m=re.match('('+fmt+').*',string.strip())
    if m is None:
      m=re.match('.*('+fmt+').*',string.strip())
    if m is not None:
      t=datetime.datetime.strptime(m.group(1),'%Y-%m-%d %H:%M:%S')
      return t
    return None

  def parse(self,x):
    #if x.find('shutdown DPE')>=0:
    #  print 'A: '+x
    # abort ASAP unless we find a tag:
    keeper=False
    for tag in _LOGTAGS:
      if x.find(tag)>=0:
        keeper=True
        break
    if not keeper:
      return
    x=x.strip()
    cols=x.split()
    if len(cols)==3:
      if cols[0]=='Threads' and cols[1]=='=':
        threads=int(cols[2])
        if self.threads is None:
          self.threads=int(cols[2])
        elif self.threads != threads:
          sys.exit('Invalid threads: %d!=%d'%(threads,self.threads))
    elif len(cols)==4:
      if x.find('shutdown DPE')>0:
        print x
        self.endtime=self.stringToTimestamp(x)
        print self.endtime
    elif len(cols)==5:
      if x.find('Number of files')>=0:
        if self.nfiles<0:
          self.nfiles=int(cols[4])
        else:
          print self.filename,self.nfiles,x
      elif x.find('Start time')==0:
        if self.starttime is None:
          self.starttime=self.stringToTimestamp(x)
    elif len(cols)==8:
      if cols[2]=='Average' and cols[3]=='processing' and cols[4]=='time':
        if self.t2<0:
          self.t2=float(cols[6])
        else:
          print self.filename,self.threads,x
    elif len(cols)==16:
      if cols[2]=='TOTAL' and cols[4]=='events' and cols[5]=='total':
        if self.events<0:
          self.events=int(cols[3])
        else:
          print self.filename,self.threads,x
      if cols[2]=='TOTAL' and cols[11]=='event' and cols[12]=='time':
        if self.t1<0:
          self.t1=float(cols[14])
        else:
          print self.filename,self.threads,x

