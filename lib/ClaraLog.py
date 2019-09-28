import os,sys,re

from JobSpecs import JobSpecs
from JobErrors import ClaraErrors
from JobErrors import SlurmErrors
from LogFinder import LogFinder

_MAXSIZEMB=10
_LOGTAGS=['Number','Threads','TOTAL','Total','Average']

class ClaraLog(JobSpecs):

  logFinder=LogFinder()

  def __init__(self,filename):
    JobSpecs.__init__(self)
    self.errors=ClaraErrors()
    self.slurmerrors=SlurmErrors()
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

  def parse(self,x):
    # abort ASAP unless we find a tag:
    keeper=False
    for tag in _LOGTAGS:
      if x.find(tag)>=0:
        keeper=True
        break
    x=x.strip()
    cols=x.split()
    if len(cols)==3:
      if cols[0]=='Threads' and cols[1]=='=':
        threads=int(cols[2])
        if self.threads is None:
          self.threads=int(cols[2])
        elif self.threads != threads:
          sys.exit('Invalid threads: %d!=%d'%(threads,self.threads))
    elif len(cols)==5:
      if x.find('Number of files')>=0:
        if self.nfiles is None:
          self.nfiles=int(cols[4])
        else:
          print self.filename,self.nfiles,x
    elif len(cols)==16:
      if cols[2]=='TOTAL' and cols[4]=='events' and cols[5]=='total':
        if self.events is None:
          self.events=int(cols[3])
        else:
          print self.filename,self.threads,x
      if cols[2]=='TOTAL' and cols[11]=='event' and cols[12]=='time':
        if self.t1 is None:
          self.t1=float(cols[14])
        else:
          print self.filename,self.threads,x
    elif len(cols)==8:
      if cols[2]=='Average' and cols[3]=='processing' and cols[4]=='time':
        if self.t2 is None:
          self.t2=float(cols[6])
        else:
          print self.filename,self.threads,x

