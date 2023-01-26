import re,os,subprocess

from JobErrors import SlurmErrors

def getNodeFlavor(hostname):
  for flavor in JobSpecs._FLAVORS:
    if hostname.find(flavor)==0:
      return flavor
  return None

class JobSpecs:

  # FIXME:  make this dynamic, not sure if that's feasible:
  _FLAVORS=['farm23','farm19','farm18','farm16','farm14','farm13']

  def __init__(self):
    self.augerid=None
    self.slurmid=None
    self.slurmstatus=None
    self.threads=None
    self.flavor=None
    self.host=None
    self.host=None
    self.errors=None
    self.slurmerrors=SlurmErrors()
    self.nfiles=-1
    self.t1=-1
    self.t2=-1
    self.events=-1
    self.starttime=None
    self.endtime=None
    self.inputdir=None
    self.outputdir=None
    self.inputfiles=[]

  def parseSlurmLog(self,filename):
    n=0
    for line in readlines_reverse(filename):
      #print line
      n+=1
      if n>10:
        break

  def isComplete(self):
    if self.flavor is None: return False
    if self.threads is None: return False
    if self.events<0: return False
    if self.t1<0: return False
    if self.t2<0: return False
    return True

  # Extract the hostname from a /farm_out logfile
  def getFarmoutHostname(self,logfilename):
    for flavor in JobSpecs._FLAVORS:
      # standard /farm_out logfile names:
      for suffix in ['out','err']:
        m=re.match('.*(%s)(\d+)\.%s$'%(flavor,suffix),logfilename)
        if m is not None:
          return m.group(1)+m.group(2)
    return None

  def __str__(self):
    return '%s/%s/%s/%s'%(self.threads,self.events,self.t1,self.t2)

