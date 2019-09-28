import re

class JobSpecs:

  _FLAVORS=['farm18','farm16','farm14','farm13','qcd12s']

  def __init__(self):
    self.threads=None
    self.events=None
    self.t1=None
    self.t2=None
    self.flavor=None
    self.host=None
    self.nfiles=None
    self.host=None
    self.errors=None
    self.slurmerrors=None

  def isComplete(self):
    if self.flavor is None: return False
    if self.threads is None: return False
    if self.events is None: return False
    if self.t1 is None: return False
    if self.t2 is None: return False
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

