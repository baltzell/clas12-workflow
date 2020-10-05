import os,re,sys,glob,logging,collections

# The first/second group must match the run/file number:
__FILEREGEX='.*clas[_A-Za-z]*_(\d+)\.evio\.(\d+)'

_LOGGER=logging.getLogger(__name__)

def setFileRegex(regex):
  global __FILEREGEX
  _LOGGER.info('Changing file regex to '+regex+'. Checking for compilation ...')
  re.compile(regex)
  __FILEREGEX=regex

def getFileRegex():
  return __FILEREGEX

def getRunFileNumber(fileName):
  mm = re.match(__FILEREGEX,fileName)
  if mm is None:
    _LOGGER.debug('Failed to find run number in:  '+fileName)
    return None
  runno=int(mm.group(1))
  fileno=int(mm.group(2))
  return {'run':runno,'file':fileno}

class RunFile:
  def __init__(self,fileName):
    self.fileName=None
    self.runNumber=None
    self.fileNumber=None
    if isinstance(fileName,str):
      fileName=str(fileName)
    fileName=fileName.strip()
    rf=getRunFileNumber(fileName)
    self.fileName=fileName
    if not rf is None:
      self.fileNumber=rf['file']
      self.runNumber=rf['run']
  def __eq__(self,other):
    if not type(other) is type(self): raise TypeError('')
    if self.runNumber != other.runNumber: return False
    if self.fileNumber != other.fileNumber: return False
    return True
  def __lt__(self,other):
    if not type(other) is type(self): raise TypeError('')
    if self.runNumber < other.runNumber: return True
    if self.runNumber > other.runNumber: return False
    if self.fileNumber < other.fileNumber: return True
    if self.fileNumber > other.fileNumber: return False
    return False
  def __gt__(self,other):
    if not type(other) is type(self): raise TypeError('')
    if self.runNumber > other.runNumber: return True
    if self.runNumber < other.runNumber: return False
    if self.fileNumber > other.fileNumber: return True
    if self.fileNumber < other.fileNumber: return False
    return False
  def __str__(self):
    return '%s(%d/%d)'%(self.fileName,self.runNumber,self.fileNumber)
  def show(self):
    print((self.fileName,self.runNumber,self.fileNumber))

class RunFileGroup(list):

  def __init__(self):
    list.__init__(self)
    self.runNumber=None

  def add(self,rf):
    if not isinstance(rf,RunFile):
      raise TypeError('must be a RunFile')
    elif rf is None or rf.runNumber is None:
      return
    elif self.runNumber is None:
      self.runNumber = rf.runNumber
      self.append(rf)
    elif self.runNumber != rf.runNumber:
      _LOGGER.critical('Run number mismatch: '+str(self.runNumber)+'/'+str(rf.runNumber))
      sys.exit()
    elif rf in self:
      _LOGGER.critical('Found duplicate run/file numbers: '+str(rf))
      sys.exit()
    else:
      inserted=False
      for ii in range(len(self)):
        if rf < self[ii]:
          self.insert(ii,rf)
          inserted=True
          break
      if not inserted:
        self.append(rf)

  def addFile(self,fileName):
    self.add(RunFile(fileName))

  def __str__(self):
    xx=str(self.runNumber)+'('
    xx += ','.join([str(yy.fileNumber) for yy in self])
    xx+=')'
    return xx

  def show(self):
    print((str(self.runNumber)))
    for rf in self: rf.show()

class RunFileGroups(collections.OrderedDict):

  def __init__(self):
    self.groupSize=0
    collections.OrderedDict.__init__(self)

  def addRun(self,run):
    if not type(run) is int:
      raise ValueError('run must be an int: '+str(run))
    self[run]=RunFileGroup()

  def addRuns(self,runs):
    for run in runs:
      self.addRun(run)

  def setGroupSize(self,groupSize):
    self.groupSize=int(groupSize)

  def addFile(self,fileName):
    rf=RunFile(fileName)
    # ignore if run# is not registered:
    if rf is None or not rf.runNumber in self:
      return
    self[rf.runNumber].addFile(fileName)

  def addDir(self,dirName):
    _LOGGER.info('Adding directory '+dirName+' ...')
    for dirpath,dirnames,filenames in os.walk(dirName):
      for filename in filenames:
        if not filename.startswith('.'):
          self.addFile(dirpath+'/'+filename)

  def findFiles(self,data):

    # recurse if it's a list:
    if isinstance(data,list):
      for datum in data:
        self.findFiles(datum)

    # walk if it's a directory:
    elif os.path.isdir(data):
      self.addDir(data)

    # file containing a file list if it's a file:
    elif os.path.isfile(data):
      for x in open(data,'r').readlines():
        self.addFile(x.split()[0])

    # else assume it's a glob:
    else:
      _LOGGER.warning('Assuming '+data+' is a glob.')
      for xx in glob.glob(data):
        if os.path.isdir(xx):
          self.addDir(xx)
        elif os.path.isfile(xx):
          self.addFile(xx)

  def getGroups(self):
    #
    # If groupSize is greater than 0, then groups will be
    # limited to that number of files, in which case a
    # single run may be split into multiple groups.
    #
    # Otherwise, each group will be contain all the files
    # from one run.
    #
    groups=[]
    phaseList=[]
    for run,rfg in list(self.items()):
      # make a new group for the next run:
      if len(phaseList)>0:
        groups.append(phaseList)
      phaseList=[]
      # loop over the files in this run:
      for rf in rfg:
        phaseList.append(rf.fileName)
        # make a new group if we're over the size limit:
        if self.groupSize>0 and len(phaseList)>=self.groupSize:
          groups.append(phaseList)
          phaseList=[]
    # make a new group for any leftovers:
    if len(phaseList)>0:
      groups.append(phaseList)
    return groups

  def getFlatList(self):
    flatList=[]
    for run,rfg in list(self.items()):
      for rf in rfg:
        flatList.append(rf.fileName)
    return flatList

  def getRunList(self,minFileCount=1):
    runs=[]
    for run,rfg in list(self.items()):
      if minFileCount>0 and len(rfg)<minFileCount: continue
      runs.append(run)
    return sorted(runs)

  def showGroups(self):
    for group in self.getGroups():
      print(group)

  def showFlatList(self):
    for key,val in list(self.items()):
      print((key,))
      val.show()

  def getFileCount(self):
    return len(self.getFlatList())


# recursive function to generate a run list:
def getRunList(data):

  runs=[]

  # it's a list, just recurse:
  if isinstance(data,list):
    for datum in data:
      runs.extend(getRunList(datum))

  # it's a directory, walk it:
  elif os.path.isdir(data):
    for dirpath,dirnames,filenames in os.walk(data):
      for filename in filenames:
        # recurse with the filename:
        runs.extend(getRunList(dirpath+'/'+filename))

  # it's a file:
  elif os.path.isfile(data):

    # use suffix to assume it's a file list (ugh):
    if data.endswith('.txt') or data.endswith('.list'):
      for line in open(data,'r').readlines():
        # recurse with the line:
        runs.extend(getRunList(line.strip()))

    # otherwise, finally, try to extract a run number:
    else:
      rf=RunFile(data)
      if rf.runNumber is not None:
        runs.append(int(rf.runNumber))

  # else assume it's a glob and keep only files:
  else:
    for xx in glob.glob(data):
      if os.path.isfile(xx):
        # recurse with the filename:
        runs.extend(getRunList(xx))

  # return sorted and unique list:
  return sorted(list(set(runs)))

