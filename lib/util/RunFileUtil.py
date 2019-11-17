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
    if isinstance(fileName,unicode):
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
    print(self.fileName,self.runNumber,self.fileNumber)

# TODO:  make this a subclass of list
class RunFileGroup():
  def __init__(self):
    self.runNumber=None
    self.runFileList=[]
  def size(self):
    return len(self.runFileList)
  def add(self,rf):
    if not isinstance(rf,RunFile):
      raise TypeError('must be a RunFile')
    elif rf is None or rf.runNumber is None:
      return
    elif self.runNumber is None:
      self.runNumber = rf.runNumber
      self.runFileList.append(rf)
    elif self.runNumber != rf.runNumber:
      _LOGGER.critical('Run number mismatch: '+str(self.runNumber)+'/'+str(rf.runNumber))
      sys.exit()
    elif rf in self.runFileList:
      _LOGGER.critical('Found duplicate run/file numbers: '+str(rf))
      sys.exit()
    else:
      inserted=False
      for ii in range(len(self.runFileList)):
        if rf < self.runFileList[ii]:
          self.runFileList.insert(ii,rf)
          inserted=True
          break
      if not inserted:
        self.runFileList.append(rf)
  def addFile(self,fileName):
    self.add(RunFile(fileName))
  def __str__(self):
    xx=str(self.runNumber)+'('
    xx += ','.join([str(yy.fileNumber) for yy in self.runFileList])
    xx+=')'
    return xx
  def show(self):
    print(str(self.runNumber))
    for rf in self.runFileList: rf.show()

class RunFileGroups:

  def __init__(self):
    self.combineRuns=False
    self.groupSize=0
    # maintain user's run insertion order:
    self.rfgs=collections.OrderedDict()

  def setCombineRuns(self,val):
    self.combineRuns=val

  def hasRun(self,run):
    return run in self.rfgs

  def addRun(self,run):
    if not type(run) is int:
      raise ValueError('run must be an int: '+str(run))
    self.rfgs[run]=RunFileGroup()

  def addRuns(self,runs):
    for run in runs:
      self.addRun(run)

  def setGroupSize(self,groupSize):
    self.groupSize=int(groupSize)

  def addFile(self,fileName):
    rf=RunFile(fileName)
    # ignore if run# is not registered:
    if rf is None or not rf.runNumber in self.rfgs:
      return
    self.rfgs[rf.runNumber].addFile(fileName)

  def addDir(self,dirName):
    _LOGGER.info('Adding directory '+dirName+' ...')
    for dirpath,dirnames,filenames in os.walk(dirName):
      for filename in filenames:
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
    groups=[]
    phaseList=[]
    for run,rfg in self.rfgs.items():
      # make a new group unless we're allowed to combine runs:
      if not self.combineRuns:
        if len(phaseList)>0:
          groups.append(phaseList)
        phaseList=[]
      # loop over the files in this run:
      for rf in rfg.runFileList:
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
    for run,rfg in self.rfgs.items():
      for rf in rfg.runFileList:
        flatList.append(rf.fileName)
    return flatList

  def getRunList(self,minFileCount=1):
    runs=[]
    for run,rfg in self.rfgs.items():
      if minFileCount>0 and rfg.size()<minFileCount: continue
      runs.append(run)
    return sorted(runs)

  def showGroups(self):
    for group in self.getGroups():
      print(group)

  def showFlatList(self):
    for key,val in self.rfgs.items():
      print(key,)
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

