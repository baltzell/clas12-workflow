import os
import re
import collections

__FILEREGEX='.*clas[_A-Za-z]*_(\d+)\.evio\.(\d+)$'
__DEBUG=False

def setFileRegex(regex):
  global __FILEREGEX
  print '\nChanging file regex to '+regex+' ... checking for compilation ...'
  re.compile(regex)
  __FILEREGEX = regex

def getFileRegex():
  return __FILEREGEX

def getRunFileNumber(fileName):
  mm = re.match(__FILEREGEX,fileName)
  if mm is None:
    if __DEBUG:
      print 'WARNING:  getRunFileNumber Failed on  '+fileName
    return None
  runno=mm.group(1)
  fileno=mm.group(2)
  # strip off leading zeroes for conversion to int, leaving '0':
  while runno.find('0')==0 and not runno=='0':
    runno=runno[1:]
  while fileno.find('0')==0 and not fileno=='0':
    fileno=fileno[1:]
  return {'run':int(runno),'file':int(fileno)}

class RunFile:
  def __init__(self,fileName):
    self.fileName=None
    self.runNumber=None
    self.fileNumber=None
    if not type(fileName) is str:
      raise ValueError('fileName must be a string: '+str(fileName))
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
    print self.fileName,self.runNumber,self.fileNumber

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
      raise ValueError('multiple run nubmers ',rf.runNumber)
    elif rf in self.runFileList:
      raise ValueError('duplicate: ',rf)
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
    print str(self.runNumber)
    for rf in self.runFileList: rf.show()

class RunFileGroups:
  def __init__(self):
    self.groupSize=0
    # maintain user's run insertion order:
    self.rfgs=collections.OrderedDict()
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
    # require run# to be registered:
    if rf is None or not rf.runNumber in self.rfgs:
      return
    self.rfgs[rf.runNumber].addFile(fileName)
  def addFiles(self,fileNames):
    for fileName in fileNames:
      self.addFile(fileName)
  def addDir(self,dirName):
    for dirpath,dirnames,filenames in os.walk(dirName):
      for filename in filenames:
        self.addFile(dirpath+'/'+filename)
  def getGroups(self):
    groups=[]
    for run,rfg in self.rfgs.iteritems():
      phaseList=[]
      for rf in rfg.runFileList:
        phaseList.append(rf.fileName)
        if self.groupSize>0 and len(phaseList)>=self.groupSize:
          groups.append(phaseList)
          phaseList=[]
      if len(phaseList)>0:
        groups.append(phaseList)
    return groups
  def getFlatList(self):
    flatList=[]
    for run,rfg in self.rfgs.iteritems():
      for rf in rfg.runFileList:
        flatList.append(rf.fileName)
    return flatList
  def getRunList(self,minFileCount):
    runs=[]
    for run,rfg in self.rfgs.iteritems():
      if rfg.size()<minFileCount: continue
      runs.append(run)
    return runs
  def showGroups(self):
    for group in self.getGroups():
      print group
  def showFlatList(self):
    for key,val in self.rfgs.iteritems():
      print key,
      val.show()
  def getFileCount(self):
    return len(self.getFlatList())

