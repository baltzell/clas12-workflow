import re,os

def readlines_reverse(filename):
  with open(filename) as qfile:
    qfile.seek(0, os.SEEK_END)
    position = qfile.tell()
    line = ''
    while position >= 0:
      qfile.seek(position)
      next_char = qfile.read(1)
      if next_char == "\n":
         yield line[::-1]
         line = ''
      else:
         line += next_char
      position -= 1
  yield line[::-1]

class Errors:
  _BITS=[]
  def __init__(self):
    self.bits=0
  def getIndex(self,string):
    return self._BITS.index(string)
  def getBit(self,string):
    return (self.bits & (1<<self.getIndex(string)))>0
  def setBit(self,string):
    self.bits |= (1<<self.getIndex(string))
  def unsetBit(self,string):
    mask=0
    for i,name in enumerate(self._BITS):
      if string!=name:
        mask |= 1<<i
    self.bits &= mask
  def __str__(self):
    ret=''
    for x in self._BITS:
      if self.getBit(x):
        ret+=x+' '
    return ret

class SlurmErrors(Errors):
  _BITS=[
      'TIME',
      'NODE',
      'PREE',
      'MEM',
      'USER',
      'ALIVE']
  def __init__(self):
    Errors.__init__(self)
    self.watchdog=False
  def parse(self,filename):
    n=0
    maxlines=5
    cancelled=False
    for line in readlines_reverse(filename):
      if line=='':
        continue
      #if n==0:
        #if line.find('waiting pid =')==0:
        #  self.setBit('ALIVE')
        #elif line.find('clara-wd:Error')>=0 and line.find('DPE_PID')>0:
        #  self.setBit('ALIVE')
      if line.find('clara-wd:SevereError  Stop the data-processing')>=0:
        self.watchdog=True
#      elif line.find('No space left on device')>0:
#        self.setBit('DISK')
      elif line.find('CANCELLED')>=0:
        cancelled=True
        if line.find('DUE TO TIME LIMIT')>=0:
          self.setBit('TIME')
        elif line.find('DUE TO NODE FAILURE')>=0:
          self.setBit('NODE')
        elif line.find('DUE TO PREEMPTION')>=0:
          self.setBit('PREE')
      elif cancelled:
        if line.find('Exceeded job memory limit')>=0:
          self.setBit('MEM')
      if n>maxlines:
        break
      n+=1
    if cancelled and self.bits==0:
      self.setBit('USER')

    for line in readlines_reverse(filename):
      if self.watchdog and line.find('clas-watchdog.sh')>0 and line.find('No such process')>0:
        self.watchdog=False
        break

class ClaraErrors(Errors):
  _BITS=[
      'NULL',
      'TRUNC',
      'CFG',
      'READ',
      'WRITE',
      'REG',
      'DEP',
      'UNDEP',
      'THREAD',
      'DIR',
      'CONT',
      'UDF',
      'DB',
      'WDOG',
      'HUGE']
  def __init__(self):
    Errors.__init__(self)
  def parse(self,lastline):
    if lastline is None:
      self.setBit('NULL')
    elif re.match('.*Could not configure.*no response for timeout.*',lastline.strip()) is not None:
      self.setBit('CFG')
    elif lastline.find('Could not stage input file')==0:
      self.setBit('READ')
    elif lastline.find('Could not open input')==0:
      self.setBit('READ')
    elif lastline.find('No space left on device')>=0:
      self.setBit('READ')
    elif lastline.find('Could not open output file')==0:
      self.setBit('WRITE')
    elif lastline.find('Could not save output')==0:
      self.setBit('WRITE')
    elif lastline.find('Cannot send query: registrar server response timeout')>=0:
      self.setBit('REG')
    elif lastline.find('failed request to deploy service')==0:
      self.setBit('DEP')
    elif lastline.find('undeployed services')==0:
      self.setBit('UNDEP')
    elif lastline.find('at java.lang.Thread.run(Thread.java:')==0:
      self.setBit('THREAD')
    elif lastline.find('Could not configure directories')>=0:
      self.setBit('DIR')
    elif lastline.find('could not start container')>=0:
      self.setBit('CONT')
    elif re.match('.*\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d\d\d.*',lastline.strip()) is not None:
      if lastline.find('Processing is complete.')<0:
        self.setBit('TRUNC')
    elif lastline.find('===========')==0:
      self.setBit('TRUNC')
    else:
      self.setBit('UDF')

