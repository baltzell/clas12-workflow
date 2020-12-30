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

class JobErrors:
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

class SlurmErrors(JobErrors):
  _BITS=[
      'TIME',
      'NODE',
      'PREE',
      'MEM',
      'USER',
      'ALIVE']
  def __init__(self):
    JobErrors.__init__(self)
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

