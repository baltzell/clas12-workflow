import re

from JobErrors import JobErrors

class ClaraErrors(JobErrors):
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
    JobErrors.__init__(self)
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

