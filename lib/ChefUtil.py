import os
import rcdb

def mkdir(path):
  if os.access(path,os.F_OK):
    if not os.access(path,os.W_OK):
      raise IOError('Permissions error on '+path)
  else:
    os.makedirs(path)

class RcdbManager():
  def __init__(self):
    self.data={}
  def loadRun(self,run):
    data={}
    db=rcdb.RCDBProvider('mysql://rcdb@clasdb.jlab.org/rcdb')
    data['solenoid']=db.get_condition(run,'solenoid_scale').value
    data['torus']   =db.get_condition(run,'torus_scale').value
    db.disconnect()
    self.data[run]=data
  def getSolenoidScale(self,run):
    if run not in self.data:
      self.loadRun(run)
    return self.data[run]['solenoid']
  def getTorusScale(self,run):
    if run not in self.data:
      self.loadRun(run)
    return self.data[run]['torus']

def getMergeDiskReq(nfiles):
  return str(int(2*nfiles*0.5)+3)+'GB'

def getMergeTimeReq(nfiles):
  return str(int(2*nfiles/10)+1)+'h'

def getFileList(fileOrDir):
  fileList=[]
  if os.path.isdir(fileOrDir):
    for dirpath,dirnames,filenames in os.walk(fileOrDir):
      for filename in filenames:
        fileList.append(dirpath.strip()+'/'+filename.strip())
  elif os.path.isfile(fileOrDir):
    with open(fileOrDir,'r') as file:
      fileList.extend(file.readlines())
  else:
    raise ValueError('It must be a file or a directory')
  return fileList

