from CLAS12Workflow import CLAS12Workflow

#
# ThreePhaseDecoding
#
# Requires N GB of temporary disk space
#
# 0 = decode N
# 1 = merge N
# 2 = delete+move N
# 3 ...
#
class ThreePhaseDecoding(CLAS12Workflow):

  def __init__(self,name,cfg):
    CLAS12Workflow.__init__(self,name,cfg)

  def generate(self):

    self.jobs=[]
    phase=0
    for evioFiles in self.getGroups():

      phase += 1
      hipoFiles = self.decode(phase,evioFiles)

      phase += 1
      mergedFiles = self.merge(phase,hipoFiles)

      phase += 1
      self.move(phase,mergedFiles)
      self.delete(phase,hipoFiles)

#
# DecodingReconTest
#
# WARNING:  Does NOT delete anything!
#
# 0 = decode
# 1 = merge
# 2 = recon
# 4 ...
#
class DecodingReconTest(CLAS12Workflow):

  def __init__(self,name,cfg):
    CLAS12Workflow.__init__(self,name,cfg)

  def generate(self):

    self.jobs=[]
    phase=0
    for evioFiles in self.getGroups():

      phase += 1
      hipoFiles = self.decode(phase,evioFiles)

      phase += 1
      mergedFiles = self.merge(phase,hipoFiles)

      phase += 1
      self.recon(phase,hipoFiles)
      self.recon(phase,mergedFiles)

#
# RollingDecoding
#
# Requires 3*N GB of temporary disk space
#
# 0 = decode N0
# 1 = decode N1, merge N0
# 2 = decode N2, merge N1, delete+move N0
# 3 = decode N3, merge N2, delete+move N1
# ...
# i   = decode N(i), merge N(i-1), delete+move N(i-2),
# i+1 = merge N(i), delete+move N(i-1)
# i+2 = delete+move N(i)
#
class RollingDecoding(CLAS12Workflow):

  def __init__(self,name,cfg):
    CLAS12Workflow.__init__(self,name,cfg)

  def generate(self):

    self.jobs=[]
    phase=0

    decodeQueue=self.getGroups()
    mergeQueue,deleteQueue,moveQueue=[],[],[]

    while True:

      phase += 1

      if len(deleteQueue)>0:
        self.delete(phase,deleteQueue.pop(0))
        self.move(phase,moveQueue.pop(0))

      if len(mergeQueue)>0:
        hipoFiles = mergeQueue.pop(0)
        mergedFiles = self.merge(phase,hipoFiles)
        deleteQueue.append(hipoFiles)
        moveQueue.append(mergedFiles)

      if len(decodeQueue)>0:
        hipoFiles = self.decode(phase,decodeQueue.pop(0))
        mergeQueue.append(hipoFiles)

      if len(decodeQueue)==0 and len(mergeQueue)==0 and len(deleteQueue)==0:
        break

#
# SinglesOnlyDecoding
#
# Requires at least N GB of temporary disk space.
#
# Just writes decoded HIPO files to disk, no merging, and requires an
# independent (cron) task to merge and move (and maintain N GB).
#
class SinglesOnlyDecoding(CLAS12Workflow):

  def __init__(self,name,cfg):
    CLAS12Workflow.__init__(self,name,cfg)

  def generate(self):

    self.jobs=[]
    phase=0

    for evioFiles in self.getGroups():
      phase += 1
      self.decode(phase,evioFiles)

class ClaraSingles(CLAS12Workflow):

  def __init__(self,name,cfg):
    CLAS12Workflow.__init__(self,name,cfg)

  def generate(self):

    reconfiles=[]

    self.jobs=[]
    phase=0

    for hipoFiles in self.getGroups():
      reconfiles.extend(self.reconclara(phase,hipoFiles))

    return reconfiles

if __name__ == '__main__':
  import os,sys
  from ChefConfig import getConfig
  cli,cfg = getConfig(sys.argv[1:])
  cfg['outDir']=os.getenv('HOME')+'/tmp/clas12-workflow/outDir'
  cfg['workDir']=os.getenv('HOME')+'/tmp/clas12-workflow/workDir'
  workflow = RollingDecoding('test',cfg)
  workflow.setPhaseSize(1000)
  workflow.addRun(4013)
  workflow.addFiles(open('/home/baltzell/clas12/rga/rga-spring-files.txt','r').readlines())
  workflow.generate()
  print workflow.getShell()
  print workflow.getJson()

