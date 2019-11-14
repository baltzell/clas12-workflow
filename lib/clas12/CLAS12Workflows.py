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

    for evioFiles in self.getGroups():

      self.phase += 1
      hipoFiles = [x.outputData[0] for x in self.decode(self.phase,evioFiles)]

      self.phase += 1
      mergedFiles = [x.outputData[0] for x in self.merge(self.phase,hipoFiles)]

      self.phase += 1
      self.move(self.phase,mergedFiles)
      self.delete(self.phase,hipoFiles)

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

    for evioFiles in self.getGroups():

      self.phase += 1
      decodeJobs = self.decode(self.phase,evioFiles)

      self.phase += 1
      mergeJobs = self.merge(self.phase,decodeJobs)

      self.phase += 1
      self.reconclara(self.phase,decodeJobs)
      self.reconclara(self.phase,mergeJobs)

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

    decodeQueue=self.getGroups()
    mergeQueue,deleteQueue,moveQueue=[],[],[]

    while True:

      self.phase += 1

      if len(deleteQueue)>0:
        self.delete(self.phase,deleteQueue.pop(0))
        self.move(self.phase,moveQueue.pop(0))

      if len(mergeQueue)>0:
        decodedFiles = mergeQueue.pop(0)
        mergeJobs = self.merge(self.phase,decodedFiles)
        moveQueue.append([x.outputData[0] for x in mergeJobs])
        deleteQueue.append(decodedFiles)

      if len(decodeQueue)>0:
        decodeJobs = self.decode(self.phase,decodeQueue.pop(0))
        mergeQueue.append([x.outputData[0] for x in decodeJobs])

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
    for evioFiles in self.getGroups():
      self.decode(self.phase,evioFiles)
      self.phase += 1

class ClaraSingles(CLAS12Workflow):
  def __init__(self,name,cfg):
    CLAS12Workflow.__init__(self,name,cfg)
  def generate(self):
    for hipoFiles in self.getGroups():
      self.reconclara(self.phase,hipoFiles)

class Train(CLAS12Workflow):
  def __init__(self,name,cfg):
    CLAS12Workflow.__init__(self,name,cfg)
  def generate(self):
    for hipoFiles in self.getGroups():
      self.train(self.phase,hipoFiles)

class SinglesDecodeAndClara(CLAS12Workflow):
  def __init__(self,name,cfg):
    CLAS12Workflow.__init__(self,name,cfg)
  def generate(self):
    for evioFiles in self.getGroups():
      decodeJobs = self.decode(self.phase,evioFiles)
      reconJobs = self.reconclara(self.phase,decodeJobs)

class InlineDecodeMerge(CLAS12Workflow):
  def __init__(self,name,cfg):
    CLAS12Workflow.__init__(self,name,cfg)
  def generate(self):
    for evioFiles in self.getGroups():
      decodeJobs = self.decodemerge(self.phase,evioFiles)

class InlineDecodeMergeClara(CLAS12Workflow):
  def __init__(self,name,cfg):
    CLAS12Workflow.__init__(self,name,cfg)
  def generate(self):
    for evioFiles in self.getGroups():
      decodeJobs = self.decodemerge(self.phase,evioFiles)
      reconJobs = self.reconclara(self.phase,decodeJobs)

class RollingDecodeAndClara(CLAS12Workflow):

  def __init__(self,name,cfg):
    CLAS12Workflow.__init__(self,name,cfg)

  def generate(self):

    decodeQueue=self.getGroups()
    mergeQueue,deleteQueue,moveQueue,reconQueue=[],[],[],[]

    while True:

      self.phase += 1

      if len(reconQueue)>0:
        self.reconclara(self.phase-1,[reconQueue.pop(0)])

      if len(deleteQueue)>0:
        self.delete(self.phase,deleteQueue.pop(0))
        moveJobs=self.move(self.phase,moveQueue.pop(0))
        reconQueue.extend(moveJobs)

      if len(mergeQueue)>0:
        decodedFiles = mergeQueue.pop(0)
        mergeJobs = self.merge(self.phase,decodedFiles)
        moveQueue.append([x.outputData[0] for x in mergeJobs])
        deleteQueue.append(decodedFiles)

      if len(decodeQueue)>0:
        decodeJobs = self.decode(self.phase,decodeQueue.pop(0))
        mergeQueue.append([x.outputData[0] for x in decodeJobs])

      if len(decodeQueue)==0 and len(mergeQueue)==0 and len(deleteQueue)==0 and len(reconQueue)==0:
        break

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
  print(workflow.getShell())
  print(workflow.getJson())

