import sys
from RunFileUtil import RunFileGroups
from ChefUtil import RcdbManager

runStart = int(sys.argv[1])
runEnd = int(sys.argv[2])

rfgs = RunFileGroups()

for run in range(runStart,runEnd+1):
  rfgs.addRun(run)

print 'loading files ...'
for fileName in open('/home/baltzell/clas12/rga-spring.list','r').readlines():
  rfgs.addFile(fileName)

rcdb = RcdbManager()

print 'checking rcdb ...'
missing=[]
for run in rfgs.getRunList(100):
  try:
    t=rcdb.getTorusScale(run)
    s=rcdb.getSolenoidScale(run)
  except AttributeError:
    missing.append(run)

print missing


