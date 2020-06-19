#!/usr/bin/env python
import sys,os,time,subprocess,logging

# requires hipo-utils to be in $PATH

import ChefUtil,ChefConfig,RunFileUtil

logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[%(name)-14s] %(message)s')
logger=logging.getLogger(__name__)

def getDecades(fileOrDir,runs):
  groups={}
  for filename in ChefUtil.getFileList(fileOrDir):
    rf=RunFileUtil.RunFile(filename)
    if rf is None: continue
    if rf.runNumber is None: continue
    if len(runs)>0 and rf.runNumber not in runs: continue
    runno = rf.runNumber
    fileno = rf.fileNumber
    decade = int(fileno/10)
    ones = fileno%10
    if not runno in groups:
      groups[runno]={}
    if not decade in groups[runno]:
      groups[runno][decade]=RunFileUtil.RunFileGroup()
    if rf in groups[runno][decade].runFileList:
      sys.exit('ERROR.  Duplicate.')
    groups[runno][decade].addFile(filename)
    if groups[runno][decade].size()>10:
      sys.exit('ERROR.  Too many files in a group.')
  return groups

def merge(decade,cfg):
  if not isinstance(decade,RunFileUtil.RunFileGroup):
    raise TypeError('Must be a RunFileGroup')
  run=decade.runNumber
  f1 = decade.runFileList[0].fileNumber
  f2 = decade.runFileList[len(decade.runFileList)-1].fileNumber
  outDir = cfg['outDir']+'/'+str(run)
  outputFile = outDir+'/'+cfg['mergePattern']%(run,f1,f2)
  logFile = cfg['workDir']+'/logs/merge/'+cfg['mergePattern']%(run,f1,f2)+'.log'
  cmd=['hipo-utils','-merge','-o',outputFile]
  cmd.extend([rf.fileName for rf in decade.runFileList])
  with open(logFile,'w') as file:
    foobar=subprocess.check_output(cmd,stderr=subprocess.STDOUT)
    file.write(foobar)
    file.flush()
    file.close()

cc = ChefConfig.ChefConfig(sys.argv[1:])
cfg = cc.cfg

print('Getting workDir groups ...')
availableDecades = getDecades(cfg['workDir'],cfg['runs'])

print('Getting inputs groups ...')
expectedDecades = getDecades(cfg['inputs'],cfg['runs'])

for run,decades in availableDecades.items():
  if not run in expectedDecades:
    continue
#    sys.exit('RUN?  '+str(run))

  ChefUtil.mkdir(cfg['outDir']+'/'+str(run))
  ChefUtil.mkdir(cfg['workDir']+'/logs')

  for decade in sorted(decades.keys()):
    if not decade in expectedDecades[run]:
      sys.exit('DECADE?  '+str(decade))
    if decades[decade].size() == expectedDecades[run][decade].size():
      print('  Complete Decade: ',run,decade,decades[decade].size(),decades[decade])
      old=True
      for fileName in [rf.fileName for rf in decades[decade].runFileList]:
        if (time.time()-os.stat(fileName).st_mtime)/60/60 < 1:
          old=False
          break
      if old:
        merge(decades[decade],cfg)
      else:
        print('** TOO NEW:        ',run,decade,decades[decade].size(),decades[decade])
    else:
      print('Incomplete Decade: ',run,decade,decades[decade],expectedDecades[run][decade])
    print()

