#!/usr/bin/env python
import sys
import os
import subprocess
from ChefConfig import ChefConfig
from CLAS12Workflows import *

cfg = dict(ChefConfig(sys.argv[1:]))

namePrefix='%s-%s-%s'%(cfg['runGroup'],cfg['task'],cfg['workflow'])
name='%s_R%dx%d_x%d'%(namePrefix,cfg['runs'][0],len(cfg['runs']),cfg['phaseSize'])

if cfg['model']==0:
  workflow = ThreePhaseDecoding(name,cfg)
elif cfg['model']==1:
  workflow = RollingDecoding(name,cfg)
elif cfg['model']==2:
  workflow = SinglesOnlyDecoding(name,cfg)
elif cfg['model']==3:
  workflow = DecodingReconTest(name,cfg)
else:
  sys.exit('FATAL ERROR:  unknown model: '+str(cfg['model']))

for anInput in cfg['inputs']:
  print 'Adding files from %s ...'%anInput
  workflow.addRuns(cfg['runs'])
  if os.path.isdir(anInput):
    workflow.addDir(anInput)
  elif os.path.isfile(anInput):
    workflow.addFiles([x.split()[0] for x in open(anInput,'r').readlines()])
  else:
    sys.exit('FATAL ERROR:  --inputs must be a file (containing a list of files) or a directory:\n'+anInput)

if workflow.getFileCount()<1:
  sys.exit('FATAL ERROR:  found no applicable input files.')

print 'Generating workflow ...'
workflow.generate()

print 'Created %d jobs based on %d runs with %d total files.'%(len(workflow.jobs),len(workflow.getRunList(0)),workflow.getFileCount())

jobFilePrefix='./jobs/'+name
print 'Writing workflow to '+jobFilePrefix+'.json ...'
with open(jobFilePrefix+'.json','w') as out:
  out.write(workflow.getJson())
with open(jobFilePrefix+'.sh','w') as out:
  out.write(workflow.getShell())

if cfg['submit']:
  print 'Submitting %s.json with %d jobs ...'%(jobFilePrefix,len(workflow.jobs))
  workflow.submitJson()

