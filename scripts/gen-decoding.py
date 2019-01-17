#!/usr/bin/env python
import sys
import os
import subprocess
from ChefConfig import getConfig
from CLAS12DecodingWorkflows import *

cli,cfg = getConfig(sys.argv[1:])

if len(cfg['runs'])<=0:
  print '\nERROR:  Must define run numbers.\n'
  cli.print_usage()
  sys.exit()

name='%s_R%dx%d_x%d'%(cfg['workflow'],cfg['runs'][0],len(cfg['runs']),cfg['phaseSize'])

if cfg['model']==0:
  workflow = ThreePhaseDecoding(name,cfg)
elif cfg['model']==1:
  workflow = RollingDecoding(name,cfg)
elif cfg['model']==2:
  workflow = SinglesOnlyDecoding(name,cfg)
else:
  sys.exit('ERROR:  unknown model: '+str(cfg['model']))

print 'Adding files from %s ...'%cfg['mssList']
workflow.addRuns(cfg['runs'])
if os.path.isdir(cfg['mssList']):
  workflow.addDir(cfg['mssList'])
elif os.path.isfile(cfg['mssList']):
  workflow.addFiles(open(cfg['mssList'],'r').readlines())
else:
  sys.exit('ERROR:  mssList must be a file (containing a list of files) or a directory:\n'+cfg['mssList'])

print 'Generating workflow ...'
workflow.generate()

print 'Writing workflow to ./jobs ...'
jobFilePrefix='./jobs/'+name
with open(jobFilePrefix+'.json','w') as out:
  out.write(workflow.getJson())
with open(jobFilePrefix+'.sh','w') as out:
  out.write(workflow.getShell())

if not cfg['dryRun']:
  print 'Submitting %s.json with %d jobs ...'%(jobFilePrefix,len(workflow.jobs))
  workflow.submitJson()

