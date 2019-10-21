#!/usr/bin/env python
import sys,os
from ChefConfig import ChefConfig

cc=ChefConfig(sys.argv[1:])
workflow=cc.getWorkflow()

print '\nGenerating workflow ...'
workflow.generate()

print '\nCreated workflow with %d jobs based on %d runs with %d total input files.'%\
    (len(workflow.jobs),len(workflow.getRunList(1)),workflow.getFileCount())

print '\nWriting workflow to ./'+workflow.name+'.json ...'
if os.path.exists(workflow.name+'.json'):
  sys.exit('ERROR: file already exists:  '+workflow.name+'.json')
with open(workflow.name+'.json','w') as out:
  out.write(workflow.getJson())

# for big workflows, swif -add-job is much slower than swif -import:
#with open(workflow.name+'.sh','w') as out:
#  out.write(workflow.getShell())

if cc.get('submit'):
  print '\nSubmitting %s.json with %d jobs ...'%(workflow.name,len(workflow.jobs))
  workflow.submitJson()

