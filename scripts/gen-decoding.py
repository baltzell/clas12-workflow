#!/usr/bin/env python
import sys
from ChefConfig import ChefConfig

cc=ChefConfig(sys.argv[1:])
workflow=cc.getWorkflow()

print 'Generating workflow ...'
workflow.generate()

print 'Creating %d jobs based on %d runs with %d total files.'%\
    (len(workflow.jobs),len(workflow.getRunList(1)),workflow.getFileCount())

print 'Writing workflow to ./'+workflow.name+'.json ...'
with open(workflow.name+'.json','w') as out:
  out.write(workflow.getJson())

# for big workflows, swif -add-job is much slower than swif -import:
#with open(workflow.name+'.sh','w') as out:
#  out.write(workflow.getShell())

if cc.get('submit'):
  print 'Submitting %s.json with %d jobs ...'%(workflow.name,len(workflow.jobs))
  workflow.submitJson()

