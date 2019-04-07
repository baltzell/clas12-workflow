#!/usr/bin/env python
import sys
from ChefConfig import ChefConfig

cc = ChefConfig(sys.argv[1:])

workflow = cc.getWorkflow()

print 'Generating workflow ...'

workflow.generate()

print 'Created %d jobs based on %d runs with %d total files.'%\
    (len(workflow.jobs),len(workflow.getRunList(0)),workflow.getFileCount())

jobFilePrefix='./jobs/'+workflow.name

print 'Writing workflow to '+jobFilePrefix+'.json ...'

with open(jobFilePrefix+'.json','w') as out:
  out.write(workflow.getJson())

with open(jobFilePrefix+'.sh','w') as out:
  out.write(workflow.getShell())

if cc.get('submit'):
  print 'Submitting %s.json with %d jobs ...'%(jobFilePrefix,len(workflow.jobs))
  workflow.submitJson()

