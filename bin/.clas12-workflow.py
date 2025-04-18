#!/usr/bin/env python3
import sys,os,logging
from Logging import ColoredLogger
from ChefConfig import ChefConfig

logger = logging.getLogger(__name__)

cc=ChefConfig(sys.argv[1:])
workflow=cc.getWorkflow()

workflow.generate()

logger.info('Created workflow with %d jobs based on %d runs with %d total input files and %d phases'%\
    (len(workflow.jobs),len(workflow.getRunList()),workflow.getFileCount(),workflow.phase+1))

if os.path.exists(workflow.name+'.json'):
  logger.critical('File already exists:  '+workflow.name+'.json')
  sys.exit(1)

logger.info('Writing workflow to %s/%s.json'%(os.path.realpath('.'),workflow.name))
with open(workflow.name+'.json','w') as out:
  out.write(workflow.getJson())

if len(workflow.ignored)>0:
    logger.warning('Ignored runs due to strict RCDB checking: '+'.'.join([str(x) for x in workflow.ignored]))

if cc.get('submit'):
  logger.info('Submitting %s.json with %d jobs ...\n'%(workflow.name,len(workflow.jobs)))
  workflow.submitJson()

