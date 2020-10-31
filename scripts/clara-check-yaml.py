#!/usr/bin/env python
import sys,os,logging

import ClaraYaml

logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[ %(name)-15s ] %(message)s')
usage = 'Usage:  ClaraYaml yamlfile'
if len(sys.argv) < 2:
  print(usage)
elif not os.path.isfile(sys.argv[1]):
  print('Missing YAML file:  '+sys.argv[2])
  print(usage)
else:
  cy = ClaraYaml.ClaraYaml(sys.argv[1],'/group/clas12/packages/clara/4.3.12_6.5.12')
  cy.checkIntegrity()

