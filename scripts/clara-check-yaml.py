#!/usr/bin/env python
import sys,os,logging,argparse

import ClaraYaml

# used to check for classes in jars:
CLARA_HOME = '/group/clas12/packages/clara/4.3.12_6.5.12'

logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[ %(name)-15s ] %(message)s')

cli=argparse.ArgumentParser(description='Check validity of CLARA YAML file to prevent common user errors.',epilog='Warnings and fatal errors are logged, and fatal errors abort and give non-zero exit code.  Fatal errors are marked with an asterisk:  (1*) All services specify a class and have a corresponding configuration section.  (2*) All classes exist in jars in Clara installation.  (3*) Names do not include spaces.  (4*) No non-ASCII characters.  (5*) CCDB timestamps have proper format.  (6*) CCDB variations actually exist in the database.  (7) All services specify CCDB timestamp/variation.')

cli.add_argument('filename',help='path to YAML file to check')
cli.add_argument('-clara',help='path to CLARA installation',type=str,default='/group/clas12/packages/clara/4.3.12_6.5.12')

args = cli.parse_args(sys.argv[1:])

if not os.path.isfile(args.filename):
  cli.error('Missing YAML file:  '+args.filename)
  sys.exit(1)
else:
  cy = ClaraYaml.ClaraYaml(args.filename,args.clara)
  if cy.checkIntegrity():
    sys.exit(0)
  else:
    sys.exit(2)

