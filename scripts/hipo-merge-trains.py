#!/usr/bin/env python
import re,os,sys,logging,subprocess,argparse
import ClaraYaml

logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[%(name)-14s] %(message)s')
logger=logging.getLogger(__name__)

cli=argparse.ArgumentParser(description='Merge a directory of clas12-workflow train outputs.')
cli.add_argument('-i',metavar='path',help='input directory',type=str,required=True)
cli.add_argument('-o',metavar='path',help='output directory',type=str,required=True)
cli.add_argument('-y',metavar='path',help='train yaml file (for determining skim names)',type=str,required=False)
args=cli.parse_args(sys.argv[1:])
args.i=args.i.rstrip('/')

if not os.path.isdir(args.i):
  logger.error('-i must be an existing directory of input files')
  sys.exit(-1)

basepath=None
singleRun=None
trainIndices=set()

# FIXME: this should be way less complicated
# FIXME: maybe args should include run number list/range
# FIXME: maybe should limit walk depth if that would simplify

# check if basename of input directory is a run number:
# (if so, we're going to limit to that run number)
mm=re.match('^(\d+)$',os.path.basename(args.i))
if mm is not None:
  singleRun=int(mm.group(1))
  logger.info('limiting to run '+str(singleRun))

# walk the input directory, requiring valid clas12-workflow
# train directory structure, ignoring/erroring otherwise:
logger.info('scanning input directory for train files ...')
for dirpath,dirnames,filenames in os.walk(args.i):
  # check that it's a run-number directory:
  mm=re.match('^(\d+)$',os.path.basename(dirpath))
  if mm is None:
    continue
  # if limiting to one run number, ignore others:
  if singleRun is not None and int(mm.group(1))!=singleRun:
    logger.info('ignoring run '+mm.group(1))
    continue
  # get the "basepath", one up from this run number directory:
  bp='/'+'/'.join(dirpath.split('/')[0:-1])
  # check that it's still the same basepath:
  if basepath is None:
    basepath=bp
  elif basepath != bp:
    logger.error('Found multiple basepaths:')
    logger.error(basepath)
    logger.error(bp)
    sys.exit(3)
  # accumulate wagon indices, assuming skim#_ prefix on HIPO files:
  for filename in filenames:
    mm=re.match('^skim(\d+)_.*\.hipo$',filename)
    if mm is not None:
      trainIndices.add(int(mm.group(1)))

if len(trainIndices)==0:
  logger.error('Found no train output files at '+args.i)
  sys.exit(2)
else:
  logger.info('Found %d wagons, merging them by run number ...'%len(trainIndices))

# retrieve train names from yaml file:
trainNames=None
if args.y is not None:
  trainNames=ClaraYaml.getTrainNames(args.y)

# call hipo-merge-runs.py once per trainIndex:
for trainIndex in sorted(trainIndices):
  logger.info('Merging wagon #'+str(trainIndex))
  if singleRun:
    inGlob='%s/%.6d/skim%d_*.hipo'%(basepath,singleRun,trainIndex)
  else:
    inGlob='%s/*/skim%d_*.hipo'%(basepath,trainIndex)
  # use custom names else just skim# indices:
  if trainNames is None or trainNames[trainIndex] is None:
    outStub='%s/skim%d/skim%d'%(args.o,trainIndex,trainIndex)
  else:
    outStub='%s/%s/%s'%(args.o,trainNames[trainIndex],trainNames[trainIndex])
  cmd=[os.path.dirname(os.path.realpath(__file__))+'/hipo-merge-runs.py']
  cmd.extend(['-A','-i',inGlob,'-o',outStub])
  print((' '.join(cmd)))
  p=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,universal_newlines=True)
  #while True:
  #  line=p.stdout.readline().rstrip()
  #  if not line:
  #    break
  #  print(line)
  for line in iter(p.stdout.readline, ''):
    print((line.rstrip()))
  p.wait()
  if p.returncode!=0:
    sys.exit(p.returncode)

