import os,re,sys,subprocess,logging

from RcdbManager import RcdbManager
import ClaraYaml

_RCDB=None
_LOGGER=logging.getLogger(__name__)

DEFAULT_EVIO_BYTES=2e9    # 2 GB EVIO file 
DEFAULT_DECODED_BYTES=4e9 # from five 2GB EVIO files
DEFAULT_DST_BYTES=1.5e9   # from five 2GB EVIO files
DEFAULT_RECON_TIME=1.5    # seconds per event
DEFAULT_EVENTS=5*7e4      # events in a file

_DIRSMADE=[]
def mkdir(path,tag=None):
  if path.startswith('/mss') or path.startswith('/cache'):
    return
  if path not in _DIRSMADE:
    if path.startswith('/mss/'):
      path=path.replace('/mss/','/cache/',1)
    if tag is None:
      _LOGGER.info('Making output directory: '+path)
    else:
      _LOGGER.info('Making '+tag+' directory: '+path)
    _DIRSMADE.append(path)
  if path is not None and not path.startswith('/mss'):
    if os.access(path,os.F_OK):
      if not os.access(path,os.W_OK):
        _LOGGER.critical('You do not have write permissions: '+path)
        sys.exit(1)
    else:
      try:
        if not path.startswith('/cache'):
          os.makedirs(path)
      except:
        _LOGGER.critical('Cannot make directory: '+path)
        sys.exit(1)

def getFileBytes(path):
  if os.path.isfile(path):
    if path.startswith('/mss'):
      for line in open(path,'r').readlines():
        cols=line.strip().split('=')
        if len(cols)==2 and cols[0]=='size':
          return int(cols[1])
    else:
      return os.path.getsize(path)
  return None

def getReconFileBytes(schema,decodedfile):
  if schema is not None and schema.startswith('/'):
    schema=ClaraYaml.getSchemaName(schema)
  s = DEFAULT_DECODED_BYTES
  if decodedfile is not None and os.path.isfile(decodedfile):
    s = getFileBytes(decodedfile)
  if   schema=='dst':   s *= 0.5
  elif schema=='dsthb': s *= 1.0
  elif schema=='trig':  s *= 1.0
  elif schema=='calib': s *= 1.5
  elif schema=='mon':   s *= 1.7
  elif schema=='full':  s *= 2.0
  elif schema=='dcalign':  s *= 1.0
  else:
    _LOGGER.warning('Unrecognized schema from YAML (%s), disk request may be bad.'%str(schema))
    s *= 4.0
  return s

def getReconSeconds(decodedfile):
  nevents = DEFAULT_EVENTS
  if os.path.isfile(decodedfile):
    if decodedfile.endswith('.hipo') and not decodedfile.startswith('/mss'):
      nevents = countHipoEvents(decodedfile)
      if nevents is None:
        nevents = DEFAULT_EVENTS
  s = 2 * nevents * DEFAULT_RECON_TIME
  return s

def getTrainDiskBytes(schema,reconfile):
  s = 0
  if not os.path.isfile(reconfile):
    s += getReconFileBytes(schema,None)
  else:
    s += getFileBytes(reconfile)
  # this 1.5 assumes trains will be at most half of recon:
  # the additional 12% is an empirical fudge factor
  return 1.5*s*1.12

def getMergeDiskReq(nfiles):
  return str(int(2*nfiles*0.5)+3)+'GB'

def getMergeTimeReq(nfiles):
  return str(int(2*nfiles/10)+1)+'h'

def getDecoderOpts(run,cfg=None):
  global _RCDB
  s,t = None,None
  if cfg is not None:
    if 'solenoid' in cfg:
      s = cfg['solenoid']
    if 'torus' in cfg:
      t = cfg['torus']
  if s is None:
    if _RCDB is None:
      _RCDB = RcdbManager()
    s = _RCDB.getSolenoidScale(int(run))
    if s is None or s == '':
      _LOGGER.critical('Unknown solenoid scale for '+str(run))
      sys.exit(2)
  if t is None:
    if _RCDB is None:
      _RCDB = RcdbManager()
    t = _RCDB.getTorusScale(int(run))
    if t is None or t == '':
      _LOGGER.critical('Unknown torus scale for '+str(run))
      sys.exit(2)
  return '-c 2 -s %.4f -t %.4f'%(s,t)

def getFileList(fileOrDir):
  fileList=[]
  if os.path.isdir(fileOrDir):
    for dirpath,dirnames,filenames in os.walk(fileOrDir):
      for filename in filenames:
        fileList.append(dirpath.strip()+'/'+filename.strip())
  elif os.path.isfile(fileOrDir):
    with open(fileOrDir,'r') as file:
      fileList.extend(file.readlines())
  else:
    _LOGGER.critical('Argument must be a file or directory.')
    sys.exit(1)
  return fileList

def countHipoEvents(filename):
  import shutil
  if not shutil.which('hipo-utils'):
    _LOGGER.critical('Cannot find hipo-utils in $PATH.')
    sys.exit(1)
  x=subprocess.check_output(['hipo-utils','-info',filename])
  for line in reversed(x.decode('UTF-8').split('\n')):
    cols=line.strip().split()
    if len(cols)==7 and cols[2]=='number' and cols[3]=='of' and cols[4]=='events':
      try:
        return int(cols[6])
      except:
        _LOGGER.error('Invalid entries from hipo-utils')
        return None
  _LOGGER.warning('Cannot find entries from hipo-utils')
  return None

def getRunList(data):
  runs=[]
  # recurse if it's a list:
  if isinstance(data,list):
    for xx in data:
      runs.extend(getRunList(xx))
  else:
    data=str(data)
    # first column is run# if it's a file:
    if os.access(data,os.R_OK):
      _LOGGER.info('Reading run numbers from file: '+data)
      for line in open(data,'r').readlines():
        # ignore comment lines starting with a "#":
        if line.startswith("#"):
          continue
        # ignore empty lines:
        if len(line.strip())<1:
          continue
        # first column is the run:
        run = line.strip().split().pop(0).strip()
        # otherwise first column must be the number:
        try:
          runs.append(int(run))
        except:
          _LOGGER.critical('Run numbers must be integers:  %s (%s)'%(data,line))
          sys.exit()
      _LOGGER.info('Read run numbers:  '+','.join([str(x) for x in runs]))
    # else it's a string run list:
    else:
      _LOGGER.info('Adding run numbers: '+data)
      for run in data.split(','):
        if run.find('-')<0:
          try:
            runs.append(int(run))
          except:
            _LOGGER.critical('Run numbers must be integers:  '+run)
            sys.exit()
        else:
          if run.count('-') != 1:
            _LOGGER.critical('Invalid run range: '+run)
            sys.exit()
          try:
            if run.startswith('-'):
              start=0
              end=int(run.lstrip('-'))
            elif run.endswith('-'):
              start=int(run.rstrip('-'))
              end=99999
            else:
              start,end=run.split('-')
              start=int(start)
              end=int(end)
            for run in range(start,end+1):
              runs.append(run)
          except:
            _LOGGER.critical('Run numbers must be integers:  '+run)
            sys.exit()
  return runs

def hipoIntegrityCheck(filename):
  if not os.path.exists(filename): return 201
  if os.path.getsize(filename)<128: return 202
  import shutil
  if not shutil.which('hipo-utils'):
    _LOGGER.critical('Cannot find hipo-utils in $PATH.')
    sys.exit(1)
  cmd=['hipo-utils','-test',filename]
  p=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
  while True:
    line=p.stdout.readline().rstrip()
    if not line:
      break
    print(line)
  p.wait()
  return p.returncode

if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[%(name)-15s] %(message)s')
  logger=logging.getLogger(__name__)
  print(getRunList(sys.argv[1]))
  #print(countHipoEvents(sys.argv[1]))

