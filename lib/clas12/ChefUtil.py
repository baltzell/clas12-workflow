import os,re,sys,subprocess,logging

from RcdbManager import RcdbManager

_RCDB=None
_LOGGER=logging.getLogger(__name__)

DEFAULT_EVIO_BYTES=2e9    # 
DEFAULT_DECODED_BYTES=4e9 # from five 2GB EVIO files
DEFAULT_DST_BYTES=1.5e9   # from five 2GB EVIO files

_DIRSMADE=[]
def mkdir(path,tag=None):
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

def getCoatjavaVersion(path):
  cj=path.split('/').pop().split('_').pop()
  m=re.match('(\d+)[abc]\.(\d+)\.(\d+)',cj)
  if m is not None:
    return [int(m.group(1)),int(m.group(2)),int(m.group(3))]
  m=re.match('.*\d+\.\d+\.\d+_(\d+)[abc]\.(\d+)\.(\d+).*',path)
  if m is not None:
    return [int(m.group(1)),int(m.group(2)),int(m.group(3))]
  return None

def getTrainIndices(yamlfile):
  ids=[]
  for line in open(yamlfile,'r').readlines():
    if line.strip().find('id: ')==0:
      if int(line.strip().split()[1]) not in ids:
        ids.append(int(line.strip().split()[1]))
  return sorted(ids)

def getSchemaName(yamlfile):
  for line in open(yamlfile,'r').readlines():
    if line.strip().find('schema_dir: ')==0:
      s=line.strip().strip('/').split('/').pop().strip('"')
      if   s=='monitoring':  s='mon'
      elif s=='calibration': s='calib'
      return s
  return ''

def getReconFileBytes(schema,decodedfile):
  if schema is not None and schema.startswith('/'):
    schema=getSchemaName(schema)
  s = DEFAULT_DECODED_BYTES
  if decodedfile is not None and os.path.isfile(decodedfile):
    s = getFileBytes(decodedfile)
  if   schema=='dst':   s *= 0.5
  elif schema=='calib': s *= 1.3
  elif schema=='mon':   s *= 1.6
  else:                 s *= 4.0
  return s

def getReconDiskReq(schema,decodedfile):
  s = 0
  if os.path.isfile(decodedfile):
    s += getFileBytes(decodedfile)
  else:
    s += DEFAULT_DECODED_BYTES
  s += getReconFileBytes(schema,decodedfile)
  return str(int(s/1e9)+1)+'GB'

def getTrainDiskReq(schema,reconfiles):
  s = 0
  for f in reconfiles:
    if not os.path.isfile(f):
      s += getReconFileBytes(schema,None)
    else:
      s += getFileBytes(f)
  # this 1.5 assumes trains will be at most half of recon:
  return '%.0fGB'%(1.5*s/1e9+1)

def getMergeDiskReq(nfiles):
  return str(int(2*nfiles*0.5)+3)+'GB'

def getMergeTimeReq(nfiles):
  return str(int(2*nfiles/10)+1)+'h'

def getDecoderOpts(run,cfg):
  global _RCDB
  s,t=None,None
  if 'solenoid' in cfg:
    s=cfg['solenoid']
  if 'torus' in cfg:
    t=cfg['torus']
  if s is None:
    if _RCDB is None:
      _RCDB=RcdbManager()
    s = _RCDB.getSolenoidScale(int(run))
    if s is None:
      _LOGGER.critical('Unknown solenoid scale for '+str(run))
      sys.exit()
  if t is None:
    if _RCDB is None:
      _RCDB=RcdbManager()
    t = _RCDB.getTorusScale(int(run))
    if t is None:
      _LOGGER.critical('Unknown torus scale for '+str(run))
      sys.exit()
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
  x=subprocess.check_output(['hipo-utils','-info',filename])
  for line in reversed(x.split('\n')):
    cols=line.strip().split()
    if len(cols)==3 and line.strip().find('Entries = ')==0:
      try:
        return int(cols[2])
      except:
        _LOGGER.error('invalid entries from hipo-utils')
        return None
  _LOGGER.error('cannot find entries from hipo-utils')
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
        run=line.strip().split()
        # ignore comment lines starting with a "#":
        if line.strip().startswith("#"):
          continue
        # ignore empty lines:
        if len(run)<1:
          continue
        # otherwise first column must be the number:
        try:
          runs.append(int(run[0]))
        except:
          _LOGGER.critical('Run numbers must be integers:  %s (%s)'%(fileName,line))
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

