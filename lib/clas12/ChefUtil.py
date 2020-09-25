import os,re,sys,subprocess,logging

from RcdbManager import RcdbManager

_RCDB=None
_LOGGER=logging.getLogger(__name__)

DEFAULT_EVIO_BYTES=2e9    # 2 GB EVIO file 
DEFAULT_DECODED_BYTES=4e9 # from five 2GB EVIO files
DEFAULT_DST_BYTES=1.5e9   # from five 2GB EVIO files
DEFAULT_RECON_TIME=1.2    # Hz

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

def getTrainIndices(yamlfile):
  ids=[]
  for line in open(yamlfile,'r').readlines():
    if line.strip().find('#')==0:
      continue
    if line.strip().find('id: ')==0:
      if int(line.strip().split()[1]) not in ids:
        ids.append(int(line.strip().split()[1]))
  return sorted(ids)

def getTrainNames(yamlfile):
  names={}
  for x in getTrainIndices(yamlfile):
    names[x]=None
  # yaml parser does not come with stock python ...
  section=False
  for line in open(yamlfile,'r').readlines():
    if line.strip().find('#')==0:
      continue
    if line.strip().find('custom-names:')==0:
      section=True
      continue
    elif line.strip().endswith(':'):
      section=False
    if not section:
      continue
    cols=line.strip().split(':')
    if len(cols)==2:
      try:
        thisid=int(cols[0])
      except:
        continue
      if thisid not in names:
        _LOGGER.error('Invalid wagon id in custom-names in train yaml:  '+line)
      names[thisid]=cols[1].strip()
  # must be all-or-none:
  if None in list(names.values()):
    for x,y in list(names.items()):
      if y is not None:
        _LOGGER.error('Missing custom-name in train yaml:  '+str(names))
        sys.exit(42)
    for x,y in list(names.items()):
      names[x]='skim%d'%int(x)
  return names

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

def getReconDiskReq(schema,decodedfile,nfiles=1):
  s = 0
  if os.path.isfile(decodedfile):
    s += getFileBytes(decodedfile)
  else:
    s += DEFAULT_DECODED_BYTES
  s += getReconFileBytes(schema,decodedfile)
  return str(int(s*nfiles/1e9)+1)+'GB'

def getReconSeconds(decodedfile,nthreads,nfiles=1):
  t = 24*60*60
  if os.path.isfile(decodedfile):
    t = int(2*countHipoEvents(decodedfile)*DEFAULT_RECON_TIME*nfiles/nthreads)
  return t

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
  for line in reversed(x.decode('UTF-8').split('\n')):
    cols=line.strip().split()
    if len(cols)==7 and cols[2]=='number' and cols[3]=='of' and cols[4]=='events':
      try:
        return int(cols[6])
      except:
        _LOGGER.error('invalid entries from hipo-utils')
        return None
    break
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

def hipoIntegrityCheck(filename):
  if not os.path.exists(filename): return 201
  if os.path.getsize(filename)<128: return 202
  hu='hipo-utils'
  if os.getenv('COATJAVA') is not None:
    hu=os.getenv('COATJAVA')+'/bin/hipo-utils'
  elif os.getenv('CLAS12DIR') is not None:
    hu=os.getenv('CLAS12DIR')+'/bin/hipo-utils'
  cmd=[hu,'-test',filename]
  p=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
  while True:
    line=p.stdout.readline().rstrip()
    if not line:
      break
    print(line)
  p.wait()
  return p.returncode

