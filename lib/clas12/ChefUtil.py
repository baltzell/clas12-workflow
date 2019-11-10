import os,sys,subprocess,logging

_LOGGER=logging.getLogger(__name__)

_DIRSMADE=[]
def mkdir(path,tag=None):
  if path not in _DIRSMADE:
    if tag is None:
      _LOGGER.info('Making output directory at '+path)
    else:
      _LOGGER.info('Making '+tag+' directory at '+path)
    _DIRSMADE.append(path)
  if path is not None:
    if os.access(path,os.F_OK):
      if not os.access(path,os.W_OK):
        _LOGGER.critical('You do not have write permissions on '+path)
        sys.exit(1)
    else:
      try:
        os.makedirs(path)
      except:
        _LOGGER.critical('Cannot make directory: '+path)
        sys.exit(1)

def getMergeDiskReq(nfiles):
  return str(int(2*nfiles*0.5)+3)+'GB'

def getMergeTimeReq(nfiles):
  return str(int(2*nfiles/10)+1)+'h'

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
        run=line.strip().split()[0]
        try:
          runs.append(int(run))
        except:
          _LOGGER.error('Run numbers must be integers:  %s (%s)'%(fileName,line))
          return None
      _LOGGER.info('Read run numbers:  '+','.join([str(x) for x in runs]))
    # else it's a string run list:
    else:
      _LOGGER.info('Adding run numbers from command-line: '+data)
      for run in data.split(','):
        if run.find('-')<0:
          try:
            runs.append(int(run))
          except:
            _LOGGER.error('Run numbers must be integers:  '+run)
            return None
        else:
          if run.count('-') != 1:
            _LOGGER.error('Invalid run range: '+run)
            return None
          try:
            start,end=run.split('-')
            start=int(start)
            end=int(end)
            for run in range(start,end+1):
              runs.append(run)
          except:
            _LOGGER.error('Run numbers must be integers:  '+run)
            return None
  return runs

