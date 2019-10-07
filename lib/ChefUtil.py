import os,sys

def mkdir(path):
  if path is not None:
    if os.access(path,os.F_OK):
      if not os.access(path,os.W_OK):
        sys.exit('\nERROR:  You do not have write permissions on '+path)
    else:
      try:
        os.makedirs(path)
      except:
        sys.exit('\nERROR:  Cannot make directory: '+path)

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
    raise ValueError('It must be a file or a directory')
  return fileList

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
      print '\nReading run numbers from file: '+data+' ... ',
      for line in open(data,'r').readlines():
        run=line.strip().split()[0]
        try:
          runs.append(int(run))
          print run,
        except:
          print '\nERROR: Run numbers must be integers:  %s (%s)\n'%(fileName,line)
          return None
      print
    # else it's a string run list:
    else:
      print '\nAdding run numbers from command-line: '+data+' ...'
      for run in data.split(','):
        if run.find('-')<0:
          try:
            runs.append(int(run))
          except:
            print '\nERROR: Run numbers must be integers:  '+run+'\n'
            return None
        else:
          if run.count('-') != 1:
            print '\nERROR:  Invalid run range: '+run+'\n'
            return None
          try:
            start,end=run.split('-')
            start=int(start)
            end=int(end)
            for run in range(start,end+1):
              runs.append(run)
          except:
            print '\nERROR: Run numbers must be integers:  '+run+'\n'
            return None
  return runs

