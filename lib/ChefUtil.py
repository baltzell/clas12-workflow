import os

def mkdir(path):
  if os.access(path,os.F_OK):
    if not os.access(path,os.W_OK):
      raise IOError('Permissions error on '+path)
  else:
    os.makedirs(path)

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

def getRunList(cfg):
  runs=[]
  for run in cfg['run']:
    run = str(run)
    print 'Adding run(s) '+run+' ...'
    for run in run.split(','):
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
  for fileName in cfg['runFile']:
    print 'Adding runs from '+fileName+' ...'
    if not os.access(fileName,os.R_OK):
      print '\nERROR:  File is not readable:  '+fileName+'\n'
      return None
    for line in open(fileName,'r').readlines():
      run=line.strip().split()[0]
      try:
        runs.append(int(run))
      except:
        print '\nERROR: Run numbers must be integers:  %s (%s)\n'%(fileName,line)
        return None
  return runs

