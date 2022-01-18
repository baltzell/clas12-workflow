import os,sys,json,getpass,logging,collections

from SwifStatus import SWIF

class SwifJob:

  __JSONFORMAT={'indent':2,'separators':(',',': ')}
  __INVALIDPATHCHARS=['[',']','(',')','?','*']

  # defaults are for decoding a 2 GB evio file
  def __init__(self,workflow):
    self.abbreviations={'jput':'j'}
    self.env={}
    self.number=-1
    self.workflow=workflow
    self.phase=0
    self.account='clas12'
    self.partition='production'
    self.cores=1
    self.os='general'
    self.time='2h'
    self.disk='3GB'
    self.ram='1GB'
    self.shell='/bin/tcsh'
    self.tags=collections.OrderedDict()
    self.antecedents=[]
    self.conditions=[]
    self.logDir='/farm_out/'+getpass.getuser()
    self.cmd=''
    # for Auger staging:
    self.inputs=[]
    self.outputs=[]
    # for non-Auger staging:
    self.inputData=[]
    self.outputData=[]
    self.copyInputs=True

  def __str__(self):
    s = 'Phase %d : %s'%(self.phase,self.getJobName())
    for key,val in list(self.tags.items()):
      if key in ['run','mode','file']:
        s += ' : %s=%s'%(str(key),str(val))
    return s

  def addEnv(self,key,val):
    self.env[key]=val

  def setPartition(self,partition):
    self.partition=partition

  def getCores(self):
    return self.cores

  def setCores(self,cores):
    self.cores=cores

  def setNumber(self,number):
    self.number=number

  def addTag(self,key,val):
    if key in self.tags:
      if isinstance(self.tags[key],set):
        self.tags[key].add(val)
      elif self.tags[key]!=val:
        s=set()
        s.add(self.tags[key])
        s.add(val)
        self.tags[key]=s
    else:
      self.tags[key]=val

  def getTag(self,key):
    if key in self.tags: return self.tags[key]
    return None

  def setPhase(self,phase):
    if not phase is None and not type(phase) is int:
      raise ValueError('phase must be None or an integer.')
    self.phase=phase

  def setDisk(self,disk):
    self.disk=disk

  def setRam(self,ram):
    self.ram=ram

  def setTime(self,time):
    self.time=time

  def setHours(self,hours):
    if hours > 72:
      self.time = '72h'
    else:
      self.time = '%.0fh'%hours

  def setCmd(self,cmd):
    self.cmd=cmd

  def setShell(self,shell):
    self.shell=shell

  def setLogDir(self,logDir):
    self.logDir=logDir

  def checkLegalPath(self,path):
    for x in SwifJob.__INVALIDPATHCHARS:
      if path.find(x) >= 0:
        logging.getLogger(__name__).critical('Invalid character "'+x+'" in path: '+path)
        sys.exit(1)

  def _addIO(self,io,local,remote):
    self.checkLegalPath(local)
    self.checkLegalPath(remote)
    if not remote.find('mss:')==0 and not remote.find('file:')==0:
      if remote.find('/mss/')==0:
        remote='mss:'+remote
      else:
        remote='file:'+remote
    io.append({'local':local,'remote':remote})

  def addInput(self,local,remote):
    self._addIO(self.inputs,local,remote)

  def addOutput(self,local,remote):
    self._addIO(self.outputs,local,remote)

  def getBytes(self,size):
    scale=1
    if   size.find('GB')>0: scale=int(1e9)
    elif size.find('MB')>0: scale=int(1e6)
    elif size.find('KB')>0: scale=int(1e3)
    return int(scale * int(size.rstrip('GMKB')))

  def getSeconds(self,time):
    scale=1
    if   time.find('h')>0:  scale=60*60
    elif time.find('m')>0:  scale=60
    elif time.find('s')>0:  scale=1
    return int(scale * int(time.rstrip('secondminutehour')))

  def abbreviate(self,x):
    for full,short in reversed(sorted(list(self.abbreviations.items()))):
      x=x.replace(full,short)
    return x

  def getJobName(self):
    task=''
    if 'mode' in self.tags:
      task='-'+self.abbreviate(self.tags['mode'])
    name='%s-p%d%s-%.5d'%(self.workflow,self.phase,task,self.number)
    if len(name)>50:
      logging.getLogger(__name__).critical('Greater than max job name length (50 characters): '+name)
      sys.exit(1)
    return name

  def getLogPrefix(self):
    prefix='%s/%s'%(self.logDir,self.getJobName())
    for key,val in list(self.tags.items()):
      if key=='run':
        prefix+='_r'+val
      elif key=='file':
        prefix+='_f'+val
#      elif key=='run_group':
#        continue
#      elif key=='task':
#        continue
#      elif val.find('/')<0:
#        prefix+='_'+key+val
    return prefix

  def getOutputDirs(self):
    ret = set()
    for o in self.outputs:
      ret.add(os.path.dirname(o))
    for o in self.outputData:
      ret.add(os.path.dirname(o))
    return ret

  def makeOutputDirs(self):
    for x in self.getOutputDirs():
      if not os.path.isdir(x):
        logging.getLogger(__name__).info('Making output directory: '+x)
        os.makedirs(x,exist_ok=True)

  def getOutputPaths(self):
    for o in self.outputs:
      if o['remote'].startswith('file:'):
        yield o['remote'][5:]
      elif o['remote'].startswith('mss:'):
        yield o['remote'][4:]
      else:
        logging.getLogger(__name__).critical('Unknown remote prefix:  '+o['remote'])
        sys.exit(1)

  def outputExists(self):
    for o in self.getOutputPaths():
      if os.path.exists(o):
        return True
    return False

  # copy Auger symlinked inputs:
  def _getCopyInputsCmd(self):
    cmd='ls -l'
    for item in self.inputs:
      if item['remote'].find('mss:/mss')==0:
        remote = item['remote'].replace('mss:/mss','/cache')
        cmd += ' && rm -f %s'%item['local']
        cmd += ' && /bin/dd bs=1M if=%s of=%s'%(remote,item['local'])
    return cmd

  # rsync non-Auger outputs:
  def _getCopyOutputsCmd(self):
    files=[]
    for xx in self.outputData:
      if xx.startswith('/mss/'):
        xx=xx.replace('/mss/','/cache/',1)
      if xx not in files:
        files.append(xx)
    cmd=''
    for file in files:
      cmd+= ' && rsync %s %s/'%(os.path.basename(file),os.path.dirname(file))
    return cmd

  # jput non-Auger outputs, if on /cache:
  def _getJputOutputsCmd(self):
    files=[]
    for xx in self.outputData:
      if xx.startswith('/cache/'):
        xx=xx.replace('/cache/mss/','/cache/')
        if xx not in files:
          files.append(xx)
    if len(files)>0:
      return ' && jcache put '+' '.join(files)
    else:
      return ''

  def _createCommand(self):
    cmd='unalias -a ; '
    if self.shell.endswith('tcsh'):
      cmd+='set echo; '
    else:
      cmd+='set -v; '
    cmd+='mkdir -p %s ; touch %s ;'%(self.logDir,self.logDir)
    cmd+='env | egrep -e SWIF -e SLURM ;'
    cmd+='echo $PWD ; pwd ;'
    cmd+='expr $PWD : ^/scratch/slurm'
    for xx in list(self.env.keys()):
      if self.shell.endswith('tcsh'):
        cmd+=' && setenv '+xx+' "'+self.env[xx]+'"'
      else:
        cmd+=' && export '+xx+'="'+self.env[xx]+'"'
    if self.copyInputs:
      cmd+=' && '+self._getCopyInputsCmd()
    d=[]
    for o in self.outputs:
      if not o['remote'].startswith('mss:'):
        if os.path.dirname(o['remote'].replace('file:/','/',1)) not in d:
          d.append(os.path.dirname(o['remote'].replace('file:/','/',1)))
    if len(d)>0:
      cmd+=' && mkdir -p %s '%(' '.join(d))
    cmd+=' && ( '+self.cmd+' )'
    #cmd+=self._getJputOutputsCmd()
    # looks like SWIF2 has a 1 kB limit, while SWIF1 was much larger ...
    if len(cmd)>1e3-1:
      logging.getLogger(__name__).critical('Command might be too long:\n '+cmd)
    return cmd

  def getShell(self):
    cmd=[SWIF]
    cmd.extend(['add-job','-workflow',self.workflow,'-constraint',self.os])
    cmd.extend(['-account',self.account,'-partition',self.partition])
    cmd.extend(['-time',self.time,'-cores',str(self.cores)])
    cmd.extend(['-disk',self.disk,'-ram',self.ram,'-shell',self.shell])
    for ant in self.antecedents: cmd.extend(['-antecedent',ant])
    for con in self.conditions: cmd.extend(['-condition','file:///'+con])
    if self.phase is not None: cmd.extend(['-phase',str(self.phase)])
    for key,val in list(self.tags.items()): cmd.extend(['-tag',key,str(val)])
    for xx in self.inputs: cmd.extend(['-input',xx['local'],xx['remote']])
    for xx in self.outputs: cmd.extend(['-output',xx['local'],xx['remote']])
    if self.logDir is not None:
      cmd.extend(['-stdout',self.getLogPrefix()+'.out'])
      cmd.extend(['-stderr',self.getLogPrefix()+'.err'])
    cmd.append('\''+self._createCommand()+'\'')
    return ' '.join(cmd)

  def toJson(self):
    jsonData = collections.OrderedDict()
    jsonData['constraint']=self.os
    jsonData['name']=self.getJobName()
    jsonData['phase']=self.phase
    jsonData['account']=self.account
    jsonData['partition']=self.partition
    jsonData['shell']=self.shell
    jsonData['cpu_cores']=self.cores
    jsonData['disk_bytes']=self.getBytes(self.disk)
    jsonData['ram_bytes']=self.getBytes(self.ram)
    jsonData['time_secs']=self.getSeconds(self.time)
    jsonData['command']=[self._createCommand()]
    if len(self.tags)>0:
      jsonData['tags']=[]
      for k,v in list(self.tags.items()):
        jsonData['tags'].append({'name':k,'value':v})
    if len(self.antecedents)>0:
      jsonData['antecedents']=self.antecedents
    if len(self.conditions)>0:
      jsonData['conditions']=self.conditions
    if len(self.inputs)>0:
      jsonData['inputs']=self.inputs
    if len(self.outputs)>0:
      jsonData['outputs']=self.outputs
    if self.logDir is not None:
      jsonData['stdout']=self.getLogPrefix()+'.out'
      jsonData['stderr']=self.getLogPrefix()+'.err'
    return jsonData

  def getJson(self):
    return json.dumps(self.toJson(),**SwifJob.__JSONFORMAT)

class JputJob(SwifJob):
  def __init__(self,workflow):
    SwifJob.__init__(self,workflow)
    self.time='1h'
    self.disk='500MB'
    self.ram='500MB'
    self.addTag('mode','jput')
    self.jputfiles = []
  def addJputs(self,jobs):
    for j in jobs:
      if 'output' in j.toJson():
        for o in j.toJson()['output']:
          if o['remote'].startswith('file:/cache'):
            if (j.getJobName()) not in self.antecedents:
              self.antecedents.append(j.getJobName())
            if o['remote'][5:] not in self.jputfiles:
              self.jputfiles.append(o['remote'][5:])
    cmd = '/site/bin/jcache put ' + ' '.join(self.jputfiles)
    SwifJob.setCmd(self,cmd)

if __name__ == '__main__':
  job=SwifJob('foobar')
  job.setCmd('ls -l')
  job.addTag('key','val')
  job.addTag('foo','bar')
  print((job.getShell()))
  print((job.getJson()))

