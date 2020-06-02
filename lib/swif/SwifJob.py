import os,sys,json,logging,collections

class SwifJob:

  __JSONFORMAT={'indent':2,'separators':(',',': ')}

  # defaults are for decoding a 2 GB evio file
  def __init__(self,workflow):
    self.abbreviations={}
    self.env={}
    self.number=-1
    self.workflow=workflow
    self.phase=0
    self.project='clas12'
    self.track='reconstruction'
    self.cores=1
    self.os='general'
    self.time='2h'
    self.disk='3GB'
    self.ram='1GB'
    self.shell='/bin/tcsh'
    self.tags=collections.OrderedDict()
    self.antecedents=[]
    self.conditions=[]
    self.logDir=None
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

  def setTrack(self,track):
    self.track=track

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

  def setCmd(self,cmd):
    self.cmd=cmd

  def setShell(self,shell):
    self.shell=shell

  def setLogDir(self,logDir):
    self.logDir=logDir

  def _addIO(self,io,local,remote):
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
    for full,short in list(self.abbreviations.items()):
      x=x.replace(full,short)
    return x

  def getJobName(self):
    task=''
    if 'mode' in self.tags:
      task='-'+self.abbreviate(self.tags['mode'])
    name='%s-p%d%s-%.5d'%(self.workflow,self.phase,task,self.number)
    if len(name)>50:
      logging.getLogger(__name__).critical('Greater than max job name length (50 characters): '+name)
      sys.exit()
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
    cmd+='mkdir -p %s ; touch %s ;'%(self.logDir,self.logDir)
    cmd+='env | egrep -e SWIF -e SLURM ;'
    cmd+='echo $PWD ; pwd ;'
    cmd+='expr $PWD : ^/scratch/slurm'
    for xx in list(self.env.keys()):
      cmd+=' && setenv '+xx+' "'+self.env[xx]+'"'
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
    return cmd

  def getShell(self):

    job=('swif add-job -create -workflow '+self.workflow+' -slurm '
      '-project '+self.project+' -track '+self.track+' '+' -os '+self.os+' '
      '-time '+self.time+' -cores '+str(self.cores)+' '
      '-disk '+self.disk+' -ram '+self.ram+' -shell '+self.shell)

    for ant in self.antecedents: job += ' -antecedent '+ant
    if con in self.conditions: job += ' -condition file:///'+con

    if not self.phase is None: job += ' -phase '+str(self.phase)

    for key,val in list(self.tags.items()): job += ' -tag %s %s'   %(key,val)
    for xx in self.inputs:  job += ' -input %s %s' %(xx['local'],xx['remote'])
    for xx in self.outputs: job += ' -output %s %s'%(xx['local'],xx['remote'])

    if self.logDir is not None:
      job += ' -stdout file:'+self.getLogPrefix()+'.out'
      job += ' -stderr file:'+self.getLogPrefix()+'.err'

    job += ' \''+self._createCommand()+'\''

    return job

  def getJson(self):
    jsonData = collections.OrderedDict()
    jsonData['os']=self.os
    jsonData['name']=self.getJobName()
    jsonData['phase']=self.phase
    jsonData['project']=self.project
    jsonData['track']=self.track
    jsonData['shell']=self.shell
    jsonData['cpuCores']=self.cores
    jsonData['diskBytes']=self.getBytes(self.disk)
    jsonData['ramBytes']=self.getBytes(self.ram)
    jsonData['timeSecs']=self.getSeconds(self.time)
    jsonData['tags']=self.tags
    jsonData['command']=self._createCommand()
    if len(self.antecedents)>0:
      jsonData['antecedents']=self.antecedents
    if len(self.conditions)>0:
      jsonData['conditions']=self.conditions
    if len(self.inputs)>0:
      jsonData['input']=self.inputs
    if len(self.outputs)>0:
      jsonData['output']=self.outputs
    if self.logDir is not None:
      jsonData['stdout']='file:'+self.getLogPrefix()+'.out'
      jsonData['stderr']='file:'+self.getLogPrefix()+'.err'
    return json.dumps(jsonData,**SwifJob.__JSONFORMAT)

if __name__ == '__main__':
  job=SwifJob('foobar')
  job.setCmd('ls -l')
  job.addTag('key','val')
  job.addTag('foo','bar')
  job.setLogDir('/tmp/logs')
  job.setPhase(77)
  print((job.getShell()))
  print((job.getJson()))

