import sys
import collections
import json

class SwifJob:

  # defaults are for decoding a 2 GB evio file
  def __init__(self,workflow):
    self.number=-1
    self.workflow=workflow
    self.phase=0
    self.project='clas12'
    self.track='reconstruction'
    self.cores=1
    self.time='2h'
    self.disk='3GB'
    self.ram='1GB'
    self.shell='/bin/tcsh'
    self.tags=collections.OrderedDict()
    self.inputs=[]
    self.outputs=[]
    self.logDir=None
    self.cmd=''

  def setNumber(self,number):
    self.number=number

  def addTag(self,key,val):
    self.tags[key]=val

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
    self.cmd='unalias -a ; '+cmd

  def setShell(self,shell):
    self.shell=shell

  def setLogDir(self,logDir):
    self.logDir=logDir

  def addIO(self,io,local,remote):
    if not remote.find('mss:')==0 and not remote.find('file:')==0:
      if remote.find('/mss/')==0:
        remote='mss:'+remote
      else:
        remote='file:'+remote
    io.append({'local':local,'remote':remote})

  def addInput(self,local,remote):
    self.addIO(self.inputs,local,remote)

  def addOutput(self,local,remote):
    self.addIO(self.outputs,local,remote)

  def getCopyInputsCmd(self):
    cmd='ls -l'
    for item in self.inputs:
      if item['remote'].find('mss:/mss')==0:
        remote = item['remote'].replace('mss:/mss','/cache')
        cmd += ' && rm -f %s'%item['local']
        cmd += ' && /bin/dd bs=1M if=%s of=%s'%(remote,item['local'])
    return cmd

  def getJobName(self):
    name='%s-%.5d'%(self.workflow,self.number)
    if len(name)>50:
      print ''
      print 'ERROR:  Maximum (JLab batch) job name length is 50 characters.'
      print 'This is too long:  '+name
      print ''
      sys.exit()
    return name

  def getLogPrefix(self):
    prefix='%s/%s_p%d'%(self.logDir,self.getJobName(),self.phase)
    for key,val in self.tags.iteritems():
      if key=='mode':
        prefix+='_'+val
      elif key=='run':
        prefix+='_r'+val
      elif key=='file':
        prefix+='_f'+val
      elif key=='run_group':
        continue
      elif key=='task':
        continue
      elif val.find('/')<0:
        prefix+='_'+key+val
    return prefix

  def getShell(self):

    job=('swif add-job -create -workflow '+self.workflow+' -slurm '
      '-project '+self.project+' -track '+self.track+' '
      '-time '+self.time+' -cores '+str(self.cores)+' '
      '-disk '+self.disk+' -ram '+self.ram+' -shell '+self.shell)

    if not self.phase is None: job += ' -phase '+str(self.phase)

    for key,val in self.tags.iteritems(): job += ' -tag %s %s'   %(key,val)
    for xx in self.inputs:  job += ' -input %s %s' %(xx['local'],xx['remote'])
    for xx in self.outputs: job += ' -output %s %s'%(xx['local'],xx['remote'])

    if self.logDir is not None:
      job += ' -stdout file:'+self.getLogPrefix()+'.out'
      job += ' -stderr file:'+self.getLogPrefix()+'.err'

    job += ' \''+self.getCopyInputsCmd()+' && '+self.cmd+'\''

    return job

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

  def getJson(self):
    jsonData = collections.OrderedDict()
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
    jsonData['command']=self.getCopyInputsCmd()+' && '+self.cmd
    if len(self.inputs)>0:
      jsonData['input']=self.inputs
    if len(self.outputs)>0:
      jsonData['output']=self.outputs
    if self.logDir is not None:
      jsonData['stdout']='file:'+self.getLogPrefix()+'.out'
      jsonData['stderr']='file:'+self.getLogPrefix()+'.err'
    return json.dumps(jsonData)

if __name__ == '__main__':
  job=SwifJob('foobar')
  job.setCmd('ls -l')
  job.addTag('key','val')
  job.addTag('foo','bar')
  job.setLogDir('/tmp/logs')
  job.setPhase(77)
  print job.getShell()
  print job.getJson()

