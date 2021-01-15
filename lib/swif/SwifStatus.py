import os,sys,glob,json,copy,subprocess,getpass,datetime,collections

SWIF='/site/bin/swif2'

# FIXME:  incomplete?
SWIF_PROBLEMS=[
'SWIF-MISSING-OUTPUT',
'SWIF-USER-NON-ZERO',
'SWIF-SYSTEM-ERROR',
'AUGER-FAILED',
'AUGER-OUTPUT',
'AUGER-CANCELLED',
'AUGER-TIMEOUT',
'AUGER-SUBMIT',
]

SWIF_JSON_KEYS=[
'workflow_name',
'workflow_id',
'workflow_user'
'job_limit',
'error_limit',
'phase_limit',
'phase',
'jobs',
'succeeded',
'attempts',
'frozen',
'dispatched',
'undispatched',
'failed',
'canceled',
'suspended',
'auger_active',
'auger_depend',
'auger_pending',
'auger_staging_in',
'auger_finishing',
'auger_staging_out',
'problems',
'problem_types',
'problem_swif_user_non_zero',
'problem_swif_system_error',
'problem_swif_missing_output',
'problem_auger_canceled',
'problem_auger_input_fail',
'problem_auger_output_fail',
'problem_auger_timeout',
'problem_auger_unknown',
'problem_auger_failed',
'problem_auger_over_rlimit',
'update_ts',
'create_ts',
'current_ts',
]

_JSONFORMAT={'indent':2,'separators':(',',': '),'sort_keys':True}

def getWorkflowNames():
  workflows=[]
  for line in subprocess.check_output([SWIF,'list']).splitlines():
    line=line.decode('UTF-8').strip()
    if line.find('workflow_name')==0:
      workflows.append(line.split('=')[1].strip())
  return workflows

def deleteWorkflow(name):
  print(subprocess.check_output([SWIF,'cancel','-delete','-workflow',name]))

class Stats(collections.OrderedDict):
  ZERO={'jobs':0,'succeeded':0}
  def __init__(self):
    collections.OrderedDict.__init__(self)
    self['total']=collections.OrderedDict(Stats.ZERO)
  def add(self,data):
    for k in data.keys():
      if k == 'total':
        continue
      if k not in self:
        self[k]=collections.OrderedDict(Stats.ZERO)
      if 'jobs' in data[k]:
        self[k]['jobs']+=data[k]['jobs']
        self['total']['jobs']+=data[k]['jobs']
      if 'succeeded' in data[k]:
        self[k]['succeeded']+=data[k]['succeeded']
        self['total']['succeeded']+=data[k]['succeeded']
  def __str__(self):
    ret=''
    for k,v in self.items():
      ret += '%10s:  %8d / %8d = %6.4f%%\n'%\
          (k,v['succeeded'],v['jobs'],100.0*v['succeeded']/v['jobs'])
    return ret

class SwifStatus():

  def __init__(self,name):
    self.name=name
    self.__status=None
    self.__details=None
    self.tagsMerged=False
    self.user=getpass.getuser()

  def __str__(self):
    return json.dumps(self.getStatus(),**_JSONFORMAT)

  def getStatus(self,source=None):
    if self.__status is None:
      if source is None:
        cmd=[SWIF,'status','-user',self.user,'-display','json','-workflow',self.name]
        self.__status=json.loads(subprocess.check_output(cmd).decode('UTF-8'))
      elif isinstance(source,list) or isinstance(source,dict):
        self.__status=source
      elif os.path.isfile(source):
        self.__status=json.load(open(source,'r'))
      elif isinstance(source,str):
        self.__status=json.loads(source)
      else:
        raise TypeError('Cannot set SwifStatus.status')
    return self.__status

  def getDetails(self,source=None):
    self.getStatus(source)
    if self.__details is None:
      if source is None:
        cmd=[SWIF,'status','-user',self.user,'-jobs','-display','json','-workflow',self.name]
        self.__details=json.loads(subprocess.check_output(cmd).decode('UTF-8'))
      elif isinstance(source,list) or isinstance(source,dict):
        self.__details=source
      elif os.path.isfile(source):
        self.__details=json.load(open(source,'r'))
      elif isinstance(source,str):
        self.__details=json.loads(source)
      else:
        raise TypeError('Cannot set SwifStatus.details')
    return self.__details

  def getValue(self,key):
    for s in self.getStatus():
      if key in s:
        return s[key]
    return None

  def getTagValues(self,tag):
    vals=[]
    if 'jobs' in self.getDetails():
      for job in self.getDetails()['jobs']:
        if 'tags' in job and tag in job['tags']:
          if job['tags'][tag] not in vals:
            vals.append(job['tags'][tag])
    return sorted(vals)

  # pull user-defined swif tags from all jobs into the global status
  def mergeTags(self):
    if 'jobs' not in self.getDetails():
      return
    tags=[]
    for job in self.getDetails()['jobs']:
      if 'tags' not in job:
        continue
      for key,val in list(job['tags'].items()):
        if key=='file': continue
        idx=-1
        for ii in range(len(tags)):
          if key in list(tags[ii].keys()):
            idx=ii
            break
        if idx<0:
          idx=len(tags)
          tags.append({key:[]})
        if val not in tags[idx][key]:
          tags[idx][key].append(val)
    for stat in self.getStatus():
      stat['tags']=tags
    self.tagsMerged=True

  # return a copy of the status with all null entries removed:
  def getPrunedStatus(self):
    status = list(self.getStatus())
    for stat in status:
      modified = True
      while modified:
        modified = False
        for key,val in list(stat.items()):
          if val==None:
            del stat[key]
            modified = True
            break
    return status

  def getPrettyJsonStatus(self):
    return json.dumps(self.getPrunedStatus(),**_JSONFORMAT)

  def getPrettyJsonDetails(self):
    return json.dumps(self.getDetails(),**_JSONFORMAT)

  def getPrettyStatus(self):
    statuses=[]
    for status in self.getStatus():
      for key in SWIF_JSON_KEYS:
        if key not in status:
          continue
        if status[key] is None:
          continue
        if key.find('_ts')==len(key)-3:
          dt=datetime.datetime.fromtimestamp(int(status[key])/1000)
          statuses.append('%-30s = %s'%(key,dt.strftime('%Y/%m/%d %H:%M:%S')))
        else:
          statuses.append('%-30s = %s'%(key,status[key]))
    return '\n'.join(statuses)

  def getProblems(self):
    problems=[]
    for status in self.getStatus():
      if 'problem_types' not in status:
        continue
      if status['problem_types'] is None:
        continue
      problems.extend(status['problem_types'].split(','))
    return problems

  def tallyAllProblems(self):
    data=collections.OrderedDict()
    if 'jobs' in self.getDetails():
      for job in self.getDetails()['jobs']:
        if 'attempts' in job:
          for attempt in job['attempts']:
            if 'problem' in attempt:
              problem,node,mode=attempt['problem'],'unknown','unknown'
              if 'auger_node' in attempt:
                node=attempt['auger_node']
              if 'tags' in job and 'mode' in job['tags']:
                mode=job['tags']['mode']
              if problem not in data:
                data[problem]={'count':0,'counts':{'nodes':{},'modes':{}}}
              if node not in data[problem]['counts']['nodes']:
                data[problem]['counts']['nodes'][node]=0
              if mode not in data[problem]['counts']['modes']:
                data[problem]['counts']['modes'][mode]=0
              data[problem]['count']+=1
              data[problem]['counts']['nodes'][node]+=1
              data[problem]['counts']['modes'][mode]+=1
    return data

  def summarizeProblems(self,pernode=False):
    ret=''
    data=sorted(self.tallyAllProblems().items())
    # YUK! FIXME
    if pernode:
      data2={'nodes':{},'modes':{}}
      nodes,modes={},{}
      for k,v in data:
        for node in v['counts']['nodes']:
          if node not in nodes:
            nodes[node]=dict(zip(SWIF_PROBLEMS,[0]*len(SWIF_PROBLEMS)))
          nodes[node][k]+=v['counts']['nodes'][node]
        for mode in v['counts']['modes']:
          if mode not in modes:
            modes[mode]=dict(zip(SWIF_PROBLEMS,[0]*len(SWIF_PROBLEMS)))
          modes[mode][k]+=v['counts']['modes'][mode]
      fmt='%12s '+(' '.join(['%20s']*len(SWIF_PROBLEMS)))
      x=['Node']
      x.extend(sorted(SWIF_PROBLEMS))
      ret+=fmt%tuple(x)
      fmt='%12s '+' '.join(['%20d']*len(SWIF_PROBLEMS))
      for node in sorted(nodes.keys()):
        if sum(nodes[node].values())==0:
          continue
        x=[node]
        x.extend([nodes[node][k] for k in sorted(SWIF_PROBLEMS)])
        ret+='\n'+fmt%tuple(x)
      ret+='\n\n'
    for k,v in data:
      ret+='%20s :'%k
      for mode,count in v['counts']['modes'].items():
        ret+=' %s:%d'%(mode,count)
      ret+='\n'
    ret+='\n'
    for k,v in data:
      ret+='%20s :  %8d\n'%(k,v['count'])
    return ret

  def getTagValue(self,tag):
    if not self.tagsMerged:
      self.mergeTags()
    for status in self.getStatus():
      if 'tags' not in status:
        continue
      for atag in status['tags']:
        for key,val in list(atag.items()):
          if key==tag:
            if type(val) is list:
              return val[0]
            else:
              return val
    return None

  def removeTag(self,tag):
    if not self.tagsMerged:
      self.mergeTags()
    for status in self.getStatus():
      if 'tags' not in status:
        continue
      for atag in status['tags']:
        for key,val in list(atag.items()):
          if key==tag:
            status['tags'].remove(atag)
            if type(val) is list:
              return val[0]
            else:
              return val
    return None

  def isComplete(self):
    for status in self.getStatus():
      if 'jobs' in status and 'succeeded' in status:
        return status['jobs']>0 and status['jobs']==status['succeeded']
    return False

  def retryProblems(self):
    ret=[]
    problems=self.getProblems()
    ret.extend(self.modifyJobReqs(problems))
    for problem in problems:
      retryCmd=[SWIF,'retry-jobs','-workflow',self.name,'-problems',problem]
      ret.append(retryCmd)
      ret.append(subprocess.check_output(retryCmd))
    return ret

  def abandonProblems(self,types):
    ret=[]
    for problem in self.getProblems():
      if problem not in types and 'ANY' not in types:
        continue
      retryCmd=[SWIF,'abandon-jobs','-workflow',self.name,'-problems',problem]
      ret.append(retryCmd)
      ret.append(subprocess.check_output(retryCmd))
    return ret

  def modifyJobReqs(self,problems):
    ret=[]
    if 'AUGER-TIMEOUT' in problems:
      modifyCmd=[SWIF,'modify-jobs','-workflow',self.name]
      modifyCmd.extend(['-time','add','300m'])
      modifyCmd.extend(['-problems','AUGER-TIMEOUT'])
      problems.remove('AUGER-TIMEOUT')
      ret.append(modifyCmd)
      ret.append(subprocess.check_output(modifyCmd))
    if 'AUGER-OVER_RLIMIT' in problems:
      modifyCmd=[SWIF,'modify-jobs','-workflow',self.name]
      modifyCmd.extend(['-ram','add','1gb'])
      modifyCmd.extend(['-problems','AUGER-OVER_RLIMIT'])
      problems.remove('AUGER-OVER_RLIMIT')
      ret.append(modifyCmd)
      ret.append(subprocess.check_output(modifyCmd))
    return ret

  def exists(self,path,tape=False):
    ret = os.path.exists(path)
    if tape or not ret:
      if path.startswith('/cache/'):
        ret = os.path.exists('/mss/'+path[7:])
    return ret

  def findMissingOutputs(self,tape=False):
    ret=[]
    if 'jobs' in self.getDetails():
      for job in self.getDetails()['jobs']:
        if 'attempts' in job:
          for att in job['attempts']:
            if 'exitcode' in att and att['exitcode']==0:
              if 'outputs' in job:
                for out in job['outputs']:
                  if 'remote' in out:
                    if not self.exists(out['remote'],tape):
                      ret.append(out['remote'])
    return ret

  def getOutputDirs(self):
    ret=[]
    if 'jobs' in self.getDetails():
      for job in self.getDetails()['jobs']:
        if 'outputs' in job:
          for out in job['outputs']:
            if 'remote' in out:
              x = os.path.dirname(out['remote'])
              try:
                # ignore last directory if it's just a (run) number:
                y = int(x.split('/').pop())
                x = os.path.dirname(x)
              except:
                pass
              ret.append(x)
    return set(ret)

  def getJobNamesByTag(self,tags):
    ret={}
    if 'jobs' in self.getDetails():
      for job in self.getDetails()['jobs']:
        if 'name' not in job or 'tags' not in job:
          continue
        for tag in tags:
          if tag in job['tags']:
            if tag not in ret:
              ret[tag]={}
            if job['tags'][tag] not in ret[tag]:
              ret[tag][job['tags'][tag]]=[]
            ret[tag][job['tags'][tag]].append(job['name'])
    return ret

  def getJobNamesByRun(self,runs):
    ret=[]
    data=self.getJobNamesByTag(['run'])
    if 'run' in data:
      for r,f in data['run'].items():
        if int(r) in runs:
          ret.extend(f)
    return ret

  def abandonJobsByRun(self,runs):
    ret=''
    files=self.getJobNamesByRun(runs)
    if len(files)>0:
      cmd=[SWIF,'abandon-jobs','-workflow',self.name,'-names']
      cmd.extend(files)
      ret+=' '.join(cmd)
      ret+='\n'+subprocess.check_output(cmd)
    else:
      ret+='No files found for run numbers:  '+','.join(runs)
    return ret

  def getSummaryStats(self,tag):
    ret = Stats()
    details = copy.deepcopy(self.getDetails())
    if 'jobs' in details:
      for job in details['jobs']:
        mode = 'unknown'
        if 'tags' in job:
          if 'phase' in job:
            job['tags']['phase'] = job['phase']
          if tag in job['tags']:
            mode = job['tags'][tag]
        ret.add({mode:{'jobs':1}})
        if 'status' in job and job['status']=='succeeded':
          ret.add({mode:{'succeeded':1}})
    return ret

  def getPersistentProblems(self,problem='ANY'):
    jobs=[]
    for job in self.getDetails()['jobs']:
      if 'attempts' in job:
        # only look at the last attempt:
        if 'problem' in job['attempts'][-1]:
          if problem=='ANY' or job['attempts'][-1]['problem']==problem:
            jobs.append(job)
    return jobs

  def getPersistentProblemInputs(self,problem='ANY'):
    ret=[]
    for job in self.getPersistentProblems(problem):
      if 'inputs' in job:
        for x in job['inputs']:
          ignore=False
          for suff in ['.sh','.yaml','.json','.sqlite']:
            if x['local'].endswith(suff):
              ignore=True
              break
          if not ignore:
            ret.append(x['remote'])
    return ret

  def getPersistentProblemJobs(self,problem='ANY'):
    return json.dumps(self.getPersistentProblems(problem),**_JSONFORMAT)

  def getPersistentProblemLogs(self,logdir=None):
    ret = []
    if logdir is None:
      logdir = '/farm_out/%s/%s'%(getpass.getuser(),self.name)
    for job in self.getPersistentProblems():
      if 'name' in job:
        ret.extend(glob.glob('%s/%s*'%(logdir,job['name'])))
    return ret


if __name__ == '__main__':
  s=SwifStatus(sys.argv[1])#'test-rec-v0_R5038x6')
#  s.mergeTags()
#  print(s.getPrettyStatus())
#  print(s.getPrettyJsonStatus())
#  s.showPersistentProblems();
  print(('\n'.join(s.getTagValues('run'))))

