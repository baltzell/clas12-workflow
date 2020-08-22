import os,sys,json,subprocess,getpass,datetime,collections

SWIF='/site/bin/swif'

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

SWIFJSONKEYS=[
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

def getWorkflowNames():
  workflows=[]
  for line in subprocess.check_output([SWIF,'list']).splitlines():
    line=line.decode('UTF-8').strip()
    if line.find('workflow_name')==0:
      workflows.append(line.split('=')[1].strip())
  return workflows

def deleteWorkflow(name):
  print(subprocess.check_output([SWIF,'cancel','-delete','-workflow',name]))

class SwifStatus():

  def __init__(self,name,filename=None):
    self.name=name
    self.status=None
    self.details=None
    self.tagsMerged=False
    self.user=getpass.getuser()
    #if filename is not None:
    #  if os.path.isfile(filename):
    #    self.details = json.load(open(filename,'r'))
    #  else:
    #    print('Error reading file:  '+filename)

  def loadStatusFromString(self,string):
    self.status=json.loads(string)

  def setUser(self,user):
    self.user=user

  def loadStatus(self):
    cmd=[SWIF,'status','-user',self.user,'-display','json','-workflow',self.name]
    self.status=json.loads(subprocess.check_output(cmd).decode('UTF-8'))

  def loadDetails(self):
    cmd=[SWIF,'status','-user',self.user,'-jobs','-display','json','-workflow',self.name]
    self.details=json.loads(subprocess.check_output(cmd).decode('UTF-8'))

  def getTagValues(self,tag):
    vals=[]
    if self.details is None:
      self.loadDetails()
    if 'jobs' in self.details:
      for job in self.details['jobs']:
        if 'tags' in job and tag in job['tags']:
          if job['tags'][tag] not in vals:
            vals.append(job['tags'][tag])
    return sorted(vals)

  # pull user-defined swif tags from all jobs into the global status
  def mergeTags(self):
    if self.status is None:
      self.loadStatus()
    if self.details is None:
      self.loadDetails()
    if 'jobs' not in self.details:
      return
    tags=[]
    for job in self.details['jobs']:
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
    for stat in self.status:
      stat['tags']=tags
    self.tagsMerged=True

  # return a copy of the status with all null entries removed:
  def getPrunedStatus(self):
    if self.status is None:
      self.loadStatus()
    status = list(self.status)
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

  def getPrunedJsonStatus(self):
    if self.status is None:
      self.loadStatus()
    return json.dumps(self.getPrunedStatus())

  def getPrettyJsonStatus(self):
    if self.status is None:
      self.loadStatus()
    return json.dumps(self.getPrunedStatus(),indent=2,separators=(',',': '),sort_keys=True)

  def getPrettyJsonDetails(self):
    if self.details is None:
      self.loadDetails()
    return json.dumps(self.details,indent=2,separators=(',',': '),sort_keys=True)

  def getPrettyStatus(self):
    if self.status is None:
      self.loadStatus()
    statuses=[]
    for status in self.status:
      for key in SWIFJSONKEYS:
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
    if self.status is None:
      self.loadStatus()
    problems=[]
    for status in self.status:
      if 'problem_types' not in status:
        continue
      if status['problem_types'] is None:
        continue
      problems.extend(status['problem_types'].split(','))
    return problems

  def tallyAllProblems(self):
    data=collections.OrderedDict()
    if self.details is None:
      self.loadDetails()
    if 'jobs' in self.details:
      for job in self.details['jobs']:
        if 'attempts' in job:
          for attempt in job['attempts']:
            if 'problem' in attempt:
              node='unknown'
              if 'auger_node' in attempt:
                node=attempt['auger_node']
              if attempt['problem'] not in data:
                data[attempt['problem']]={'count':0,'counts':{}}
              if node not in data[attempt['problem']]['counts']:
                data[attempt['problem']]['counts'][node]=0
              data[attempt['problem']]['count']+=1
              data[attempt['problem']]['counts'][node]+=1
    return data

  def summarizeProblems(self,pernode=False):
    if self.details is None:
      self.loadDetails()
    ret=''
    data=sorted(self.tallyAllProblems().items())
    # yuk, FIXME
    if pernode:
      nodes={}
      for k,v in data:
        for node in v['counts']:
          if node not in nodes:
            nodes[node]=dict(zip(SWIF_PROBLEMS,[0]*len(SWIF_PROBLEMS)))
          nodes[node][k]+=v['counts'][node]
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
      ret+='%20s :  %8d\n'%(k,v['count'])
    return ret

  def getTagValue(self,tag):
    if not self.tagsMerged:
      self.mergeTags()
    for status in self.status:
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
    for status in self.status:
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
    if self.status is None:
      self.loadStatus()
    for status in self.status:
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
      modifyCmd.extend(['-time','add','60m'])
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

  def findMissingOutputs(self):
    ret=[]
    if self.details is None:
      self.loadDetails()
    if 'jobs' in self.details:
      for job in self.details['jobs']:
        if 'attempts' in job:
          for att in job['attempts']:
            if 'exitcode' in att and att['exitcode']==0:
              if 'outputs' in job:
                for out in job['outputs']:
                  if 'remote' in out:
                    if not os.path.exists(out['remote']):
                      ret.append(out['remote'])
    return ret

  def getJobNamesByTag(self,tags):
    ret={}
    if self.details is None:
      self.loadDetails()
    if 'jobs' in self.details:
      for job in self.details['jobs']:
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

  def getSummaryData(self,tag):
    data=collections.OrderedDict()
    data['total']={'jobs':0,'success':0}
    if self.details is None:
      self.loadDetails()
    if 'jobs' in self.details:
      for job in self.details['jobs']:
        mode = 'unknown'
        if 'tags' in job:
          if tag in job['tags']:
            mode = job['tags'][tag]
        if mode not in data:
          data[mode] = {'jobs':0,'success':0}
        data[mode]['jobs'] += 1
        data['total']['jobs'] += 1
        if 'status' in job:
          if job['status']=='succeeded':
            data[mode]['success'] += 1
            data['total']['success'] += 1
    return data

  def summarize(self,tag):
    ret=''
    for k,v in self.getSummaryData(tag).items():
      ret+='%10s:  %8d / %8d = %6.4f%%\n'%(k,v['success'],v['jobs'],float(v['success'])/v['jobs']*100)
    return ret

  def getPersistentProblems(self):
    problemJobs=[]
    if self.details is None:
      self.loadDetails()
    for job in self.details['jobs']:
      job = json.loads(json.dumps(job))
      if 'attempts' not in job:
        continue
      nproblems = 0
      # go in reverse order on attempts, since we're
      # looking for jobs that still have problems
      for attempt in job['attempts'][::-1]:
        if 'problem' in attempt:
          nproblems += 1
        else:
          break
      if nproblems > 0:
        problemJobs.append(job)
    return problemJobs

  def showPersistentProblems(self):
    for job in self.getPersistentProblems():
      print((json.dumps(job,indent=2,separators=(',',': '),sort_keys=True)))

if __name__ == '__main__':
  s=SwifStatus(sys.argv[1])#'test-rec-v0_R5038x6')
#  s.setUser('clas12-4')
#  s.mergeTags()
#  print(s.getPrettyStatus())
#  print(s.getPrettyJsonStatus())
#  s.showPersistentProblems();
  print(('\n'.join(s.getTagValues('run'))))

