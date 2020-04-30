import os,sys,json,subprocess,getpass,datetime

SWIF='/site/bin/swif'

# FIXME:  incomplete/incorrect
SWIF_PROBLEMS=[
'SWIF-MISSING-OUTPUT',
'SWIF-USER-ERROR',
'SWIF-SYSTEM-ERROR',
'AUGER-FAILED',
'AUGER-OUTPUT',
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
    line=line.strip()
    if line.find('workflow_name')==0:
      workflows.append(line.split('=')[1].strip())
  return workflows

def deleteWorkflow(name):
  print subprocess.check_output([SWIF,'cancel','-delete','-workflow',name])

class SwifStatus():

  def __init__(self,name):
    self.name=name
    self.status=None
    self.details=None
    self.tagsMerged=False
    self.user=getpass.getuser()

  def loadStatusFromString(self,string):
    self.status=json.loads(string)

  def setUser(self,user):
    self.user=user

  def loadStatus(self):
    cmd=[SWIF,'status','-user',self.user,'-display','json','-workflow',self.name]
    self.status=json.loads(subprocess.check_output(cmd))

  def loadDetails(self):
    cmd=[SWIF,'status','-user',self.user,'-jobs','-display','json','-workflow',self.name]
    self.details=json.loads(subprocess.check_output(cmd))

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
      for key,val in job['tags'].items():
        if key=='file': continue
        idx=-1
        for ii in range(len(tags)):
          if key in tags[ii].keys():
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
        for key,val in stat.items():
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

  def getTagValue(self,tag):
    if not self.tagsMerged:
      self.mergeTags()
    for status in self.status:
      if 'tags' not in status:
        continue
      for atag in status['tags']:
        for key,val in atag.items():
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
        for key,val in atag.items():
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
      if problem not in types:
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
      print(json.dumps(job,indent=2,separators=(',',': '),sort_keys=True))

if __name__ == '__main__':
  s=SwifStatus(sys.argv[1])#'test-rec-v0_R5038x6')
#  s.setUser('clas12-4')
#  s.mergeTags()
#  print(s.getPrettyStatus())
#  print(s.getPrettyJsonStatus())
#  s.showPersistentProblems();
  print('\n'.join(s.getTagValues('run')))

