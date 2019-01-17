import subprocess
import json
import getpass
import datetime

SWIF='/site/bin/swif'

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

class SwifStatus():

  def __init__(self,workflow):
    self.workflow=workflow
    self.status=None
    self.details=None
    self.tagsMerged=False
    self.user=getpass.getuser()

  def loadStatusFromString(self,string):
    self.status=json.loads(string)

  def setUser(self,user):
    self.user=user

  def loadStatus(self):
    cmd=[SWIF,'status','-user',self.user,'-display','json','-workflow',self.workflow]
    self.status=json.loads(subprocess.check_output(cmd))

  def loadDetails(self):
    cmd=[SWIF,'status','-user',self.user,'-jobs','-display','json','-workflow',self.workflow]
    self.details=json.loads(subprocess.check_output(cmd))

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
      for key,val in job['tags'].iteritems():
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
    status=self.status
    for stat in status:
      stat['tags']=tags
    self.status=status
    self.tagsMerged=True

  def getPrettyJsonStatus(self):
    if self.status is None:
      self.loadStatus()
    return json.dumps(self.status,indent=2,separators=(',',': '))

  def getPrettyJsonDetails(self):
    if self.details is None:
      self.loadDetails()
    return json.dumps(self.details,indent=2,separators=(',',': '))

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
      if 'tags' in status:
        for key,val in status['tags'].iteritems():
          if tag==key:
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
      retryCmd=[SWIF,'retry-jobs','-workflow',self.workflow,'-problems',problem]
      ret.append(retryCmd)
      ret.append(subprocess.check_output(retryCmd))
    return ret

  def modifyJobReqs(self,problems):
    ret=[]
    if 'AUGER-TIMEOUT' in problems:
      modifyCmd=[SWIF,'modify-jobs','-workflow',self.workflow]
      modifyCmd.extend(['-time','add','60m'])
      modifyCmd.extend(['-problems','AUGER-TIMEOUT'])
      problems.remove('AUGER-TIMEOUT')
      ret.append(modifyCmd)
      ret.append(subprocess.check_output(modifyCmd))
    if 'AUGER-OVER_RLIMIT' in problems:
      modifyCmd=[SWIF,'modify-jobs','-workflow',self.workflow]
      modifyCmd.extend(['-ram','add','1gb'])
      modifyCmd.extend(['-problems','AUGER-OVER_RLIMIT'])
      problems.remove('AUGER-OVER_RLIMIT')
      ret.append(modifyCmd)
      ret.append(subprocess.check_output(modifyCmd))
    return ret

if __name__ == '__main__':
  s=SwifStatus('decode9_R4146x14_x2500')
  s.setUser('clas12')
  s.mergeTags()
  print s.getPrettyStatus()
  print s.getPrettyJsonStatus()


