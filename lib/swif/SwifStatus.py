import os,sys,glob,json,copy,subprocess,getpass,datetime,collections
import FileUtil

SWIF='/site/bin/swif2'

_JSONFORMAT={'indent':2,'separators':(',',': '),'sort_keys':True}

# keeping this just for sorting the darned dictionary for printing, until can
# figure out how to make json.loads preserve sorting without manipulating structure,
# key/value to 2-long tuples, apparently json.loads as of python 3.7 preserves order
# by default, presumably without manipulating structure..
SWIF_JSON_KEYS=[
'workflow_name',
'workflow_user',
'workflow_site',
'workflow_id',
'jobs',
'max_concurrent',
'phase',
'succeeded',
'attempts',
'undispatched',
'abandoned',
'dispatched',
'dispatched_preparing',
'dispatched_running',
'dispatched_pending',
'dispatched_other',
'dispatched_reaping',
'problems',
'problem_types',
'input_mb_processed',
'output_mb_generated',
'update_ts',
'create_ts',
'summary_ts',
'xfer_mb_from_tape',
'workflow_suspended'
]

SWIF_PROBLEMS=[
'SLURM_FAILED',        # the job returned non-zero exit code, swif itself returns 13 in some cases
'SITE_LAUNCH_FAIL',    # problem with the submission, e.g. sbatch failed due to invalid SLURM partition/constraint
'SLURM_NODE_FAIL',     # system problem on the particular node on which the job landed
'SITE_PREP_FAIL',      # e.g. disk request is smaller than inputs
'SWIF_INPUT_FAIL',     # requested input files do not exist
'SWIF-MISSING-OUTPUT', # requested output files do not exist
'SITE_REAP_FAIL',      # this can happen if the output file isn't in the swif tarball
'SLURM_CANCELLED',     # unclear what all can trigger this
'SWIF_SYSTEM_ERROR',   # some system problem
'SLURM_OUT_OF_MEMORY'  # cgroups OOM
]

# These are always system problems, always retry them, regardless the request:
SWIF_PROBLEMS_ALWAYS_RETRY=[
'SLURM_NODE_FAIL',
'SITE_LAUNCH_FAIL',
'SLURM_CANCELLED',
'SWIF_SYSTEM_ERROR'
]

def getWorkflowNames():
  for x in json.loads(subprocess.check_output([SWIF,'list','-display','json']).decode('UTF-8')):
    if x.get('workflow_name') is not None:
      yield x.get('workflow_name')

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
        # if we hook to OrderedDict here, the order is preserved, but key/values get converted to tuple.
        # python3 apparently preserves ordering
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
    # This is the same as running "swif status" without specifying JSON,
    # but we recreate it here just to avoid running swif again ...
    row=[]
    for status in self.getStatus():
      # wanted to do this via pair/hook and OrderedDict in json.loads just
      # to preserve ordering from SWIF, but that converted stuff to tuples,
      # so here we sort manually, grr ....
      #
      # FIXME:  sounds like this syntax changes in python3:
      for k,v in sorted(status.items(), key=lambda (i,j): SWIF_JSON_KEYS.index(i)):
      #for k,v in sorted(status.items(), key=lambda (i): SWIF_JSON_KEYS.index(i[0])):
        if k.endswith('_ts'):
          v = datetime.datetime.fromtimestamp(int(v)/1000).strftime('%Y/%m/%d %H:%M:%S')
        row.append('%-30s = %s'%(k,v))
    return '\n'.join(row)

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
        if job.get('job_status') == 'done':
          ret.add({mode:{'succeeded':1}})
    return ret

##############################################################################
##############################################################################

  def getTagValues(self,tag):
    vals=[]
    if 'jobs' in self.getDetails():
      for job in self.getDetails()['jobs']:
        if 'tags' in job and tag in job['tags']:
          if job['tags'][tag] not in vals:
            vals.append(job['tags'][tag])
    return sorted(vals)

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
      tot = status.get('jobs')
      suc = status.get('succeeded')
      aba = status.get('abandoned')
      if tot is not None and tot>0:
        try:
          if aba is None:
            return tot == suc
          else:
            return tot == suc+aba
        except TypeError:
          return False

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

##############################################################################
##############################################################################

  def getLastAttempt(self,job):
    # FIXME at python3.
    if 'attempts' in job:
      return job['attempts'][-1]
    elif u'attempts' in job:
      return job[u'attempts'][-1]
    return None

  def getPersistentProblems(self,problem='ANY'):
    jobs=[]
    for job in self.getDetails()['jobs']:
      last_attempt = self.getLastAttempt(job)
      if last_attempt is None:
        continue
      if 'job_attempt_problem' in last_attempt:
        if problem=='ANY' or last_attempt['job_attempt_problem']==problem:
          jobs.append(job)
      elif u'job_attempt_problem' in last_attempt:
        if problem=='ANY' or str(last_attempt[u'job_attempt_problem'])==problem:
          jobs.append(job)
    return jobs

  def listExitCodes(self):
    for job in self.getPersistentProblems('SLURM_FAILED'):
      last_attempt = self.getLastAttempt(job)
      if last_attempt is None:
        continue
      exit = last_attempt.get('slurm_exitcode')
      slurmid = last_attempt.get('slurm_id')
      swifid = last_attempt.get('job_id')
      stdout = last_attempt.get('site_job_stdout')
      print(str(exit)+' '+str(slurmid)+' '+str(swifid)+' '+str(stdout))

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
    if logdir is None or logdir is False:
      logdir = '/farm_out/%s/%s'%(getpass.getuser(),self.name)
    for job in self.getPersistentProblems():
      if 'job_name' in job:
        ret.extend(glob.glob('%s/%s*'%(logdir,job['job_name'])))
    return ret

  def tailPersistentProblemLogs(self,logdir=None,nlines=10):
    for x in self.getPersistentProblemLogs(logdir):
      print('##########################################################')
      print('  '+x)
      print('##########################################################')
      print('\n'.join(FileUtil.tail(x,nlines)))

  def getCurrentProblems(self):
    ret=[]
    for status in self.getStatus():
      # hmm, this picked up unicode when switching to SWIF2 apparentely,
      # This makes no sense, must've been some other change, maybe different python2 versions ...
      # This should all go away with python3
      if status.get(u'problem_types') is not None:
        for p in status[u'problem_types'].split(','):
          ret.append(p)
      elif status.get('problem_types') is not None:
        for p in status['problem_types'].split(','):
          ret.append(p)
    return ret

  def tallyAllProblems(self):
    data = collections.OrderedDict()
    if 'jobs' not in self.getDetails():
      return data
    for job in self.getDetails()['jobs']:
      if 'attempts' not in job:
        continue
      for attempt in job['attempts']:
        if 'job_attempt_problem' not in attempt:
          continue
        problem,node,mode=attempt['job_attempt_problem'],'unknown','unknown'
        node = attempt.get('slurm_nodelist')
        if 'tags' in job and 'mode' in job['tags']:
          mode = job['tags']['mode']
        if problem not in data:
          data[problem] = {'count':0,'counts':{'nodes':{},'modes':{}}}
        if node not in data[problem]['counts']['nodes']:
          data[problem]['counts']['nodes'][node] = 0
        if mode not in data[problem]['counts']['modes']:
          data[problem]['counts']['modes'][mode] = 0
        data[problem]['count'] += 1
        data[problem]['counts']['nodes'][node] += 1
        data[problem]['counts']['modes'][mode] += 1
    return data

  def getAllProblems(self):
    ret=set()
    if 'jobs' in self.getDetails():
      for job in self.getDetails()['jobs']:
        if 'attempts' in job:
          for attempt in job['attempts']:
            if 'job_attempt_problem' in attempt:
              ret.add(attempt.get('job_attempt_problem'))
    return ret

  def tallyNodeProblems(self,data):
    ret=''
    problems=self.getAllProblems()
    data2={'nodes':{},'modes':{}}
    nodes,modes={},{}
    for k,v in data:
      for node in v['counts']['nodes']:
        if node not in nodes:
          nodes[node]=dict(zip(problems,[0]*len(problems)))
        nodes[node][k]+=v['counts']['nodes'][node]
      for mode in v['counts']['modes']:
        if mode not in modes:
          modes[mode]=dict(zip(problems,[0]*len(problems)))
        modes[mode][k]+=v['counts']['modes'][mode]
    header_fmt='%12s '+(' '.join(['%20s']*len(problems)))
    header=['Node']
    header.extend(sorted(problems))
    ret+=header_fmt%tuple(header)
    fmt='%12s '+' '.join(['%20d']*len(problems))
    for node in sorted(nodes.keys()):
      if sum(nodes[node].values())==0:
        continue
      x=[node]
      x.extend([nodes[node][k] for k in sorted(problems)])
      ret+='\n'+fmt%tuple(x)
    ret+='\n'+header_fmt%tuple(header)
    ret+='\n\n'
    return ret

  def summarizeProblems(self,pernode=False):
    ret=''
    data=sorted(self.tallyAllProblems().items())
    if pernode:
      ret += self.tallyNodeProblems(data)
    for k,v in data:
      ret+='%20s :'%k
      for mode,count in v['counts']['modes'].items():
        ret+=' %s:%d'%(mode,count)
      ret+='\n'
    ret+='\n'
    for k,v in data:
      ret+='%20s :  %8d\n'%(k,v['count'])
    return ret

  def retryProblems(self,problem_request=[]):
    ret=[]
    problems=self.getCurrentProblems()
    ret.extend(self.modifyJobReqs(problems))
    for problem in problems:
      if problem not in problem_request:
        if problem not in SWIF_PROBLEMS_ALWAYS_RETRY:
          continue
      retryCmd=[SWIF,'retry-jobs','-workflow',self.name,'-problems',problem]
      ret.append(' '.join(retryCmd))
      ret.append(subprocess.check_output(retryCmd))
    return ret

  def abandonProblems(self,types):
    ret=[]
    for problem in self.getCurrentProblems():
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
      ret.append(' '.join(modifyCmd))
      ret.append(subprocess.check_output(modifyCmd))
    if 'AUGER-OVER_RLIMIT' in problems:
      modifyCmd=[SWIF,'modify-jobs','-workflow',self.name]
      modifyCmd.extend(['-ram','add','1gb'])
      modifyCmd.extend(['-problems','AUGER-OVER_RLIMIT'])
      problems.remove('AUGER-OVER_RLIMIT')
      ret.append(' '.join(modifyCmd))
      ret.append(subprocess.check_output(modifyCmd))
    return ret

  def exists(self,path,tape=False):
    ret = os.path.exists(path)
    if tape or not ret:
      if path.startswith('/cache/'):
        ret = os.path.exists('/mss/'+path[7:])
    return ret

# FIXME:  update for SWIF2
#SWIF_JSON_KEYS=[
#'workflow_name',
#'workflow_site',
#'workflow_id',
#'workflow_user'
#'max_concurrent',
#'job_limit',
#'error_limit',
#'phase_limit',
#'phase',
#'jobs',
#'succeeded',
#'attempts',
#'frozen',
#'undispatched',
#'dispatched',
#'dispatched_preparing',
#'dispatched_running',
#'dispatched_pending',
#'dispatched_other',
#'dispatched_reaping',
#'abandoned'
#'failed',
#'canceled',
#'suspended',
#'auger_active',
#'auger_depend',
#'auger_pending',
#'auger_staging_in',
#'auger_finishing',
#'auger_staging_out',
#'problems',
#'problem_types',
#'problem_swif_user_non_zero',
#'problem_swif_system_error',
#'problem_swif_missing_output',
#'problem_auger_canceled',
#'problem_auger_input_fail',
#'problem_auger_output_fail',
#'problem_auger_timeout',
#'problem_auger_unknown',
#'problem_auger_failed',
#'problem_auger_over_rlimit',
#'input_mb_processed',
#'output_mb_generated',
#'update_ts',
#'create_ts',
#'current_ts',
#'summary_ts'
#]

if __name__ == '__main__':
  s=SwifStatus(sys.argv[1])#'test-rec-v0_R5038x6')
#  s.mergeTags()
#  print(s.getPrettyStatus())
#  print(s.getPrettyJsonStatus())
#  s.showPersistentProblems();
  print(('\n'.join(s.getTagValues('run'))))

