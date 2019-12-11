import os,sys,copy,json,logging

_LOGGER=logging.getLogger(__name__)

class RcdbManager():

  _URI='mysql://rcdb@clasdb.jlab.org/rcdb'
  _IGNORE=['temperature','json_cnd','test']

  def __init__(self):

    self.data={}
    self.uri=self._URI
    self.types=None

    # let environment override database connection:
    #if os.getenv('RCDB_CONNECTION') is not None:
    #  self.uri=os.getenv('RCDB_CONNECTION')

    # try to import RCDB python module:
    try:
      self.rcdb=__import__('rcdb')
    except:
      _LOGGER.warning('Failed to load RCDB python module from $PYTHONPATH.')
      self.rcdb=None

  def load(self,run):

    # exit if we couldn't find RCDB python module:
    if self.rcdb is None:
      _LOGGER.error('Failed to load RCBD python module from $PYTHONPATH.')
      sys.exit()

    # exit if run isn't an integer:
    try:
      int(run)
    except:
      _LOGGER.error('Run number is not an integer:  '+run)
      sys.exit()

    # return if we already cached this run:
    if int(run) in self.data:
      return

    # connect to database:
    _LOGGER.debug('Opening connection to '+self.uri+' for run '+str(run))
    try:
      db=self.rcdb.RCDBProvider(self.uri)
      # load all the condition types:
      if self.types is None:
        self.types=db.get_condition_types()
        while True:
          pruned=False
          for ii in range(len(self.types)):
            if self.types[ii].name in self._IGNORE:
              self.types.pop(ii)
              pruned=True
              break
          if not pruned:
            break
    except:
      _LOGGER.critical('Failed connecting to '+self.uri)
      sys.exit()

    # read all variables from database for this run:
    found=False
    self.data[int(run)]={}
    for t in self.types:
      self.data[int(run)][t.name]=''
      try:
        self.data[int(run)][t.name]=db.get_condition(int(run),t.name).value
        found=True
      except:
        pass
    # if we found no conditions for this run, set it to None:
    if not found:
      self.data[int(run)]=None
      _LOGGER.error('Failed to retrieve constants for run '+str(run))

    # close connection:
    db.disconnect()
    _LOGGER.debug('Closed connection to '+self.uri)

  def __str__(self):
    # prune the null entries:
    data=copy.deepcopy(self.data)
    for run in data.keys():
      if data[run] is None:
        data.pop(run)
    return json.dumps(data,default=str,indent=2,separators=(',',': '))

  def get(self,run,key):
    self.load(run)
    if int(run) in self.data and self.data[int(run)] is not None and key in self.data[int(run)]:
      return self.data[int(run)][key]
    raise ValueError('Cannot find %s for run %d in RCDB'%(key,int(run)))

  def getSolenoidScale(self,run):
    return self.get(run,'solenoid_scale')

  def getTorusScale(self,run):
    return self.get(run,'torus_scale')

  def getRunStartTime(self,run):
    return self.get(run,'run_start_time')

  def _csvHeader(self):
    return 'run,'+','.join([t.name for t in self.types])

  def csv(self):
    csv=[self._csvHeader()]
    for r in sorted(self.data.keys()):
      if self.data[r] is not None:
        csv.append(str(r)+','+','.join([str(self.data[r][t.name]) for t in self.types]))
    return '\n'.join(csv)

if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO,format='%(levelname)-9s[%(name)-15s] %(message)s')
  logger=logging.getLogger(__name__)
  usage = 'python RcdbManager.py run# [run# [run# [...]]]'
  r=RcdbManager()
  if len(sys.argv)<2:
    sys.exit(usage)
  for run in sys.argv[1:]:
    r.getSolenoidScale(run)
  print(r)
  print

