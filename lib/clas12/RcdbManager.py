import os,sys,json,logging

_LOGGER=logging.getLogger(__name__)

class RcdbManager():

  _URI='mysql://rcdb@clasdb.jlab.org/rcdb'
  _RCDBKEYS=['solenoid_scale','torus_scale','run_start_time']

  def __init__(self):

    self.data={}
    self.uri=self._URI

    # let environment override database connection:
    if os.getenv('RCDB_CONNECTION') is not None:
      self.uri=os.getenv('RCDB_CONNECTION')

    # try to import RCDB python module:
    try:
      self.rcdb=__import__('rcdb')
    except:
      _LOGGER.warning('Failed to load RCDB python module from $PYTHONPATH.')
      self.rcdb=None

  def get(self,run):

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

    # return it if we already cached this run:
    if int(run) in self.data:
      return self.data[int(run)]

    # connect to database:
    _LOGGER.debug('Opening connection to '+self.uri+' for run '+str(run))
    try:
      db=self.rcdb.RCDBProvider(self.uri)
    except:
      _LOGGER.critical('Failed connecting to '+self.uri)
      sys.exit()

    # read all variables from database for this run:
    try:
      self.data[int(run)]={}
      for key in self._RCDBKEYS:
        self.data[int(run)][key]=None
      for key in self._RCDBKEYS:
        self.data[int(run)][key]=db.get_condition(int(run),key).value
    except:
      _LOGGER.error('Failed to retrieve constants for run '+str(run))

    # close connection:
    db.disconnect()
    _LOGGER.debug('Closed connection to '+self.uri)

    # return the cached data for this run:
    return self.data[int(run)]

  def __str__(self):
    return json.dumps(self.data,default=str,indent=2,separators=(',',': '))

  def getSolenoidScale(self,run):
    return self.get(run).get('solenoid_scale')

  def getTorusScale(self,run):
    return self.get(run).get('torus_scale')

  def getRunStartTime(self,run):
    return self.get(run).get('run_start_time')

if __name__ == '__main__':
  usage = 'python RcdbManager.py run# [run# [run# [...]]]'
  r=RcdbManager()
  if len(sys.argv)<2:
    sys.exit(usage)
  for run in sys.argv[1:]:
    r.getSolenoidScale(run)
#    print(r.getSolenoidScale(run))
  print (r)

