import sys,traceback

_RCDBKEYS=['solenoid_scale','torus_scale','run_start_time']

class RcdbEntry():

  def __init__(self,run):
    self.run = run
    self.data={}

  def _add(self,key,val):
    self.data[key] = val

  def get(self,key):
    if key in self.data:
      return self.data[key]
    return None

  def __str__(self):
    comma=''
    ret = str(self.run)+' = {'
    for key,val in self.data.iteritems():
      ret += comma+' '+key+':'+str(val)
      comma=' ,'
    ret += ' } '
    return ret

class RcdbManager():

  _uri='mysql://rcdb@clasdb.jlab.org/rcdb'

  def __init__(self):
    self.rcdb=False
    self.data={}
    try:
      import rcdb
      self.rcdb=True
    except:
      print 'WARNING:  Failure to load RCDB python module from PYTHONPATH.'
      self.rcdb=False
  def _loadRun(self,run):
    entry = RcdbEntry(run)
    import rcdb
    try:
      db=rcdb.RCDBProvider(self._uri)
    except:
      print traceback.format_exc()
      sys.exit('***\n*** ERROR:  Could not connect to '+self._uri+'\n***')
    try:
      for key in _RCDBKEYS:
        entry._add(key,db.get_condition(run,key).value)
    except:
      print traceback.format_exc()
      db.disconnect()
      sys.exit('***\n*** ERROR:  Could not find RCDB constants for run '+str(run)+'\n***')
    db.disconnect()
    self.data[run] = entry

  def getEntry(self,run):
    assert(self.rcdb),'***\n*** ERROR:  Trying to use nonexistent RCBD module.\n***'
    if run not in self.data:
      self._loadRun(run)
    return self.data[run]

  def getSolenoidScale(self,run):
    return self.getEntry(run).get('solenoid_scale')

  def getTorusScale(self,run):
    return self.getEntry(run).get('torus_scale')

  def getRunStartTime(self,run):
    return self.getEntry(run).get('run_start_time')

if __name__ == '__main__':
  usage = 'python RcdbManager.py run# [run# [run# [...]]]'
  r=RcdbManager()
  if len(sys.argv)<2:
    sys.exit(usage)
  for run in sys.argv[1:]:
    try:
      run = int(run)
    except:
      sys.exit(usage+'\nRun must be an integer: '+run)
    print r.getEntry(run)

