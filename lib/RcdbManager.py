import rcdb,sys,traceback

class RcdbEntry():

  def __init__(self,run):
    self.run = run
    self.data={}

  def add(self,key,val):
    self.data[key] = val

  def get(self,key):
    if key in self.data:
      return self.data[key]
    return None

  def __str__(self):
    ret = str(run)
    for key,val in self.data.iteritems():
      ret += ' '+key+'='+str(val)
    return ret

class RcdbManager():

  _uri='mysql://rcdb@clasdb.jlab.org/rcdb'

  def __init__(self):
    self.data={}

  def _loadRun(self,run):
    entry = RcdbEntry(run)
    try:
      db=rcdb.RCDBProvider(self._uri)
    except:
      print traceback.format_exc()
      sys.exit('***\n*** ERROR:  Could not connect to '+self._uri+'\n***')
    try:
      entry.add('solenoid_scale',db.get_condition(run,'solenoid_scale').value)
      entry.add('torus_scale'   ,db.get_condition(run,'torus_scale').value)
      entry.add('run_start_time',db.get_condition(run,'run_start_time').value)
    except:
      print traceback.format_exc()
      db.disconnect()
      sys.exit('***\n*** ERROR:  Could not retrieve RCDB constants for run '+str(run)+'\n***')
    db.disconnect()
    self.data[run] = entry

  def getEntry(self,run):
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

