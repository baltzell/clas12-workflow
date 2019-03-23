import rcdb,sys,traceback

class RcdbManager():

  _uri='mysql://rcdb@clasdb.jlab.org/rcdb'

  def __init__(self):
    self.data={}

  def _loadRun(self,run):
    data={}
    try:
      db=rcdb.RCDBProvider(self._uri)
    except:
      print traceback.format_exc()
      sys.exit('*** Error Connecting to '+self._uri)
    try:
      data['solenoid_scale']=db.get_condition(run,'solenoid_scale').value
      data['torus_scale']   =db.get_condition(run,'torus_scale').value
      data['run_start_time']=db.get_condition(run,'run_start_time').value
    except:
      print traceback.format_exc()
      db.disconnect()
      sys.exit('*** Error retrieving RCDB constants for run '+str(run))
    db.disconnect()
    self.data[run]=data

  def getSolenoidScale(self,run):
    if run not in self.data:
      self._loadRun(run)
    return self.data[run]['solenoid_scale']

  def getTorusScale(self,run):
    if run not in self.data:
      self._loadRun(run)
    return self.data[run]['torus_scale']

  def getRunStartTime(self,run):
    if run not in self.data:
      self._loadRun(run)
    return self.data[run]['run_start_time']


if __name__ == '__main__':
  r=RcdbManager()
  print '4013 solenoid:  '+str(r.getSolenoidScale(4013))
  print '4014 torus:     '+str(r.getTorusScale(4014))
  print '4015 time:      '+str(r.getRunStartTime(4015))

