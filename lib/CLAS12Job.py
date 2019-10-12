from SwifJob import SwifJob

class CLAS12Job(SwifJob):
  def __init__(self,workflow):
    SwifJob.__init__(self,workflow)
    self.env['CCDB_CONNECTION']='mysql://clas12reader@clasdb-farm.jlab.org/clas12'
    self.env['RCDB_CONNECTION']='mysql://rcdb@clasdb-farm.jlab.org/rcdb'

