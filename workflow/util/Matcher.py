
def matchAll(string,tags):
  for x in tags:
    if string.find(x)<0:
      return False
  return True

def matchAny(string,tags):
  if tags is None or len(tags)==0:
    return True
  for x in tags:
    if string.find(x)>=0:
      return True
  return False

class Matcher():
  def __init__(self):
    self.all=[]
    self.any=[]
  def addAll(self,tag):
    self.all.append(tag)
  def addAny(self,tag):
    self.any.append(tag)
  def matches(string):
    return matchAll(string,self.all) and matchAny(string,self.any)

