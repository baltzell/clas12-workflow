
def matchAll(string,tags):
  for x in tags:
    if string.find(x)<0:
      return False
  return True

def matchAny(string,tags):
  for x in tags:
    if string.find(x)>=0:
      return True
  return False

