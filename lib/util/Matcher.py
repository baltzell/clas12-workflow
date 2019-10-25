
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

