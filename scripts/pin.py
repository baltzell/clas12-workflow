#!/usr/bin/env python3
import os,sys,glob
sizes = {}
print('<p>/cache Pinning for CLAS12<br>')
print('<p>Paths lists from run group chefs and analysis coordinators are used to calculate disk volume with <a href=\'https://code.jlab.org/hallb/clas12/clas12-workflow/-/blob/main/scripts/pin.py?ref_type=heads\'>this</a>.<p>')
print('<ul>')
for f in glob.glob(os.path.dirname(os.path.realpath(__file__))+'/../pins/*.txt'):
    k = f.split('/').pop().split('.').pop(0)
    sizes[k] = 0
    for path in open(f):
        if path.startswith('/cache'):
            path = '/mss' + path[6:]
        for root, dirs, files in os.walk(path.strip()):
            for file in files:
                for line in open(os.path.join(root, file)):
                    if line.startswith('size'):
                        sizes[k] += int(line.strip().split('=').pop())
                        break
    print('<li>%s:  %.1f TB</li>' % (k.upper(),sizes[k]/1e12))
print('<li>TOTAL: %.1f TB</li>' % (sum(sizes.values())/1e12))
print('</ul>')
#rm -f ./pins/README.html && ./scripts/pin.py >& ./pins/README.html && rsync -avz --delete ./pins clas12@ifarm:/group/clas/www/clasweb/html/clas12offline/disk/cache
