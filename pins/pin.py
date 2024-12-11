#!/usr/bin/env python3
import os,sys,glob

def get_sizes(f):
    for path in open(f):
        if path.startswith('/cache'):
            path = '/mss' + path[6:]
        for root, dirs, files in os.walk(path.strip()):
            for file in files:
                for line in open(os.path.join(root, file)):
                    if line.startswith('size'):
                        yield int(line.strip().split('=').pop())
                        break

top = os.path.dirname(os.path.realpath(__file__))

with open(top+'/README.html','w') as o:
    def write(line):
        o.write(line)
        print(line, flush=True)
    write('<p>/cache Pinning for CLAS12<br>')
    write(f'<p>Paths lists from run group chefs and analysis coordinators are used to calculate disk volume with <a href=\'https://code.jlab.org/hallb/clas12/clas12-workflow/-/blob/main/pins/pin.py\'>this</a>.<p>')
    write('<ul>')
    sizes = {}
    for f in glob.glob(top+'/*.txt'):
        k = f.split('/').pop().split('.').pop(0)
        sizes[k] = sum(list(get_sizes(f)))
        write('<li>%s:  %.1f TB</li>' % (k.upper(),sizes[k]/1e12))
    write('<li>TOTAL: %.1f TB</li>' % (sum(sizes.values())/1e12))
    write('</ul>')

print(f'rsync -avz --delete {top} clas12@ifarm:/group/clas/www/clasweb/html/clas12offline/disk/cache')
print('https://clasweb.jlab.org/clas12offline/disk/cache/pin')
