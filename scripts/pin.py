#!/usr/bin/env python3
import os,sys,glob
sizes = {}
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
    print('%s:  %.1f TB' % (k.upper(),sizes[k]/1e12))
print('TOTAL: %.1f TB' % (sum(sizes.values())/1e12))
