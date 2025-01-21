#!/usr/bin/env python3

def get_sizes(f):
    for path in open(f):
        if path.startswith('/cache'):
            path = '/mss' + path[6:]
        import os
        for root, dirs, files in os.walk(path.strip()):
            for file in files:
                for line in open(os.path.join(root, file)):
                    if line.startswith('size'):
                        yield int(line.strip().split('=').pop())
                        break

def crawl(d):
    with open(d+'/README.html','w') as o:
        def write(line):
            o.write(line)
            print(line, flush=True)
        write('<p>/cache Pinning for CLAS12<br>')
        write(f'<p>Paths lists from run group chefs and analysis coordinators are used to calculate disk volume with <a href=\'https://code.jlab.org/hallb/clas12/clas12-workflow/-/blob/main/pins/pin.py\'>this</a>.<p>')
        write('<ul>')
        sizes = {}
        import glob
        for f in sorted(glob.glob(d+'/*.txt')):
            k = f.split('/').pop().split('.').pop(0)
            sizes[k] = sum(list(get_sizes(f)))
            write('<li>%s:  %.1f TB</li>' % (k.upper(),sizes[k]/1e12))
        write('<li>TOTAL: %.1f TB</li>' % (sum(sizes.values())/1e12))
        write('</ul>')
    print(f'rsync -avz --delete {d} clas12@ifarm:/group/clas/www/clasweb/html/clas12offline/disk/cache')
    print('https://clasweb.jlab.org/clas12offline/disk/cache/pin')

def pin(d):
    import os
    if os.path.isdir(d):
        import glob
        for f in sorted(glob.glob(d+'/*.txt')):
            pin(f)
    elif os.path.isfile(d):
        for path in open(d):
            if path.startswith('/cache'):
                path = '/mss' + path[6:]
            import os
            for root, dirs, files in os.walk(path.strip()):
                if len(files) > 0:
                    print('jcache get -D 60 '+root+'/*.hipo')

if __name__ == '__main__':
    import os,argparse
    cli = argparse.ArgumentParser()
    cli.add_argument('-c',help='calculate data volume',action='store_true')
    cli.add_argument('-p',help='issue pin requests',action='store_true')
    cli.add_argument('-i',help='input directory or filename',default=os.path.dirname(os.path.realpath(__file__)))
    args = cli.parse_args()
    if args.c:
        crawl(args.i)
    if args.p:
        pin(args.i)

