#!/bin/bash
#
# quick hack to get denoising validation in the workflow
#

[ -z $1 ] && v=1.9 || v=$1

d=/group/clas12/packages/hipo/$v/extensions/dc

# requires this subdir in $PWD:
ln -sf $d/network

g=/apps/gcc/10.2.0
export PATH=${g}/bin:${PATH}
export LD_LIBRARY_PATH=${g}/lib64:${g}/lib:${PATH}

for x in `find . -maxdepth 1 -xtype f -name '*.hipo' | sed 's;^\./;;'`
do
  date +'DENOISE START: %F %H:%M:%S'
  $d/denoise2.exe -i $x -o dn_${x} -l 0.01 -t 16 
  [ $? -ne 0 ] && echo 'DENOISE ERROR' && exit 100
  date +'DENOISE STOP: %F %H:%M:%S'
  mv -f dn_${x} $x # replace the input file
done

exit 0

