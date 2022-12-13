#!/bin/bash
#
# quick hack to get denoising validation in the workflow
#

d=/work/clas12/users/devita/denoise
ln -sf $d/network

g=/apps/gcc/9.2.0
export PATH=${g}/bin:${PATH}
export LD_LIBRARY_PATH=${g}/lib64:${g}/lib:${PATH}

for x in `find . -maxdepth 1 -xtype f -name '*.hipo' | sed 's;^\./;;'`
do
  ln -sf dn_${x} output.h5 # reroute the output file
  date +'DENOISE START: %F %H:%M:%S'
  $d/denoise2.exe $x 16 12
  [ $? -ne 0 ] && echo 'DENOISE ERROR' && exit 123
  date +'DENOISE STOP: %F %H:%M:%S'
  mv -f dn_${x} $x # replace the input file
done

echo 0

