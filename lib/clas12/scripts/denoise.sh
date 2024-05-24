#!/bin/bash

if grep -q -i Alma /etc/redhat-release
then
    source /scigroup/cvmfs/hallb/clas12/soft/setup.sh
    module load denoising/4.0.1
    exe=$(which denoise2.exe)
    network=$DENOISING_NETWORKS
else
    g=/apps/gcc/10.2.0
    export PATH=${g}/bin:${PATH}
    export LD_LIBRARY_PATH=${g}/lib64:${g}/lib:${PATH}
    d=/group/clas12/packages/hipo/2.0/extensions/dc
    exe=$d/denoise2.exe
    network=$d/network
fi

# denoising requires $PWD/network
ln -sf $network

for x in `find . -maxdepth 1 -xtype f -name '*.hipo' | sed 's;^\./;;'`
do
  date +'DENOISE START: %F %H:%M:%S'
  $exe -i $x -o dn_${x} -l 0.01 -t 16 
  [ $? -ne 0 ] && echo 'DENOISE ERROR' && exit 100
  date +'DENOISE STOP: %F %H:%M:%S'
  mv -f dn_${x} $x # replace the input file
done

exit 0

