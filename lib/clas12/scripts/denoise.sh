#!/bin/bash

source /etc/profile.d/modules.sh
module use /scigroup/cvmfs/hallb/clas12/sw/modulefiles
module load hipo/4.0.1 denoise/4.0.1
module list

exe=$(which denoise2.exe)
network=$DENOISING_NETWORKS

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

