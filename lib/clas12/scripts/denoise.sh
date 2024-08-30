#!/bin/bash

# environment should be set externally, e.g.:
# module load denoise/4.0.1

exe=$(which denoise2.exe)

# denoising requires $PWD/network:
ln -sf $DENOISING_NETWORKS

for x in `find . -maxdepth 1 -xtype f -name '*.hipo' | sed 's;^\./;;'`
do
  date +'DENOISE START: %F %H:%M:%S'
  $exe -i $x -o dn_${x} -l 0.01 -t 16 
  [ $? -ne 0 ] && echo 'DENOISE ERROR' && exit 100
  date +'DENOISE STOP: %F %H:%M:%S'
  mv -f dn_${x} $x # replace the input file
done

exit 0

