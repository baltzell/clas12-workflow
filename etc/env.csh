#!/bin/csh
module load ccdb rcdb coatjava
set sourced=($_)
set d=`/usr/bin/readlink -f $sourced[2]`
set d=`/usr/bin/dirname $d`/..
setenv PYTHONPATH ${d}/lib/clas12:${d}/lib/hps:${d}/lib/swif:${d}/lib/util:${PYTHONPATH}
setenv PATH ${d}/bin:${PATH}

