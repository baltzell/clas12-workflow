#!/bin/csh

set d=`/usr/bin/readlink -f $0`
set d=`/usr/bin/dirname $d`/..
setenv PYTHONPATH ${d}/lib/swif:${d}/lib/util:${d}/lib/clas12:${d}/lib/ccdb

if ( $#argv > 0 ) then
  $d/scripts/swif-status.py $argv
else
  $d/scripts/swif-status.py --retry
endif

