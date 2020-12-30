#!/bin/csh

set d=`/usr/bin/readlink -f $0`
set d=`/usr/bin/dirname $d`/..
setenv PYTHONPATH ${d}/workflow

if ( $#argv > 0 ) then
  $d/bin/swif-status.py $argv
else
  $d/bin/swif-status.py --retry
endif

