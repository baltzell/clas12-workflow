#!/bin/csh

if ( ! $?RCDB_HOME ) then
    setenv RCDB_HOME /group/clas12/rcdb
endif
if (! $?LD_LIBRARY_PATH) then
    setenv LD_LIBRARY_PATH $RCDB_HOME/cpp/lib
else
    setenv LD_LIBRARY_PATH "$RCDB_HOME/cpp/lib":$LD_LIBRARY_PATH
endif

if ( ! $?PYTHONPATH ) then
    setenv PYTHONPATH "$RCDB_HOME/python"
else
    setenv PYTHONPATH "$RCDB_HOME/python":$PYTHONPATH
endif
setenv PATH "$RCDB_HOME":"$RCDB_HOME/bin":"$RCDB_HOME/cpp/bin":$PATH
#Changes python version to 2.7 in order to keep python scripts working. May affect user environment.
setenv PATH /apps/python/2.7.12/bin:$PATH

set d=`/usr/bin/readlink -f $0`
set d=`/usr/bin/dirname $d`/..

setenv PYTHONPATH ${PYTHONPATH}:${d}/lib/swif:${d}/lib/util:${d}/lib/clas12

$d/scripts/swif-status.py --retry

