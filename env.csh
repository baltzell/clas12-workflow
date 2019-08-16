source /group/clas12/packages/setup.csh
module load rcdb

#Changes python version to 2.7 in order to keep python scripts working. May affect user environment.
setenv PATH /apps/python/2.7.12/bin:$PATH

set sourced=($_)
set curdir=`/usr/bin/readlink -f $sourced[2]`
set curdir=`/usr/bin/dirname $curdir`
setenv PYTHONPATH ${PYTHONPATH}:${curdir}/lib

