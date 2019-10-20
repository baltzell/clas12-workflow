
# load dependencies:
if ( -e /group/clas12/packages/setup.csh ) then
    source /group/clas12/packages/setup.csh
    module load rcdb
    module load root
else
    echo WARNING:  Cannot find RCDB installation.
endif

# put clas12-workflow/lib in PYTHONPATH:
set sourced=($_)
set curdir=`/usr/bin/readlink -f $sourced[2]`
set curdir=`/usr/bin/dirname $curdir`
setenv PYTHONPATH ${PYTHONPATH}:${curdir}/lib
setenv PATH ${PATH}:${curdir}/scripts

