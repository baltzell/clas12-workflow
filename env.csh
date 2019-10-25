
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
set d=`/usr/bin/readlink -f $sourced[2]`
set d=`/usr/bin/dirname $d`
setenv PYTHONPATH ${PYTHONPATH}:${d}/lib/clas12:${d}/lib/util:${d}/lib/swif
setenv PATH ${PATH}:${d}/scripts

