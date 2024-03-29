
# load dependencies:
if ( -e /group/clas12/packages/setup.csh ) then
    source /group/clas12/packages/setup.csh
    module purge
    module load clas12
else
    echo CANNOT SET ENVIRONMENT from /group/clas12/packages
endif

# put clas12-workflow/lib in PYTHONPATH:
set sourced=($_)
set d=`/usr/bin/readlink -f $sourced[2]`
set d=`/usr/bin/dirname $d`
setenv PYTHONPATH ${d}/lib/clas12:${d}/lib/hps:${d}/lib/util:${d}/lib/swif:${d}/lib/ccdb:${PYTHONPATH}
setenv PATH ${d}/scripts:${PATH}

