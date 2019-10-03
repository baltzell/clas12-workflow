
if ( -e /group/clas12/packages/setup.csh ) then
    source /group/clas12/packages/setup.csh
    module load rcdb
    module load root
else
    echo WARNING:  Cannot find RCDB installation.
endif

# NAB:  copied this from old /group/clas12/environment.csh ... pdo we need this?
#Changes python version to 2.7 in order to keep python scripts working. May affect user environment.
setenv PATH /apps/python/2.7.12/bin:$PATH

# put clas12-workflow/lib in PYTHONPATH:
set sourced=($_)
set curdir=`/usr/bin/readlink -f $sourced[2]`
set curdir=`/usr/bin/dirname $curdir`
setenv PYTHONPATH ${PYTHONPATH}:${curdir}/lib

