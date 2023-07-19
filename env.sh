
# load dependencies:
if [ -e /group/clas12/packages/setup.sh ]
then
    source /group/clas12/packages/setup.sh
    module purge
    module load clas12
else
    echo WARNING:  Cannot find RCDB installation.
fi

# put clas12-workflow/lib in $PYTHONPATH
d="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=${d}/lib/clas12:${d}/lib/hps:${d}/lib/swif:${d}/lib/util:${d}/lib/ccdb:${PYTHONPATH}
export PATH=${d}/scripts:${PATH}

