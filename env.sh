
# load dependencies:
if [ -e /group/clas12/packages/setup.sh ]
then
    source /group/clas12/packages/setup.sh
    module purge
    module load rcdb/1.0
    module load ccdb/1.06.02
    module load root/6.14.04
else
    echo WARNING:  Cannot find RCDB installation.
fi

# put clas12-workflow/lib in $PYTHONPATH
d="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=${d}/lib/clas12:${d}/lib/hps:${d}/lib/swif:${d}/lib/util:${PYTHONPATH}
export PATH=${d}/scripts:${PATH}

