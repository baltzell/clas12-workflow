
if [ -e /group/clas12/packages/setup.sh ]
then
    source /group/clas12/packages/setup.sh
    module load rcdb
else
    echo WARNING:  Cannot find RCDB installation.
fi

# put clas12-workflow/lib in $PYTHONPATH
d="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=${PYTHONPATH}:${d}/lib

