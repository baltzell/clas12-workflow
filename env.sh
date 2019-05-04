
if [ -e /group/clas12/rcdb/environment.bash ]
then
    source /group/clas12/rcdb/environment.bash
else
    echo WARNING:  Cannot find RCDB installation.
fi

d="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=${PYTHONPATH}:${d}/lib

