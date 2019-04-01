source /group/clas12/rcdb/environment.bash

d="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=${PYTHONPATH}:${d}/lib
