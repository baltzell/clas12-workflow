
d="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=${d}/lib/clas12:${d}/lib/hps:${d}/lib/swif:${d}/lib/util:${d}/lib/ccdb:${PYTHONPATH}
export PATH=${d}/scripts:${PATH}

