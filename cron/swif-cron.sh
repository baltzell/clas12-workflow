#!/bin/bash

d=$(dirname $(dirname -- "${BASH_SOURCE[0]}"))

export PYTHONPATH=${d}/lib/swif:${d}/lib/util:${d}/lib/clas12

if [ "$#" -gt 0 ]; then
  $d/bin/swif-status.py $@
else
  $d/bin/swif-status.py --retry
fi

