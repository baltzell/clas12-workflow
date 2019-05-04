#!/bin/bash

# Check hipo file integrity
# Aborts with non-zero status on first error.

cmd=/group/clas12/packages/coatjava-6.0.0/bin/hipo-utils

if [ "$#" -lt 1 ]
then
  echo "Usage: check-integrity.sh filename [filename [filename ...]]]"
  exit 1
fi

for file in $@
do
    $cmd -test $file
    if [ ! "$?" -eq 0 ]
    then
      echo Corrupt: $file
      exit 2
    fi
done

