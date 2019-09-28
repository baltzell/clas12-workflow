#!/bin/bash

# Check hipo file integrity
# Aborts with non-zero status on first error.
# Requires hipo-utils is in $PATH

if [ "$#" -lt 1 ]
then
  echo "Usage: check-integrity.sh filename [filename [filename ...]]]"
  exit 1
fi

for file in $@
do
    hipo-utils -test $file
    if [ ! "$?" -eq 0 ]
    then
      echo Corrupt: $file
      exit 2
    fi
done

