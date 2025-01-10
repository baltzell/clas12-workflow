#!/bin/bash

function usage() {
    x=$(basename $0)
    echo -e "\nUsage: $x [-h] [-q] [-d #] [-z #]\n\t -d = delete older than # days\n\t -z = gzip older than # days\n"
    exit $1
}

DIRECTORY=/farm_out/$USER
export PATH=/bin:/usr/bin

while getopts "d:z:qh" opt
do
    case "${opt}" in
        q)
            QUIET=1
            ;;
        d)
            [ -z ${QUIET+x} ] || set -x
            find $DIRECTORY -mindepth 1 -type f -mtime +$OPTARG -delete 
            find $DIRECTORY -mindepth 1 -type d -mtime +$OPTARG -empty -delete 
            ;;
        z)
            [ -z ${QUIET+x} ] || set -x
            find $DIRECTORY -mindepth 1 -type f -mtime +$OPTARG -not -name '*.gz' -exec gzip -f {} \;
            ;;
        h)
            usage 0
            ;;
        *)
            usage 1
            ;;
    esac
done

if [ "$OPTIND" -eq 1 ]; then
    echo "ERROR:  At least one of -d/-z is required."
    usage 1
fi

