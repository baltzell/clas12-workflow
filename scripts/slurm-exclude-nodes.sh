#!/bin/bash

usage="\nUsage:  slurm-exclude-nodes.sh NODELIST\n"
usage="$usage\twhere NODELIST is a SLURM specification for nodes to exclude, e.g.\n"
usage="$usage\t'farm[1909-1912]'.  This operates on all \$USERS's PENDING jobs.\n"

if [ "$#" -ne 1 ]
then
    echo -e $usage
    echo -e "ERROR: Exactly ONE argument is required, the NODELIST.\n"
    exit
elif [ "$1" = "-h" ]
then
    echo -e $usage
    exit
fi

ExcNodeList=$1

# Seems we cannot just apply it to all jobs, but only via id/name,
# (hence this convenience script), so let's get and loop over a list
# of applicable jobs:
for x in $(squeue --noheader -u $USER --states=PENDING -o '%A,%x')
do
    # check whether it already matches, just to speed
    # this up and avoid unnecessary SLURM modifications:
    jobid=$(echo $x | awk -F, '{print$1}')
    exclu=$(echo $x | awk -F, '{print$2}')
    if [ "$exclu" = "" ] || ! [ $exclu = $1 ]
    then
        echo "Excluding nodes for job #$jobid: $ExcNodeList"
        scontrol update UserID=$USER JobId=$jobid ExcNodeList=$ExcNodeList
    fi
done

