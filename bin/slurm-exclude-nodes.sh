#!/bin/bash

USAGE () {
    echo -e "\nUsage:  slurm-exclude.nodes.sh [-h] [-k] [-e] NODES\n"
    echo -e "\t-h     print this usage"
    echo -e "\t-k     kill jobs running on the specified nodes"
    echo -e "\t-e     exclude the specified nodes from pending jobs"
    echo -e "\tNODES  a SLURM specificiation for a list of nodes\n"
    echo -e "Example of a SLURM node specification: 'farm[1909-1912]'\n"
}

KILL=0
EXCLUDE=0
while getopts "khe" opt
do
    case "${opt}" in
        k)
            KILL=1
            ;;
        e)
            EXCLUDE=1
            ;;
        *)
            USAGE
            exit
            ;;
    esac
done

shift $((OPTIND-1))

if [ $KILL -eq 0 ] && [ $EXCLUDE -eq 0 ]
then
    echo "ERROR:  At least one of -k or -e must be specified."
    USAGE
    exit
elif [ "$#" -ne 1 ]
then
    echo -e "ERROR:  Exactly 1 NODES must be specified."
    USAGE
    exit
fi

NODES=$1

if [ $KILL -eq 1 ]
then
    scancel -u $USER -w $NODES
fi

if [ $EXCLUDE -eq 1 ]
then

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
            echo "Excluding $NODES for job #$jobid"
            scontrol update UserID=$USER JobId=$jobid ExcNodeList=$NODES
        fi
    done
fi

