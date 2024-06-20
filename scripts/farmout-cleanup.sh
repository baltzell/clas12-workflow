#!/bin/bash

USAGE='farmout-cron.sh [-d #] [-z #]\n\t -d = delete older than # days\n\t -z = gzip older than # days'
DIRECTORY=/farm_out/$USER
PATH=/bin:/usr/bin

while getopts "d:z:" opt
do
    case "${opt}" in
        d)
            find $DIRECTORY -mindepth 1 -type f -mtime +$OPTARG -delete 
            find $DIRECTORY -mindepth 1 -type d -mtime +$OPTARG -empty -delete 
            ;;
        z)
            find $DIRECTORY -mindepth 1 -type f -mtime +$OPTARG -not -name '*.gz' -exec gzip -f {} \;
            ;;
        *)
            echo -e "\nUsage = $USAGE\n"
            exit
            ;;
    esac
done

