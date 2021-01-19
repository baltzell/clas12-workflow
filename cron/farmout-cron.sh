#!/bin/bash

USAGE='farmout-cron.sh [-d #] [-z #]\n\t -d = delete older than # days\n\t -z = gzip older than # days'
DIRECTORY=/farm_out/$USER
PATH=/bin:/usr/bin

while getopts "d:z:" opt
do
    case "${opt}" in
        d)
            echo find $DIRECTORY -type f -mtime +$OPTARG -delete 
            echo find $DIRECTORY -type d -mtime +$OPTARG -empty -delete 
            ;;
        z)
            echo find $DIRECTORY -type f -mtime +$OPTARG -not -name '*.gz' -exec gzip {} \;
            ;;
        *)
            echo -e "\nUsage = $USAGE\n"
            exit
            ;;
    esac
done

