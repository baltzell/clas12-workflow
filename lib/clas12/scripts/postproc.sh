#!/bin/bash

BIN=$CLARA_HOME/plugins/clas12/bin

recharge=0
postproc=0
helflip=0
output=

while getopts "rpo:" OPTION; do
    case $OPTION in
        p)  postproc=1 ;;
        f)  helflip=1 ;;
        r)  recharge=1 ;;
        o)  output=$OPTARG ;;
        ?)  exit 1 ;;
    esac
done

# input files:
shift $((OPTIND-1))
input=$@

# rebuild REC::scaler banks:
if [ $recharge -eq 1 ]
then
    ls -l
    $BIN/rebuild-scalers -o tmp.hipo $input
    if [ $? -eq 0 ]
    then
        mv -f tmp.hipo $output
        input=$output
    else
        echo ERROR RUNNING rebuild-scalers
        rm -f tmp.hipo $output
        exit 1
    fi
fi

# copy tag-1 info to REC::Event:
if [ $postproc -eq 1 ]
then
    ls -l
    opt="-d 1 -q 1"
    [ $helflip -eq 1 ] && opt="$opt -f 1"
    $BIN/postprocess $opts -o tmp.hipo $input
    if [ $? -eq 0 ]
    then
        mv -f tmp.hipo $output
        input=$output
    else
        echo ERROR RUNNING postprocess
        rm -f tmp.hipo $output
        exit 2
    fi
fi

$BIN/hipo-utils -test $output || rm -f $output

ls -l $output

