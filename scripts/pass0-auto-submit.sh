#!/bin/bash

d=`/usr/bin/readlink -f $0`
d=`/usr/bin/dirname $d`/..
export PYTHONPATH=${d}/lib/swif:${d}/lib/util:${d}/lib/clas12:${d}/lib/ccdb

workdir=$1
inputdir=$2
config=$workdir/config.json
blacklist=$workdir/blacklist.txt

[ $# -ne 2 ] && echo Usage:  pass0-auto-submit.sh workdir inputdir && exit 1
! [ -d $workdir ] && echo Nonexistent work directory:  $workdir && exit 2
! [ -w $workdir ] && echo Error with write access:  $workdir && exit 3
! [ -d $inputdir ] && echo Nonexistent input directory:  $inputdir && exit 4
! [ -r $config ] && echo Unreadable config file:  $config && exit 5
! [ -r $blacklist ] && echo Unreadable blacklist:  $blacklist && exit 6

timestamp=$(date +%Y%m%d@%H%M%S)
filelist=$(mktemp $workdir/logs/filelist_$timestamp.XXXXXX)
logfile=$(mktemp $workdir/logs/log_$timestamp.XXXXXX)

# find files to process:
echo FILELIST: $filelist >> $logfile
find $inputdir -type f -name '*.evio.0004?' -mmin +30 | grep -v -f $blacklist >> $filelist
! [ -s $filelist ] && echo NO NEW FILES >> $logfile && exit 0

# submit the jobs:
source "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../env.sh
cmd="clas12-workflow.py --config $config --inputs $filelist --tag op0$timestamp --submit"
echo $cmd >> $logfile
$cmd >> $logfile 2>&1
[ $? -ne 0 ] && echo !!!!!!!!!ERROR GENERATING WORKFLOW!!!!!!!! && cat $logfile && exit 7
cat $filelist >> $blacklist

