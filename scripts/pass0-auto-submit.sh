#!/bin/bash

d=`/usr/bin/readlink -f $0`
d=`/usr/bin/dirname $d`/..
export PYTHONPATH=${d}/lib/swif:${d}/lib/util:${d}/lib/clas12:${d}/lib/ccdb

USAGE () {
    echo -e "\nUsage:  pass0-auto-submit.sh [-d] TAG WORKDIR INPUTDIR\n"
    echo -e "\t-d       dry run, do not submit"
    echo -e "\tTAG      tag for workflow generator (--tag)"
    echo -e "\tWORKDIR  directory containing config.json and blacklist.txt"
    echo -e "\tINPUTDIR input directory for workflow generator (--inputs)\n"
}

DRYRUN=0
while getopts "dh" opt
do
    case "${opt}" in
        d)
            DRYRUN=1
            ;;
        h)
            USAGE
            exit
            ;;
        *)
            USAGE
            exit
            ;;
    esac
done

shift $((OPTIND-1))
[ $# -ne 3 ] && USAGE && exit 1

tag=$1
workdir=$2
inputdir=$3

config=$workdir/config.json
blacklist=$workdir/blacklist.txt

if ! [ -e $blacklist ]
then
    touch $blacklist
fi

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
find $inputdir -type f -name '*.evio.0004?' -mmin +90 | grep -v -f $blacklist >> $filelist
! [ -s $filelist ] && echo NO NEW FILES >> $logfile && exit 0

# submit the jobs:
source "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../env.sh
if [ $DRYRUN -eq 0 ]
then
    cmd="clas12-workflow.py --config $config --inputs $filelist --tag $tag --submit"
else
    cmd="clas12-workflow.py --config $config --inputs $filelist --tag $tag"
fi
echo $cmd >> $logfile
$cmd >> $logfile 2>&1
[ $? -ne 0 ] && echo !!!!!!!!!ERROR GENERATING WORKFLOW!!!!!!!! && cat $logfile && exit 7
cat $filelist >> $blacklist

