#!/bin/bash

export CLARA_USER_DATA=.
export CLAS12DIR=${CLARA_HOME}/plugins/clas12
export PATH=${PATH}:$CLAS12DIR/bin

export CLARA_MONITOR_FE="129.57.70.24%9000_java"
export CCDB_CONNECTION=mysql://clas12reader@clasdb-farm.jlab.org/clas12
export RCDB_CONNECTION=mysql://rcdb@clasdb-farm.jlab.org/rcdb

nevents=
outprefix=rec_
logdir=.
threads=16
while getopts "p:l:t:n:" OPTION; do
    case $OPTION in
        p)  outprefix=$OPTARG ;;
        l)  logdir=$OPTARG ;;
        t)  threads=$OPTARG ;;
        n)  nevents="-e $OPTARG" ;;
        ?)  exit 1 ;;
    esac
done
shift $((OPTIND-1))
if [[ $# -ne 1 ]]; then
    echo "usage: clara.sh [ OPTIONS ] jobname"
    exit 1
fi
jobname=$1

# check existence, size, and hipo-utils -test:
hipocheck() {
    ( [ -e $1 ] && [ $(stat -c%s $1) -gt 100 ] && hipo-utils -test $1 ) \
        || \
    ( echo "clara.sh:ERROR  Corrupt File: $1" 2>&1 && false )
}

# run-clara uses some of these to store info during job:
mkdir -p $logdir
mkdir -p $CLARA_USER_DATA/log
mkdir -p $CLARA_USER_DATA/config
mkdir -p $CLARA_USER_DATA/data/output

# setup filelist:
find . -maxdepth 1 -type f -name '*.hipo' | sed 's;^\./;;' > filelist.txt
ls -lt

# check inputs:
for xx in `cat filelist.txt`
do
    hipocheck $xx || ( rm -f *.hipo && exit 501)
done

# run clara:
$CLARA_HOME/lib/clara/run-clara \
        -i . \
        -o . \
        -z $outprefix \
        -x $logdir \
        -t 16 \
        $nevents \
        -s $jobname \
        ./clara.yaml \
        ./filelist.txt
claraexit=$?

# check outputs:
for xx in `cat filelist.txt`
do
    hipocheck $outprefix$xx || ( rm -f *.hipo && exit 502 )
done

# remove this later:
#ls -lt

# if all else is well, use exit code from run-clara:
exit $claraexit

