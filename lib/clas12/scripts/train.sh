#!/bin/bash

ulimit -u 49152

export CLARA_USER_DATA=.
export CLAS12DIR=${CLARA_HOME}/plugins/clas12
export PATH=${PATH}:$CLAS12DIR/bin

unset CLARA_MONITOR_FE
#export CLARA_MONITOR_FE="129.57.70.24%9000_java"

export CCDB_CONNECTION=mysql://clas12reader@clasdb-farm.jlab.org/clas12
export RCDB_CONNECTION=mysql://rcdb@clasdb-farm.jlab.org/rcdb

nevents=
logdir=.
threads=12
yaml=clara.yaml
jobname=train
while getopts "p:l:t:n:y:" OPTION; do
    case $OPTION in
        l)  logdir=$OPTARG ;;
        t)  threads=$OPTARG ;;
        n)  nevents="-e $OPTARG" ;;
        y)  yaml=$OPTARG ;;
        ?)  exit 1 ;;
    esac
done

shift $((OPTIND-1))
[[ $# -ne 0 ]] && jobname=$1

# if it's an exclusive job:
[[ $threads -eq 0 ]] && threads=`grep -c ^processor /proc/cpuinfo`

# get libraries:
CLASSPATH="${CLARA_HOME}/lib/*"
for plugin in "${CLARA_HOME}/plugins"/*/; do
    plugin=${plugin%*/}
    for subdir in lib/core lib/services services lib
    do
      if [ -e ${plugin}/$subdir ]
      then
        CLASSPATH+=":${plugin}/$subdir/*"
      fi
    done
done
export CLASSPATH

# get train ids for expected output files:
trainids=`sed 's/^\s*//' $yaml | grep '^id:' | awk '{print$2}' | sort -n | uniq`
echo "train.sh: INFO: Train IDs:  "$trainids

# check existence, size, and hipo-utils -test:
hipocheck() {
    ( [ -e $1 ] && [ $(stat -L -c%s $1) -gt 100 ] && hipo-utils -test $1 ) \
        || \
    ( echo "train.sh: ERROR: Corrupt File: $1" 2>&1 && false )
}

# run-clara uses some of these to store info during job:
mkdir -p $logdir
mkdir -p $CLARA_USER_DATA/log
mkdir -p $CLARA_USER_DATA/config
mkdir -p $CLARA_USER_DATA/data/output

# setup filelist:
find . -maxdepth 1 -xtype f -name '*.hipo' | sed 's;^\./;;' > filelist.txt
ls -lt

# check inputs:
for xx in `cat filelist.txt`
do
    hipocheck $xx || ( rm -f *.hipo && false ) || exit 103
done

echo YAMLYAMLYAMLYAMLYAMLYAMLYAMLYAMLYAMLYAMLYAMLYAMLYAMLYAMLYAMLYAML
cat $yaml
echo YAMLYAMLYAMLYAMLYAMLYAMLYAMLYAMLYAMLYAMLYAMLYAMLYAMLYAMLYAMLYAML

# run clara:
date +'CLARA START: %F %H:%M:%S'
$CLARA_HOME/lib/clara/run-clara \
        -i . \
        -o . \
        -z skim_ \
        -x $logdir \
        -t $threads \
        $nevents \
        -s $jobname \
        $yaml \
        ./filelist.txt
claraexit=$?
date +'CLARA STOP: %F %H:%M:%S'
ls -lt

# check and rename outputs:
for xx in `cat filelist.txt`
do
    for nn in $trainids
    do
        yy=./skim_${xx}_${nn}.hipo
        zz=./skim${nn}_${xx}
        hipocheck $yy || ( rm -f *.hipo && false ) || exit 104
        mv -f $yy $zz
    done
done
ls -lt

# if all else is well, use exit code from run-clara:
exit $claraexit

