#!/bin/bash

export CLARA_USER_DATA=.
export CLAS12DIR=${CLARA_HOME}/plugins/clas12
export PATH=${PATH}:$CLAS12DIR/bin

export CLARA_MONITOR_FE="129.57.70.24%9000_java"
export CCDB_CONNECTION=mysql://clas12reader@clasdb-farm.jlab.org/clas12
export RCDB_CONNECTION=mysql://rcdb@clasdb-farm.jlab.org/rcdb

nevents=
logdir=.
threads=16
while getopts "p:l:t:n:" OPTION; do
    case $OPTION in
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

# if it's an exclusive job:
if [ $threads -eq 0 ]
then
  threads=`grep -c ^processor /proc/cpuinfo`
fi

# get libraries:
CLASSPATH="${CLARA_HOME}/lib/*"
for plugin in "${plugins_dir}"/*/; do
    plugin=${plugin%*/}
    if [ "${plugin##*/}" = "grapes" ]; then # COAT has special needs
        CLASSPATH+=":${plugin}/lib/core/*:${plugin}/lib/services/*"
    else
        CLASSPATH+=":${plugin}/services/*:${plugin}/lib/*"
    fi
done
export CLASSPATH

# count services:
nservices=`python - <<'EOF'
n,go=0,False
for line in open('train.yaml','r').readlines():
  line=line.strip()
  if line.find('services:')==0:
    go=True
  elif go:
    if line.find('- class:')==0:
      n+=1
print n  
EOF`

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
        -z skim_ \
        -x $logdir \
        -t $threads \
        $nevents \
        -s $jobname \
        ./clara.yaml \
        ./filelist.txt
claraexit=$?

# check and rename outputs:
for xx in `cat filelist.txt`
do
    for nn in `seq $nservices`
    do
        yy=skim_${xx}_${nn}.hipo
        hipocheck $yy || ( rm -f *.hipo && exit 502 )
        zz=skim${nn}_${xx}
        mv -f $yy $zz
    done
done

# remove this later:
#ls -lt

# if all else is well, use exit code from run-clara:
exit $claraexit

