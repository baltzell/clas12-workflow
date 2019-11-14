#!/bin/bash

export CLARA_USER_DATA=.
export CLAS12DIR=${CLARA_HOME}/plugins/clas12
export PATH=${PATH}:$CLAS12DIR/bin

export CLARA_MONITOR_FE="129.57.70.24%9000_java"
export CCDB_CONNECTION=mysql://clas12reader@clasdb-farm.jlab.org/clas12
export RCDB_CONNECTION=mysql://rcdb@clasdb-farm.jlab.org/rcdb

nevents=
logdir=.
threads=12
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

# count services:
# to identify the expected output files
nservices=`python - <<'EOF'
ids=[]
for line in open('clara.yaml','r').readlines():
  if line.strip().find('id: ')==0:
    id=int(line.strip().split()[1])
    if id not in ids:
      ids.append(id)
print len(ids)
EOF`

echo "#Services: "$nservices

# check existence, size, and hipo-utils -test:
hipocheck() {
    ( [ -e $1 ] && [ $(stat -L -c%s $1) -gt 100 ] && hipo-utils -test $1 ) \
        || \
    ( echo "clara.sh:ERROR  Corrupt File: $1" 2>&1 && false )
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
    # FIXME: this requires service ids are sequential, no skips
    for nn in `seq $nservices`
    do
        yy=./skim_${xx}_${nn}.hipo
        zz=./skim${nn}_${xx}
        hipocheck $yy || ( rm -f *.hipo && exit 502 )
        mv -f $yy $zz
    done
done

# if all else is well, use exit code from run-clara:
exit $claraexit

