#!/bin/bash

usage="postproc.sh [-r] [-p] [-d] -o output input [input ...]]"
[ "x$CLARA_HOME" != "x" ] && bin=$CLARA_HOME/plugins/clas12/bin
[ "x$COATJAVA" != "x" ] && [ "x$bin" == "x" ] && bin=$COATJAVA/bin
[ "x$CLAS12DIR" == "x" ] && export CLAS12DIR=$COATJAVA

export PATH=$bin:${PATH}
while getopts "rpdo:" OPTION; do
    case $OPTION in
        r)  recharge=1 ;;
        p)  postproc=1 ;;
        d)  delay=1 ;;
        o)  output=$OPTARG ;;
        ?)  echo $usage && exit 1 ;;
    esac
done
shift $((OPTIND-1))
input=$@
[ "x$recharge" == "x" ] && [ "x$postproc" == "x" ] && echo $usage && echo ERROR:  at least one of -r/-p is required. && exit 1
[ "x$output" == "x" ] && echo $usage && echo ERROR:  -o is required. && exit 1
[ "x$input" == "x" ] && echo $usage && echo ERROR:  input file required. && exit 1

set -x
set -e
tmpfile=tmp.hipo
cat /etc/redhat-release

function jjava {
    java -Xmx768m -Xms768m -XX:+UseSerialGC \
        -Djava.io.tmpdir=. -Dorg.sqlite.tmpdir=. \
        -cp "$COATJAVA/lib/clas/*:$COATJAVA/lib/services/*:$COATJAVA/lib/utils/*" \
        org.jlab.analysis.postprocess.$1 "${@:2}"
}

trap '[ "$?" -eq 0 ] || rm -f $output && rm -f $tmpfile' EXIT

if [ "x$recharge" != "x" ]; then
    if [ "x$postproc" != "x" ]; then
        jjava RebuildScalers -o $tmpfile $input || exit 111
    else
        jjava RebuildScalers -o $output $input || exit 111
    fi
    input=$tmpfile
fi

if [ "x$postproc" != "x" ]; then
    opts='-q 1'
    [ "x$delay" != "x" ] && opts="$opts -d 1"
    jjava Tag1ToEvent $opts -o $output $input || exit 112
fi

[ -e $output ] && [ $(stat -L -c%s $output) -gt 100 ] || exit 113
hipo-utils -test $output || exit 114

exit 0

