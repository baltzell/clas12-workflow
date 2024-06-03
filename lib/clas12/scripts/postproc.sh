#!/bin/bash
usage="postproc.sh [-r] [-p] [-d] -o output input [input ...]]"
export JAVA_OPTS="$JAVA_OPTS -Djava.io.tmpdir=. -Dorg.sqlite.tmpdir=."
[ "x$CLARA_HOME" != "x" ] && bin=$CLARA_HOME/plugins/clas12/bin
[ "x$COATJAVA" != "x" ] && [ "x$bin" == "x" ] && bin=$COATJAVA/bin
while getopts "rpdo:" OPTION; do
    case $OPTION in
        r)  recharge=1 ;;
        p)  postproc=1 ;;
        d)  nodelay=1 ;;
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
dbg=
trap 'rm -f $tmpfile && exit 107' EXIT

if [ "x$recharge" != "x" ]; then
    if [ "x$postproc" != "x" ]; then
        $dbg $bin/rebuild-scalers -o $tmpfile $input
    else
        $dbg $bin/rebuild-scalers -o $output $input
    fi
    input=$tmpfile
fi

if [ "x$postproc" != "x" ]; then
    opts='-q 1'
    [ "x$nodelay" != "x" ] || opts="$opts -d 1"
    $dbg $bin/postprocess $opts -o $output $input
fi

$dbg $bin/hipo-utils -test $output || rm -f $output
