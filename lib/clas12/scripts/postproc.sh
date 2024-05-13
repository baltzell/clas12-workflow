#!/bin/bash
export JAVA_OPTS="$JAVA_OPTS -Djava.io.tmpdir=. -Dorg.sqlite.tmpdir=."
usage="postproc.sh [-r] [-p] [-d] -o output input [input ...]]"

[ "x$CLARA_HOME" != "x" ] && bin=$CLARA_HOME/plugins/clas12/bin
[ "x$COATJAVA" != "x" ] && [ "x$bin" != "x" ] && bin=$COATJAVA/bin

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

if [ "x$recharge" != "x" ]; then 
    $bin/rebuild-scalers -o $output $input
    input=$output
fi
if [ "x$postprocess" != "x" ]; then
    [ "$input" == "$output" ] && mv -f $output x.hipo && input=x.hipo
    opts='-q 1'
    [ "x$nodelay" != "x" ] || opts="$opts -d 1"
    $bin/postprocess $opts -o $output $input
fi

$bin/hipo-utils -test $output || rm -f $output && exit 99
