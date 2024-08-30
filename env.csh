
module load ccdb rcdb coatjava

set sourced=($_)
set d=`/usr/bin/readlink -f $sourced[2]`
set d=`/usr/bin/dirname $d`
if ( ! $?PYTHONPATH ) setenv PYTHONPATH
setenv PYTHONPATH ${d}/lib/clas12:${d}/lib/hps:${d}/lib/util:${d}/lib/swif:${PYTHONPATH}
setenv PATH ${d}/scripts:${PATH}

