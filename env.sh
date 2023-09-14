
# load dependencies:
shell_ext=$(ps -p $$ -ocomm= | sed -e 's;^.*\.;;g' -e 's;^bash$;sh;')
setup=/group/clas12/packages/setup.$shell_ext
if [ -e $setup ]
then
    source $setup
    module purge
    module load clas12
else
    echo "WARNING:  Cannot find CLAS12 modules setup file $setup"
fi

# put clas12-workflow/lib in $PYTHONPATH
d="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=${d}/lib/clas12:${d}/lib/hps:${d}/lib/swif:${d}/lib/util:${d}/lib/ccdb:${PYTHONPATH}
export PATH=${d}/scripts:${PATH}

