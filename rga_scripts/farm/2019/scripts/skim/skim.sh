#!/bin/bash

export MALLOC_ARENA_MAX=2
export MALLOC_MMAP_THRESHOLD_=131072
export MALLOC_TRIM_THRESHOLD_=131072
export MALLOC_TOP_PAD_=131072
export MALLOC_MMAP_MAX_=65536
export MALLOC_MMAP_MAX_=65536
export JAVA_OPTS="-XX:+UseNUMA -XX:+UseBiasedLocking"

export CLARA_MONITOR_FE="clara1601%9000_java"
export CLARA_USER_DATA=$PWD

export CLAS12DIR=$COATJAVA

echo "listing the files";
ls -l

mkdir log

#FILES=*.hipo
#for file in $FILES; do hipo-utils -filter -e 330 -l 11:330:331:332:333:334:335:336:337:338:339:340:20013  -o "dst_$file" $file; done

perl check.pl

# find plugins directory
if [ -n "${CLARA_PLUGINS}" ]; then
    if [ ! -d "${CLARA_PLUGINS}" ]; then
        echo "Error: \$CLARA_PLUGINS is not a directory."
        exit 1
    fi
    plugins_dir="${CLARA_PLUGINS}"
else
    plugins_dir="${CLARA_HOME}/plugins"
fi

# set default classpath
if [ -z "${CLASSPATH}" ]; then
    CLASSPATH="${CLARA_HOME}/lib/*"

    # Add every plugin
    for plugin in "${plugins_dir}"/*/; do
        plugin=${plugin%*/}
        if [ "${plugin##*/}" = "grapes" ]; then # COAT has special needs
            CLASSPATH+=":${plugin}/lib/core/*:${plugin}/lib/services/*"
        else
            CLASSPATH+=":${plugin}/services/*:${plugin}/lib/*"
        fi
    done

    CLASSPATH+=":${CLARA_HOME}/services/*"
    export CLASSPATH
fi


ls -1 *.hipo > clarafiles.txt

#"$CLARA_HOME/bin/kill-dpes"

sleep $[ ( $RANDOM % 20 )  + 1 ]s

$CLARA_HOME/lib/clara/run-clara \
        -i ./ \
        -o ./ \
        -z skim_ \
        -t 16 \
        -r 1000 \
        -s clara_000 \
         $1 \
        clarafiles.txt
echo "ls ./"
ls 
echo "Moving files"
perl check.pl
mv skim_* $2
