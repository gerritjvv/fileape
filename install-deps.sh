#!/usr/bin/env bash

BASE_DIR=`dirname $BASH_SOURCE`

mvn install:install-file -Dfile="$BASE_DIR/libs/hadoop-lzo-0.1.0.jar" -DgroupId="org.hadoop" -DartifactId="hadoop-lzo" -Dpackaging="jar" -Dversion="0.1.0"
