#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#
#$DIR/lint.sh
#
#if [ "$?" -ne 0 ]; then
# echo "Failed linting"
# exit -1
#fi

cd "$DIR"; lein midje

if [ "$?" -ne 0 ]; then
 echo "Failed testing"
 exit -1
fi

cd "$DIR"; LEIN_SNAPSHOTS_IN_RELEASE=true lein install; 
