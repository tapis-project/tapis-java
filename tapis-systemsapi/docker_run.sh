#!/bin/sh
PrgName=`basename $0`

# Determine absolute path to location from which we are running.
export RUN_DIR=`pwd`
export PRG_RELPATH=`dirname $0`
cd $PRG_RELPATH/.
export PRG_PATH=`pwd`

export VER=`cat target/classes/tapis.version`
export TAG="tapis/systems:${VER}"
# docker run -d --rm --network="host" -p 7070:8080 -p 7000:8000 ${TAG}
# Running with network=host exposes ports directly. Only works for linux
docker run -d --rm --network="host" ${TAG}

cd $RUN_DIR
