#!/bin/sh
# export VER=`cat target/v3#systems/WEB-INF/classes/tapis.version`
# export GIT_COMMIT=`awk '{print $2}' target/v3#systems/WEB-INF/classes/git.info`
export VER="0.0.1"
export GIT_COMMIT="abcdefg"

export TAG="tapis/systems:${VER}"
# docker run -d --rm --network="host" -p 7070:8080 -p 7000:8000 ${TAG}
# Running with network=host exposes ports directly. Only works for linux
set -v
docker run -d --rm --network="host" ${TAG}
