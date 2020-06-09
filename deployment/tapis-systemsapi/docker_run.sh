#!/bin/sh
PrgName=$(basename "$0")

USAGE1="Usage: $PRG_NAME <service_password>"

# Run docker image for Systems service
BUILD_DIR=../../tapis-systemsapi/target

##########################################################
# Check number of arguments.
##########################################################
if [ $# -ne 1 ]; then
  echo "Please provide service password"
  echo "$USAGE1"
  exit 1
fi

# Determine absolute path to location from which we are running.
export RUN_DIR=$(pwd)
export PRG_RELPATH=$(dirname "$0")
cd "$PRG_RELPATH"/. || exit
export PRG_PATH=$(pwd)

# Make sure service has been built (used for getting version)
if [ ! -d "$BUILD_DIR" ]; then
  echo "Build directory missing. Please build. Directory: $BUILD_DIR"
  exit 1
fi

SVC_PASSWD=$1

export VER=$(cat "$BUILD_DIR/classes/tapis.version")
export TAG="tapis/systems:${VER}"
# docker run -d --rm --network="host" -p 7070:8080 -p 7000:8000 ${TAG}
# Running with network=host exposes ports directly. Only works for linux
docker run -e TAPIS_SERVICE_PASSWORD="${SVC_PASSWD}" -d --rm --network="host" "${TAG}"

cd "$RUN_DIR"
