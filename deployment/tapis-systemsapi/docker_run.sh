#!/bin/sh
# Start up local docker image for tapis/systems service.
# Environment value must be passed in as first argument: dev, staging, prod
# Service password must be set as the env variable TAPIS_SERVICE_PASSWORD
# Following services from a running tapis3 are required: tenants, tokens, security-kernel
# Base URL for remote services is determined by environment value passed in.
# Systems service is available at http://localhost:8080/v3/systems

PrgName=$(basename "$0")

USAGE1="Usage: $PRG_NAME { dev, staging, prod }"

# Run docker image for Systems service
BUILD_DIR=../../tapis-systemsapi/target
ENV=$1
TAG="tapis/systems:${ENV}"

##########################################################
# Check number of arguments.
##########################################################
if [ $# -ne 1 ]; then
  echo "Please provide environment"
  echo $USAGE1
  exit 1
fi

if [ -z "$TAPIS_SERVICE_PASSWORD" ]; then
  echo "Please set env variable TAPIS_SERVICE_PASSWORD to the service password"
  echo $USAGE1
  exit 1
fi

# Set base url for services we depend on (tenants, tokens, security-kernel)
if [ "$ENV" = "dev" ]; then
 BASE_URL="https://master.develop.tapis.io"
elif [ "$ENV" = "staging" ]; then
 BASE_URL="https://master.staging.tapis.io"
elif [ "$ENV" = "prod" ]; then
 BASE_URL="https://master.tapis.io"
else
  echo $USAGE1
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

VER=$(cat "$BUILD_DIR/classes/tapis.version")
echo
echo "Build version: $VER"
echo

# Running with network=host exposes ports directly. Only works for linux
docker run -e TAPIS_SERVICE_PASSWORD="${TAPIS_SERVICE_PASSWORD}" \
           -e TAPIS_TENANT_SVC_BASEURL="$BASE_URL" \
           -d --rm --network="host" "${TAG}"

cd "$RUN_DIR"
