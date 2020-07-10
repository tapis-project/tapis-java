#!/bin/sh
# Build and optionally push docker image for Systems service
# This is the job run in Jenkins as part of job Java-MultiService-Build-Publish-Deploy-<ENV>
# Environment name must be passed in as first argument
# Existing docker login is used for push
# Docker image is created with a unique tag: tapis/systems-<ENV>-<VER>-<COMMIT>-<YYYYmmddHHMM>
#   - other tags are created and updated as appropriate
#
# REPO env var may be set to push to $REPO/systems. Default is tapis/systems

PrgName=$(basename "$0")

USAGE="Usage: $PrgName { dev staging prod } [ -push ]"

if [ -z "$REPO" ]; then
  REPO=tapis
fi

BUILD_DIR=../../tapis-systemsapi/target
ENV=$1

# Check number of arguments
if [ $# -lt 1 -o $# -gt 2 ]; then
  echo $USAGE
  exit 1
fi

# Check that env name is valid
if [ "$ENV" != "dev" -a "$ENV" != "staging" -a "$ENV" != "prod" ]; then
  echo $USAGE
  exit 1
fi

# Check second arg
if [ $# -eq 2 -a "x$2" != "x-push" ]; then
  echo $USAGE
  exit 1
fi

# Determine absolute path to location from which we are running
#  and change to that directory.
export RUN_DIR=$(pwd)
export PRG_RELPATH=$(dirname "$0")
cd "$PRG_RELPATH"/. || exit
export PRG_PATH=$(pwd)

# Make sure service has been built
if [ ! -d "$BUILD_DIR" ]; then
  echo "Build directory missing. Please build. Directory: $BUILD_DIR"
  exit 1
fi

# Copy Dockerfile to build dir
cp Dockerfile $BUILD_DIR

# Move to the build directory
cd $BUILD_DIR || exit

# Set variables used for build
VER=$(cat classes/tapis.version)
GIT_BRANCH=$(awk '{print $1}' classes/git.info)
GIT_COMMIT=$(awk '{print $2}' classes/git.info)
TAG_UNIQ="${REPO}/systems:${ENV}-${VER}-$(date +%Y%m%d%H%M)-${GIT_COMMIT}"
TAG_ENV_VER="${REPO}/systems:${ENV}-${VER}"
TAG_ENV="${REPO}/systems:${ENV}"
TAG_LATEST="${REPO}/systems:latest"

# Build image from Dockerfile
echo "Building local image using primary tag: $TAG_UNIQ"
echo "  ENV=        ${ENV}"
echo "  VER=        ${VER}"
echo "  GIT_BRANCH= ${GIT_BRANCH}"
echo "  GIT_COMMIT= ${GIT_BRANCH}"
docker build -f Dockerfile \
   --label VER="${VER}" --label GIT_COMMIT="${GIT_COMMIT}" --label GIT_BRANCH="${GIT_BRANCH}" \
    -t "${TAG_UNIQ}" .

# Create other tags for remote repo
echo "Creating ENV image tag: $TAG_ENV"
docker tag "$TAG_UNIQ" "$TAG_ENV"
echo "Creating ENV_VER image tag: $TAG_ENV_VER"
docker tag "$TAG_UNIQ" "$TAG_ENV_VER"

# Push to remote repo
if [ "x$2" = "x-push" ]; then
  if [ "$ENV" = "prod" ]; then
    echo "Creating third image tag for prod env: $TAG_LATEST"
    docker tag "$TAG_UNIQ" "$TAG_LATEST"
  fi
  echo "Pushing images to docker hub."
  # NOTE: Use current login. Jenkins job does login
  docker push "$TAG_UNIQ"
  docker push "$TAG_ENV_VER"
  docker push "$TAG_ENV"
  if [ "$ENV" = "prod" ]; then
    docker push "$TAG_LATEST"
  fi
fi
cd "$RUN_DIR"
