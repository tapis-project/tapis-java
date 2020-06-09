#!/bin/sh
PrgName=$(basename "$0")

# Build docker image for Systems service
BUILD_DIR=../../tapis-systemsapi/target

# Determine absolute path to location from which we are running.
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
export VER=$(cat classes/tapis.version)
export GIT_BRANCH=$(awk '{print $1}' classes/git.info)
export GIT_COMMIT=$(awk '{print $2}' classes/git.info)
export TAG="tapis/systems:${VER}"
export TAG2="tapis/systems:${VER}"

# Build image from Dockerfile
docker build -f Dockerfile --build-arg VER="${VER}" \
    --build-arg GIT_COMMIT="${GIT_COMMIT}" --build-arg GIT_BRANCH="${GIT_BRANCH}" \
    -t "${TAG}" .

# Create tagged image for remote repo
docker tag "$TAG" "$TAG2"

# Push to remote repo
if [ "x$1" = "x-push" ]; then
  # Login to docker. Credentials set by Jenkins
  docker login -u "$USERNAME" -p "$PASSWD"
  docker push "$TAG2"
fi
cd "$RUN_DIR"
