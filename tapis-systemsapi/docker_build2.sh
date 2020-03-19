#!/bin/sh
PrgName=`basename $0`

# Determine absolute path to location from which we are running.
export RUN_DIR=`pwd`
export PRG_RELPATH=`dirname $0`
cd $PRG_RELPATH/.
export PRG_PATH=`pwd`

export VER=`cat target/classes/tapis.version`
export GIT_COMMIT=`awk '{print $2}' target/classes/git.info`
export TAG="tapis/systems:${VER}"
export TAG2="tapis/systems:${VER}"
# Build image from Dockerfile
docker build -f Dockerfile2 --build-arg VER=${VER} --build-arg GIT_COMMIT=${GIT_COMMIT} -t ${TAG} .
# Create tagged image for remote repo
docker tag $TAG $TAG2
# Push to remote repo
if [ "x$1" = "x-push" ]; then
  # Login to docker. Credentials set by Jenkins
  docker login -u $USERNAME -p $PASSWD
  docker push $TAG2
fi
cd $RUN_DIR
