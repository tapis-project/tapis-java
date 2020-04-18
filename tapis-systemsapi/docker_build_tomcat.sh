#!/bin/sh
export VER=`cat target/v3#systems/WEB-INF/classes/tapis.version`
export GIT_COMMIT=`awk '{print $2}' target/v3#systems/WEB-INF/classes/git.info`
export TAG="tapis/systems:${VER}"
export TAG2="tapis/systems:${VER}"
# Build image from Dockerfile
docker build -f Dockerfile --build-arg VER=${VER} --build-arg GIT_COMMIT=${GIT_COMMIT} -t ${TAG} .
# Create tagged image for remote repo
docker tag $TAG $TAG2
# Push to remote repo
if [ "x$1" = "x-push" ]; then
  # Login to docker. Credentials set by Jenkins
  docker login -u $USERNAME -p $PASSWD
  docker push $TAG2
fi
