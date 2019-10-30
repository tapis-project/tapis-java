#!/bin/sh
export VER=`cat target/systems/WEB-INF/classes/tapis.version`
export GIT_COMMIT=`awk '{print $2}' target/systems/WEB-INF/classes/git.info`
export TAG="tapis/systems:${VER}"
docker build --build-arg VER=${VER} --build-arg GIT_COMMIT=${GIT_COMMIT} -t ${TAG} .
# docker image build -f $BUILD_FILE --build-arg SRVC_ROOT=${SRVC} --build-arg VER=$VER --build-arg GIT_COMMIT=$GIT_COMMIT  -t $TAG$TAPIS_ENV $BUILD_DIR
