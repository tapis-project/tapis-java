#!/usr/bin/env bash
###########################################################
#  This script helps build images for service specified
#  It relies on Docker 18.06.0-ce and acts as a template
#  for future Tapis services and building an image from a
#  set of maven artifacts.
#
# environment : TAPIS_VERSION set to the version in tapis/pom.xml 
#
# usage : $TAPIS_ROOT/deployment/build-sampleapi.sh
#
###########################################################
export VER=${TAPIS_VERSION}
export TAPIS_ENV=${TAPIS_ENV}
export SRVC=security
export SRVC_API=${SRVC}api
export TAPIS_ROOT=$(pwd)
export SRVC_DIR="${TAPIS_ROOT}/tapis-${SRVC_API}/target"
export TAG="tapis/${SRVC_API}:$VER"
export BUILD_DIR="$TAPIS_ROOT/deployment/tapis-${SRVC_API}"
export BUILD_FILE="$BUILD_DIR/Dockerfile"
export GIT_COMMIT=${GIT_COMMIT}
export WAR_NAME=v3

# See if we can determine the git commit if it's not already set.
# Basically, we take the second word in the git.info file.
if [ -z "${GIT_COMMIT}" ]
then 
    export GIT_COMMIT="$(awk '{print $2}' ${SRVC_DIR}/${SRVC}/WEB-INF/classes/git.info)" 
fi

echo "VER: $VER"
echo "TAPIS_ENV: $TAPIS_ENV"
echo "SRVC: $SRVC"
echo "SRVC_API: $SRVC_API"
echo "TAPIS_ROOT: $TAPIS_ROOT"
echo "SRVC_DIR: $SRVC_DIR"
echo "TAG: $TAG"
echo "BUILD_DIR: $BUILD_DIR"
echo "BUILD_FILE: $BUILD_FILE"
echo "GIT_COMMIT: $GIT_COMMIT"
echo "WAR_NAME: $WAR_NAME"
echo ""

echo "    removing any old service war files from Docker build context"
rm $BUILD_DIR/$WAR_NAME.war

echo "    unzipping $SRVC.war to ${BUILD_DIR}/${SRVC} "
unzip $SRVC_DIR/$WAR_NAME.war -d ${BUILD_DIR}/${WAR_NAME}

echo "    building the docker image from deployment/tapis-${SRVC_API}/Dockerfile"
echo " docker image build -f $BUILD_FILE --build-arg SRVC_WAR=$SRVC.war --build-arg VER=$VER --build-arg GIT_COMMIT=$GIT_COMMIT -t $TAG-$TAPIS_ENV $BUILD_DIR "
docker image build -f $BUILD_FILE --build-arg SRVC_ROOT=${WAR_NAME} --build-arg VER=$VER --build-arg GIT_COMMIT=$GIT_COMMIT  -t $TAG$TAPIS_ENV $BUILD_DIR

echo "    remvoing ${BUILD_DIR}/${SRVC}"
rm -fr ${BUILD_DIR}/${WAR_NAME}

