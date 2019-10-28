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
export SRVC=securitymigrate
export TAPIS_ROOT=$(pwd)
export SRVC_DIR="${TAPIS_ROOT}/tapis-${SRVC}/target"
export TAG="tapis/${SRVC}:$VER"
export BUILD_DIR="$TAPIS_ROOT/deployment/tapis-${SRVC}"
export BUILD_FILE="$BUILD_DIR/Dockerfile"
export GIT_COMMIT=${GIT_COMMIT}

# See if we can determine the git commit if it's not already set.
# Basically, we take the second word in the git.info file.
if [ -z "${GIT_COMMIT}" ]
then 
    export GIT_COMMIT="$(awk '{print $2}' ${SRVC_DIR}/${SRVC}/WEB-INF/classes/git.info)" 
fi

echo "VER: $VER"
echo "TAPIS_ENV: $TAPIS_ENV"
echo "SRVC: $SRVC"
echo "TAPIS_ROOT: $TAPIS_ROOT"
echo "SRVC_DIR: $SRVC_DIR"
echo "TAG: $TAG"
echo "BUILD_DIR: $BUILD_DIR"
echo "BUILD_FILE: $BUILD_FILE"
echo "GIT_COMMIT: $GIT_COMMIT"
echo ""

echo "    removing any old service jar files from Docker build context"
rm $BUILD_DIR/${SRVC}.jar

echo "    moving $SRVC.jar to Dockerfile context deployment/tapis-${SRVC} "
cp $SRVC_DIR/${SRVC}.jar $BUILD_DIR

echo "    building the docker image from deployment/tapis-${SRVC}/Dockerfile"
echo "    docker image build -f $BUILD_FILE --build-arg SRVC_JAR=$SRVC.war --build-arg VER=$VER --build-arg GIT_COMMIT=$GIT_COMMIT -t $TAG-$TAPIS_ENV $BUILD_DIR "
docker image build -f $BUILD_FILE --build-arg SRVC_JAR=${SRVC}.jar --build-arg VER=$VER --build-arg GIT_COMMIT=$GIT_COMMIT  -t $TAG$TAPIS_ENV $BUILD_DIR

echo "    removing $BUILD_DIR/${SRVC}.jar"
rm $BUILD_DIR/${SRVC}.jar
