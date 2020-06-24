#!/usr/bin/env bash

###########################################################
#  This script helps build images for service specified
#  It relies on Docker 18.06.0-ce and acts as a template
#  for future Tapis services and building an image from a
#  set of maven artifacts.
#
# environment : TAPIS_VERSION set to the version in tapis/pom.xml 
#
# usage : $TAPIS_ROOT/deployment/build-metaapi.sh
#
###########################################################
export VER=$TAPIS_VERSION
export TAPIS_ENV=dev
export SRVC=meta
export SRVC_API=${SRVC}api
export TAPIS_ROOT=$(pwd)
export SRVC_DIR="${TAPIS_ROOT}/tapis-${SRVC_API}/target"
export TAG="tapis/${SRVC_API}:$VER"
export IMAGE_BUILD_DIR="$TAPIS_ROOT/deployment/tapis-${SRVC_API}"
export BUILD_FILE="$IMAGE_BUILD_DIR/Dockerfile"
export GIT_COMMIT=$(git log -1 --pretty=format:"%h")
export WAR_NAME=meta    # matches final name in pom file

echo "VER: $VER"
echo "TAPIS_ENV: $TAPIS_ENV"
echo "SRVC: $SRVC"
echo "SRVC_API: $SRVC_API"
echo "TAPIS_ROOT: $TAPIS_ROOT"
echo "SRVC_DIR: $SRVC_DIR"
echo "TAG: $TAG"
echo "IMAGE_BUILD_DIR: $IMAGE_BUILD_DIR"
echo "BUILD_FILE: $BUILD_FILE"
echo "GIT_COMMIT: $GIT_COMMIT"
echo "WAR_NAME: $WAR_NAME"
echo ""

# cd tapis-metaapi
# echo " ***   do a build on metaapi  "
# echo " ***   mvn clean install -DskipTests"
# mvn clean install -DskipTests

echo "";echo ""

# cd ..  # jump back up to project root directory

echo "***      removing any old service war meta directory from Docker build context"
echo "***      $IMAGE_BUILD_DIR/$WAR_NAME "
if test -d "$IMAGE_BUILD_DIR/$WAR_NAME"; then
     rm -rf $IMAGE_BUILD_DIR/$WAR_NAME
fi

echo "";echo ""

echo "***          copy the new service package directory to our docker build directory "
echo "***   cp -r $SRVC_DIR/$WAR_NAME ${IMAGE_BUILD_DIR}/ "
            cp -r $SRVC_DIR/$WAR_NAME ${IMAGE_BUILD_DIR}/

echo "";echo ""

echo " ***   jump to the deployment build directory "
echo " ***   cd ${IMAGE_BUILD_DIR}"
             cd ${IMAGE_BUILD_DIR}
echo "";echo ""

echo "***      building the docker image from deployment directory docker build tapis-${SRVC_API}/Dockerfile"
echo "***      docker image build --build-arg VER=$VER --build-arg GIT_COMMIT=$GIT_COMMIT  -t $TAG-$TAPIS_ENV . "
               docker image build --build-arg VER=$VER --build-arg GIT_COMMIT=$GIT_COMMIT  -t $TAG-$TAPIS_ENV .
echo "";echo ""

echo "***    push the image to docker hub "
echo "***      export META_IMAGE=$TAG-$TAPIS_ENV"
#               docker push "$TAG-$TAPIS_ENV"


echo "***      "
echo "***      rm -rf ${IMAGE_BUILD_DIR}/${WAR_NAME}"
             #  rm -rf ${IMAGE_BUILD_DIR}/${WAR_NAME}

