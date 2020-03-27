#!/usr/bin/env bash -x

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
export VER=${TAPIS_VERSION}
export TAPIS_ENV=${TAPIS_ENV}
export SRVC=meta
export SRVC_API=${SRVC}api
export TAPIS_ROOT=$(pwd)

export SRVC_DIR="${TAPIS_ROOT}/tapis-${SRVC_API}/target"
export TAG="tapis/${SRVC_API}:$VER"
export IMAGE_BUILD_DIR="$TAPIS_ROOT/deployment/tapis-${SRVC_API}"
export BUILD_FILE="$IMAGE_BUILD_DIR/Dockerfile"
export GIT_COMMIT=${GIT_COMMIT}
export WAR_NAME=v3#meta    # matches final name in pom file

# See if we can determine the git commit if it's not already set.
# Basically, we take the second word in the git.info file.
if [ -z "${GIT_COMMIT}" ]
then
    echo   "***  export GIT_COMMIT=$(awk '{print $2}' ${SRVC_DIR}/${WAR_NAME}/WEB-INF/classes/git.info)"
    export GIT_COMMIT="$(awk '{print $2}' ${SRVC_DIR}/${WAR_NAME}/WEB-INF/classes/git.info)"
fi

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

# echo " ***   do a build on metaapi  "
# echo " ***   mvn clean install -rf :tapis-metaapi "
# mvn clean install -rf :tapis-metaapi

echo "";echo ""

echo "***      removing any old service war v3#meta.war file from Docker build context"
echo "***      $IMAGE_BUILD_DIR/$WAR_NAME.war "
if test -f "$IMAGE_BUILD_DIR/$WAR_NAME.war"; then
     rm $IMAGE_BUILD_DIR/$WAR_NAME.war
fi

echo "";echo ""

#echo " ***   unzip $SRVC_DIR/$WAR_NAME.war -d ${IMAGE_BUILD_DIR}/${SRVC} "
#       unzip $SRVC_DIR/$WAR_NAME.war -d ${IMAGE_BUILD_DIR}/${SRVC}

# echo "";echo ""
echo " ***   cp $SRVC_DIR/$WAR_NAME.war ${IMAGE_BUILD_DIR}/ "
             cp $SRVC_DIR/$WAR_NAME.war ${IMAGE_BUILD_DIR}/

echo "";echo ""

echo " ***   jump to the deployment build directory "
echo " ***   cd ${IMAGE_BUILD_DIR}"
             cd ${IMAGE_BUILD_DIR}
echo "";echo ""

echo "***      building the docker image from deployment/tapis-${SRVC_API}/Dockerfile"
echo "***      docker image build --build-arg VER=0.0.1 --build-arg GIT_COMMIT=$GIT_COMMIT  -t $TAG-$TAPIS_ENV . "
               docker image build --build-arg VER=0.0.1 --build-arg GIT_COMMIT=$GIT_COMMIT  -t $TAG-$TAPIS_ENV .

echo "";echo ""

echo "***      rm ${IMAGE_BUILD_DIR}/${WAR_NAME}.war"
               rm ${IMAGE_BUILD_DIR}/${WAR_NAME}.war

