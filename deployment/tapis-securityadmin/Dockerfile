##########################################################################
# tapis image for securitymigrate services
#
# usage:
#   docker image build -f $BUILD_FILE --build-arg SRVC_JAR=shaded-securitylib.jar --build-arg VER=0.0.1 --build-arg GIT_COMMIT=1234 -t $TAG .
#
#   $SKADMIN_PARMS  the command line parameters passed into SkAdmin
#
#   In addtion to the SKADMIN_PARMS environment variable, a JWT or Kubernetes token environment variable may
#   also need to be set depending on the action to be performed by SkAdmin.
#
#   Tested with Docker version 18.06.0-ce
##########################################################################
FROM openjdk:15-jdk-oracle

ARG VER
ARG SRVC_JAR
ARG GIT_COMMIT
ARG BUILD_TIME

MAINTAINER CIC Support <cicsupport@tacc.utexas.edu>

# Set commit info
LABEL "GIT_COMMIT"=$GIT_COMMIT
LABEL "VERSION"=$VER
LABEL "BUILD_TIME"=$BUILD_TIME

# add
COPY shaded-securitylib.jar /usr/local/bin/shaded-securitylib.jar

# Launch the migration script. The tls directive is to avoid a jvm bug involving certificates.
CMD java -Djdk.tls.client.protocols=TLSv1.2 -cp /usr/local/bin/shaded-securitylib.jar edu.utexas.tacc.tapis.security.commands.SkAdmin $SKADMIN_PARMS 
