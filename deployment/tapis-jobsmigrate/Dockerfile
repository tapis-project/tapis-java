##########################################################################
# tapis image for jobsmigrate services
#
# usage:
#   docker build -f $BUILD_FILE --build-arg SRVC_WAR=jobs.war --build-arg VER=0.1 --build-arg GIT_COMMIT=1234 -t $TAG .
#
#   ${pwd}          the root of the git repository working directory.
#   $SRVC_WAR       the aloe service api being installed as a war file from target
#                     directory   ex. target/jobs.war
#   $CATALINA_HOME  set as installation convention from tomcat image
#   $TAG            the tag for image identification
#
#   Tested with Docker version 18.06.0-ce
##########################################################################
FROM openjdk:17-jdk-bullseye

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
COPY $SRVC_JAR /usr/local/bin/jobsmigrate.jar

# Launch the migration script
CMD java -jar /usr/local/bin/jobsmigrate.jar -h $HOST -p $PORT -u $USER -pw $PW -tpw $TPW $FLAGS 
