##########################################################################
# tapis image for securityapi services
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
FROM tomcat:9.0.27-jdk13-openjdk-oracle

ARG VER
ARG SRVC_ROOT
ARG GIT_COMMIT

MAINTAINER CIC Support <cicsupport@tacc.utexas.edu>

# make security webapp subdirectory.
RUN mkdir /usr/local/tomcat/webapps/v3

# add
COPY $SRVC_ROOT /usr/local/tomcat/webapps/v3

# Set environment and turn on remote debug
ENV CATALINA_HOME /usr/local/tomcat

# Set most Java options here.
ENV CATALINA_OPTS -Xms6g -Xmx6g

# Debug port and http port
EXPOSE 8000
EXPOSE 8080

LABEL "GIT_COMMIT"=$GIT_COMMIT

# Launch Tomcat on startup
CMD ${CATALINA_HOME}/bin/catalina.sh run


