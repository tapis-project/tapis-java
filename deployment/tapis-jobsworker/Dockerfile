##########################################################################
# tapis image for jobs workers
#
# usage:
#   deployment/build-jobsworker.sh
#
# These values are passed in as environment variables on docker run.  The
# MAIN_CLASS variable is required as it determines what program runs.  The
# JOBS_PARMS will differ based on the program being run.
#
#   $MAIN_CLASS     the Java class whose main() method will be invoked
#   $JOBS_PARMS     the command line parameters passed into the main() method
#   $JAVA_OPTS      JVM options
#
# Of course, other environment variables that aren't expected as command-line
# parameter can also be passed into docker and accessed as environment variables
# by the application.  This is a common case in Kubernetes environments.
#
# Since the same image can be used use run any program in shaded-jobslib.jar,
# the container's name should always be set on the docker run command to reflect
# the executing program.
#
# A minimal docker run command will look something like this:
#
#   docker run --name RecoveryReader -e 'MAIN_CLASS=edu.utexas.tacc.tapis.jobs.reader.RecoveryReader' tapis/jobsworker:dev
#
##########################################################################
FROM openjdk:17-jdk-bullseye

# Build-time arguments
ARG VER
ARG GIT_COMMIT
ARG BUILD_TIME

MAINTAINER CIC Support <cicsupport@tacc.utexas.edu>

# add
COPY shaded-jobslib.jar /usr/local/bin/shaded-jobslib.jar

# Set commit info
LABEL "GIT_COMMIT"=$GIT_COMMIT
LABEL "VERSION"=$VER
LABEL "BUILD_TIME"=$BUILD_TIME

# Java module commands that are always needed.
ENV JAVA_MODULE_OPTS --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.time=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED

# Launch the class and parameters specified in the environment. 
# Add the following tls directive to avoid a jvm bug involving certificates.
#  -Djdk.tls.client.protocols=TLSv1.2 
CMD java -cp /usr/local/bin/shaded-jobslib.jar ${JAVA_OPTS} ${JAVA_MODULE_OPTS} ${MAIN_CLASS} ${JOBS_PARMS} 
