##########################################################################
# Dockerfile for Tapis systems service
#
#   $CATALINA_HOME  set as installation convention from tomcat image
#   $TAG            the tag for image identification
#
##########################################################################
FROM tomcat:8.5-jdk13-openjdk-oracle

ARG VER
ARG GIT_COMMIT

MAINTAINER CIC Support <cicsupport@tacc.utexas.edu>

# Create directory for the service
RUN mkdir /usr/local/tomcat/webapps/v3#systems

# add
COPY target/v3#systems /usr/local/tomcat/webapps/v3#systems

# Environment variables for testing without JWT
ENV TAPIS_ENVONLY_ALLOW_TEST_HEADER_PARMS true
ENV TAPIS_ENVONLY_JWT_OPTIONAL true
ENV TAPIS_ENVONLY_SKIP_JWT_VERIFY true
# Environment variables to turn on debugging
ENV JPDA_ADDRESS "*:8000"
# might need ENV JPDA_ADDRESS *:8000 for tomcat 9
ENV JPDA_TRANSPORT dt_socket
# Other environment variables
ENV TAPIS_REQUEST_LOGGING_FILTER_PREFIXES /v3/systems

# Debug port and http port
EXPOSE 8000
EXPOSE 8080

# Launch Tomcat on startup
# Run with debugger off
# CMD ["catalina.sh", "run"]
# Run with debugger on
CMD ["catalina.sh", "jpda", "run"]
