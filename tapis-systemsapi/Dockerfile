##########################################################################
# Dockerfile for Tapis systems service
#
#   $TAG            the tag for image identification
#
##########################################################################
FROM tomcat:8.5-jdk13-openjdk-oracle

ARG VER
ARG GIT_COMMIT

MAINTAINER CIC Support <cicsupport@tacc.utexas.edu>

WORKDIR /app

# Copy jar file into place
COPY target/v3#systems.jar .


# Environment variables for testing without JWT
ENV TAPIS_ENVONLY_ALLOW_TEST_HEADER_PARMS true
ENV TAPIS_ENVONLY_JWT_OPTIONAL true
ENV TAPIS_ENVONLY_SKIP_JWT_VERIFY true

# Other environment variables
ENV TAPIS_REQUEST_LOGGING_FILTER_PREFIXES /v3/systems

# Options for JVM
ENV JAVA_TOOL_OPTIONS -agentlib:jdwp=transport=dt_socket,address=8000,server=y,suspend=n \
                      -Djava.rmi.server.hostname=0.0.0.0 -Dcom.sun.management.jmxremote \
                      -Dcom.sun.management.jmxremote.port=9090  -Dcom.sun.management.jmxremote.rmi.port=9090 \
                      -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false \
                      -Dcom.sun.management.jmxremote.ssl=false 

# TODO turn on debuggin
# Server port, debug port and jmx port
EXPOSE 8080
EXPOSE 8000
EXPOSE 9090

CMD ["java", "-Xms4g", "-Xmx4g", "-jar", "v3#systems.jar"]
