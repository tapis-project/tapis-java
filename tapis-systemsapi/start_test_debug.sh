# For some reason env vars not getting picked up when running
#    an embedded grizzly server using a fat jar.
# So use commands under ~/dev/tapisv3 to do curl and use a JWT instead
export TAPIS_ENVONLY_ALLOW_TEST_HEADER_PARM=true
export TAPIS_ENVONLY_JWT_OPTIONAL=true
export TAPIS_ENVONLY_SKIP_JWT_VERIFY=true
export TAPIS_ENVONLY_KEYSTORE_PASSWORD=password
export TAPIS_REQUEST_LOGGING_FILTER_PREFIXES=/v3/systems
export TAPIS_SVC_URL_TOKENS=https://dev.develop.tapis.io
export TAPIS_SVC_URL_TENANTS=https://dev.develop.tapis.io

set -xv
java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8081 \
     -jar target/v3#systems.jar
