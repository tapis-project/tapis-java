#!/bin/sh
# Use mvn to run the tapis-systemslib integration tests.
# The argument p6spy.config.modulelist= turns off debug logging of SQL calls.
#   Simply remove the argument to see SQL calls
#
# To run against DEV environment services (tenants, tokens) and a local DB
#   with default DB username and password as set in service.properties then
#   set the following:
#     TAPIS_TENANT_SVC_BASEURL=https://master.develop.tapis.io
#     TAPIS_SERVICE_PASSWORD=****
#
# In general the following env variables should be set prior to running this script:
#   TAPIS_DB_USER
#   TAPIS_DB_PASSWORD
#   TAPIS_DB_JDBC_URL
#   TAPIS_TENANT_SVC_BASEURL
#   TAPIS_SERVICE_PASSWORD
# For example:
#   TAPIS_DB_USER=tapis
#   TAPIS_DB_PASSWORD=******
#   TAPIS_DB_JDBC_URL=jdbc:postgresql://localhost:5432/tapissysdb
#   TAPIS_TENANT_SVC_BASEURL=https://master.develop.tapis.io
#   TAPIS_SERVICE_PASSWORD=****
#
# The following env variables must be set
#   TAPIS_SERVICE_PASSWORD
#
# If env variables are not set then these defaults will be used:
# (see service.properties for defaults)
#   TAPIS_TENANT_SVC_BASEURL=https://localhost
#   TAPIS_DB_USER=tapis
#   TAPIS_DB_PASSWORD=******
#   TAPIS_DB_JDBC_URL=jdbc:postgresql://localhost:5432/tapissysdb

# Determine absolute path to location from which we are running
#  and change to that directory
export RUN_DIR=$(pwd)
export PRG_RELPATH=$(dirname "$0")
cd "$PRG_RELPATH"/. || exit
export PRG_PATH=$(pwd)

cd ../../tapis-systemslib
mvn verify -DskipIntegrationTests=false -Dp6spy.config.modulelist=
