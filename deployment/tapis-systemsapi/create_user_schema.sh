#!/bin/bash
# Script to create user and schema if they do not exist.
# Postgres password must be set in env var POSTGRES_PASSWORD
PrgName=$(basename "$0")

# DB_HOST=systems-postgres
DB_HOST=localhost
DB_USER=postgres
DB_NAME=tapissysdb
DB_PW=${POSTGRES_PASSWORD}

# Determine absolute path to location from which we are running.
export RUN_DIR=`pwd`
export PRG_RELPATH=`dirname $0`
cd $PRG_RELPATH/.
export PRG_PATH=`pwd`
cd $RUN_DIR

if [ -z "${POSTGRES_PASSWORD}" ]; then
  echo "Please set env var POSTGRES_PASSWORD before running this script"
  exit 1
fi

PGPASSWORD=${DB_PW} psql --host=${DB_HOST} --username=${DB_USER} --dbname=${DB_NAME} -q << EOF
-- Create user if it does not exist
DO \$\$
BEGIN
  CREATE ROLE tapis WITH LOGIN;
  EXCEPTION WHEN DUPLICATE_OBJECT THEN
  RAISE NOTICE 'User already exists. User name: tapis';
END
\$\$;
ALTER USER tapis WITH ENCRYPTED PASSWORD '${POSTGRES_PASSWORD}';
GRANT ALL PRIVILEGES ON DATABASE tapissysdb TO tapis;

-- Create schema if it does not exist
CREATE SCHEMA IF NOT EXISTS tapis_sys AUTHORIZATION tapis;
ALTER ROLE tapis SET search_path = 'tapis_sys';
EOF
