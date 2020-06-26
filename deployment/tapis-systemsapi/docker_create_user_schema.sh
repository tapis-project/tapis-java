#!/bin/bash
# Script to create the DB by using a docker image to run psql
# Postgres password must be set in env var POSTGRES_PASSWORD
PrgName=$(basename "$0")

if [ -z "$DB_HOST" ]; then
  DB_HOST=systems-postgres
fi

DB_HOST=localhost
DB_USER=postgres
DB_NAME=tapissysdb
DB_PW=${POSTGRES_PASSWORD}

# Determine absolute path to location from which we are running.
export RUN_DIR=$(pwd)
export PRG_RELPATH=$(dirname "$0")
cd "$PRG_RELPATH"/. || exit
export PRG_PATH=$(pwd)

if [ -z "${POSTGRES_PASSWORD}" ]; then
  echo "Please set env var POSTGRES_PASSWORD before running this script"
  exit 1
fi

# Running with network=host exposes ports directly. Only works for linux
docker run -e DB_PW="${DB_PW}" -i --rm --network="host" bitnami/postgresql:latest /bin/bash << EOF
PGPASSWORD=${DB_PW} psql --host=${DB_HOST} --username=${DB_USER} --dbname=${DB_NAME} -q << EOB
-- Create user if it does not exist
DO \\$\\$
BEGIN
  CREATE ROLE tapis WITH LOGIN;
  EXCEPTION WHEN DUPLICATE_OBJECT THEN
  RAISE NOTICE 'User already exists. User name: tapis';
END
\\$\\$;
ALTER USER tapis WITH ENCRYPTED PASSWORD '${POSTGRES_PASSWORD}';
GRANT ALL PRIVILEGES ON DATABASE tapissysdb TO tapis;

-- Create schema if it does not exist
CREATE SCHEMA IF NOT EXISTS tapis_sys AUTHORIZATION tapis;
ALTER ROLE tapis SET search_path = 'tapis_sys';
EOB
EOF
