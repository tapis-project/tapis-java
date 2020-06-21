#!/bin/bash
# Script to drop Systems service DB using psql
# Postgres password must be set in env var POSTGRES_PASSWORD

DB_HOST=localhost
DB_USER=postgres
DB_NAME=tapissysdb

# Determine absolute path to location from which we are running.
export RUN_DIR=$(pwd)
export PRG_RELPATH=$(dirname "$0")
cd "$PRG_RELPATH"/. || exit
export PRG_PATH=$(pwd)

if [ -z "${POSTGRES_PASSWORD}" ]; then
  echo "Please set env var POSTGRES_PASSWORD before running this script"
  exit 1
fi

# Put PGPASSWORD in environment for psql to pick up
export PGPASSWORD=${POSTGRES_PASSWORD}

# Run sql to drop the DB
psql --host=${DB_HOST} --username=${DB_USER} -q << EOB
-- Drop the DB
DROP DATABASE ${DB_NAME}
EOB
