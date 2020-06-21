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
# Create database if it does not exist by running a psql command
echo "SELECT 'CREATE DATABASE ${DB_NAME} ENCODING=\"UTF8\" LC_COLLATE=\"en_US.utf8\" LC_CTYPE=\"en_US.utf8\" ' \
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${DB_NAME}')\gexec" \
  | PGPASSWORD=${DB_PW} psql --host=${DB_HOST} --username=${DB_USER}
EOF
