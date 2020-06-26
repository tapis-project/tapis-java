#!/bin/bash
# Script to initialize Systems service DB using psql
# Create database, user and schema
# Postgres password must be set in env var POSTGRES_PASSWORD

if [ -z "$DB_HOST" ]; then
  DB_HOST=systems-postgres
fi

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

# Run psql command to create database if it does not exist
echo "SELECT 'CREATE DATABASE ${DB_NAME} ENCODING=\"UTF8\" LC_COLLATE=\"en_US.utf8\" LC_CTYPE=\"en_US.utf8\" ' \
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${DB_NAME}')\gexec" \
  | psql --host=${DB_HOST} --username=${DB_USER}


# Run sql to create user and schema if they do not exist
psql --host=${DB_HOST} --username=${DB_USER} --dbname=${DB_NAME} -q << EOB
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
EOB
