#/bin/bash
# Create database if it does not exist by running a psql command
DB_NAME=tapissysdb
echo "SELECT 'CREATE DATABASE ${DB_NAME} ENCODING=\"UTF8\" LC_COLLATE=\"en_US.utf8\" LC_CTYPE=\"en_US.utf8\" ' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${DB_NAME}')\gexec" \
  | PGPASSWORD=${POSTGRES_PASSWORD} psql --host=localhost --username=postgres
