#!/bin/sh
# Use a postgres docker image to run a script that uses psql
PrgName=$(basename "$0")

# Determine absolute path to location from which we are running.
export RUN_DIR=$(pwd)
export PRG_RELPATH=$(dirname "$0")
cd "$PRG_RELPATH"/. || exit
export PRG_PATH=$(pwd)

# Running with network=host exposes ports directly. Only works for linux
docker run -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" -i --rm --network="host" bitnami/postgresql:latest /bin/bash < create_db.sh
