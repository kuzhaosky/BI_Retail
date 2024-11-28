#!/bin/bash

# Exit on error
set -e

# Get the script's directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

# Define the volumes directory
VOLUMES_DIR="$PROJECT_ROOT/docker/Volumes"

# Create the Postgres data volume directory if it doesn't exist
if [ ! -d "$VOLUMES_DIR/postgres-db-volume" ]; then
  echo "Creating Postgres data volume directory..."
  mkdir -p "$VOLUMES_DIR/postgres-db-volume"
  echo "Postgres data volume directory created at $VOLUMES_DIR/postgres-db-volume."
fi

# Create necessary Airflow directories if they don't exist
for DIR in dags logs plugins; do
  if [ ! -d "$PROJECT_ROOT/$DIR" ]; then
    echo "Creating $DIR directory..."
    mkdir -p "$PROJECT_ROOT/$DIR"
  fi
done

# Ensure requirements.txt exists in the docker directory
if [ ! -f "$PROJECT_ROOT/requirements.txt" ]; then
  echo "Error: requirements.txt not found in the docker directory. Exiting."
  exit 1
fi

# Build and start the containers
echo "Starting Docker Compose setup..."
cd "$PROJECT_ROOT/docker"
docker compose build
docker compose up -d

echo "Docker Compose setup is running."
