#!/bin/bash

VOLUMES_DIR="./docker/Volumes"

# Create the Postgres data volume directory if it doesn't exist
if [ ! -d "$VOLUMES_DIR/postgres-db-volume" ]; then
  echo "Creating Postgres data volume directory..."
  mkdir -p "$VOLUMES_DIR/postgres-db-volume"
  echo "Postgres data volume directory created at $VOLUMES_DIR/postgres-db-volume."
fi

# Create necessary Airflow directories if they don't exist
AIRFLOW_DIRS=(dags logs config plugins)
for DIR in "${AIRFLOW_DIRS[@]}"; do
  if [ ! -d "$DIR" ]; then
    echo "Creating $DIR directory..."
    mkdir -p "$DIR"
  fi
done

# Ensure requirements.txt exists
if [ ! -f "./requirements.txt" ]; then
    echo "Error: requirements.txt not found in the current directory. Exiting."
    exit 1
fi

# Build and start the containers
echo "Starting Docker Compose setup..."
docker compose -f docker/docker-compose.yaml build
docker compose -f docker/docker-compose.yaml up -d

echo "Docker Compose setup is running."
