#!/bin/bash

VOLUMES_DIR="./docker/Volumes"

# Stop and remove Docker containers
echo "Stopping Docker Compose setup..."
docker compose -f docker/docker-compose.yaml down --volumes

# Remove the Volumes directory if it exists
if [ -d "$VOLUMES_DIR" ]; then
  echo "Removing Volumes directory..."
  sudo rm -rf "$VOLUMES_DIR"
  echo "Volumes directory removed."
else
  echo "Volumes directory does not exist. Skipping removal."
fi

echo "Docker Compose setup stopped and cleaned up."
