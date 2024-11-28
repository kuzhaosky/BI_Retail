#!/bin/bash

set -e

# Get the script's directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

VOLUMES_DIR="$PROJECT_ROOT/docker/Volumes"

# Stop and remove Docker containers
echo "Stopping Docker Compose setup..."
cd "$PROJECT_ROOT/docker"
docker compose down --volumes
# Remove the Volumes directory if it exists
if [ -d "$VOLUMES_DIR" ]; then
  echo "Removing Volumes directory at $VOLUMES_DIR..."
  rm -rf "$VOLUMES_DIR"
  echo "Volumes directory removed."
else
  echo "Volumes directory does not exist. Skipping removal."
fi

echo "Docker Compose setup stopped and cleaned up."
