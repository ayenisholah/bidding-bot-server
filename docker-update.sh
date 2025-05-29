#!/bin/bash

# This script is included in the Docker image and can be triggered by the application
# It handles the update process from within the container

set -e

LOG_FILE="/tmp/update.log"
COMPOSE_FILE="/app/compose.yaml"

# Ensure log directory exists and create temp log
touch "$LOG_FILE" || true

echo "[$(date)] ========== Starting Docker Update Process ==========" >> "$LOG_FILE"

# Function to log messages
log() {
    echo "[$(date)] $1" | tee -a "$LOG_FILE"
}

# Check if we have Docker access
if ! docker version > /dev/null 2>&1; then
    log "ERROR: Docker is not accessible from within the container"
    log "Ensure Docker socket is mounted and user has permissions"
    exit 1
fi

# Check if compose file exists
if [ ! -f "$COMPOSE_FILE" ]; then
    log "ERROR: Compose file not found at $COMPOSE_FILE"
    exit 1
fi

# Pull latest images first
log "Pulling latest images..."
if docker compose -f "$COMPOSE_FILE" pull >> "$LOG_FILE" 2>&1; then
    log "Successfully pulled latest images"
else
    log "ERROR: Failed to pull images"
    exit 1
fi

# First, we need to stop and remove containers with fixed names
log "Stopping and removing existing containers..."

# Get all running containers from this compose project
COMPOSE_CONTAINERS=$(docker compose -f "$COMPOSE_FILE" ps -q 2>/dev/null || true)

# Stop all containers
if [ ! -z "$COMPOSE_CONTAINERS" ]; then
    log "Stopping compose containers..."
    docker compose -f "$COMPOSE_FILE" stop >> "$LOG_FILE" 2>&1 || log "Warning: Some containers may not have stopped"
fi

# Remove containers with fixed names to avoid conflicts
log "Removing containers with fixed names..."
FIXED_NAME_CONTAINERS="mongodb_container redis_node_1 redis_node_2 redis_node_3 redis_node_4 redis_node_5 redis_node_6"
for container in $FIXED_NAME_CONTAINERS; do
    if docker ps -a --format "{{.Names}}" | grep -q "^${container}$"; then
        docker rm -f "$container" >> "$LOG_FILE" 2>&1 || log "Warning: Failed to remove $container"
    fi
done

# Also remove any compose-created containers
docker compose -f "$COMPOSE_FILE" rm -f >> "$LOG_FILE" 2>&1 || true

# Now start all containers with new images
log "Starting all containers with new images..."
if docker compose -f "$COMPOSE_FILE" up -d >> "$LOG_FILE" 2>&1; then
    log "Successfully started all containers with latest images"
    log "Update completed successfully"
    log "Note: The server container running this script will be recreated shortly"
else
    log "ERROR: Failed to start containers"
    exit 1
fi

# Clean up old images
log "Cleaning up old images..."
docker image prune -f >> "$LOG_FILE" 2>&1 || log "Warning: Failed to prune old images"

log "========== Update Process Completed Successfully =========="
echo "Update completed successfully"