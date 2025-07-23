#!/bin/bash

echo "=== Switching to 3-node Redis cluster ==="

# Stop current services
echo "Stopping current services..."
docker compose -f compose.dev.optimized.yaml down -v

# Clean up any leftover containers
docker rm -f redis_node_1 redis_node_2 redis_node_3 redis_node_4 redis_node_5 redis_node_6 2>/dev/null

# Start 3-node cluster
echo "Starting 3-node cluster..."
docker compose -f compose.dev.3node.yaml up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 10

# Check cluster status
echo ""
echo "=== Cluster Status ==="
docker exec redis_node_1 redis-cli cluster info | head -5

echo ""
echo "=== Container Stats ==="
docker stats --no-stream

echo ""
echo "=== Next Steps ==="
echo "1. Copy your .env file to .env.3node and update REDIS_NODES"
echo "2. Update your start command to use .env.3node"
echo "3. Apply CPU optimization fixes from CPU_OPTIMIZATION_FIXES.md"
echo ""
echo "To start the server with 3-node config:"
echo "  cp .env .env.backup"
echo "  cp .env.3node .env"
echo "  npm run dev:3node"