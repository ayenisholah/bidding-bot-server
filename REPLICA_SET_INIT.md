# MongoDB Replica Set Initialization

## Overview

MongoDB transactions require a replica set configuration, even for single-node deployments. This is a **one-time setup** that must be performed after starting the MongoDB container for the first time.

## Why This Is Required

The "Bulletproof Task Update System" uses MongoDB transactions to ensure atomic database-memory synchronization. Without replica set configuration, you'll encounter this error:

```
MongoServerError: Transaction numbers are only allowed on a replica set member or mongos
```

## Initialization Steps

### 1. Start Docker Containers

First, ensure your Docker containers are running:

```bash
docker-compose -f compose.optimized.yaml up -d
```

### 2. Wait for MongoDB to Start

Wait approximately 10-20 seconds for MongoDB to fully initialize. You can check the status:

```bash
docker logs mongodb_container
```

Look for log messages indicating MongoDB is ready to accept connections.

### 3. Initialize the Replica Set

Run this command to initialize the replica set (one-time operation):

```bash
docker exec -it mongodb_container mongosh --eval "rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'localhost:27017'}]})"
```

**Expected output:**
```javascript
{ ok: 1 }
```

### 4. Verify Replica Set Status

Confirm the replica set is configured correctly:

```bash
docker exec -it mongodb_container mongosh --eval "rs.status()"
```

You should see output showing the replica set member status as `PRIMARY`.

### 5. Restart Server Container

After initializing the replica set, restart the server to establish fresh connections:

```bash
docker-compose -f compose.optimized.yaml restart server
```

## Troubleshooting

### Error: "already initialized"

If you see this error, the replica set is already configured. No action needed.

### Error: "replSetInitiate quorum check failed"

This usually means MongoDB hasn't fully started. Wait another 10-20 seconds and try again.

### Connection String Requirements

Ensure your MongoDB connection string includes the `replicaSet` parameter:

```bash
# Docker environment
MONGODB_URI="mongodb://mongodb:27017/BIDDING_BOT?replicaSet=rs0"

# Local development
MONGODB_URI="mongodb://localhost:27017/BIDDING_BOT?replicaSet=rs0"
```

## When to Re-run Initialization

You only need to initialize the replica set **once** per MongoDB data volume. You do NOT need to re-run initialization when:

- Restarting containers
- Updating server code
- Redeploying the application

You ONLY need to re-run initialization if:

- You delete the MongoDB data volume (`docker volume rm mongodb_data`)
- You completely rebuild the MongoDB container from scratch
- You manually reset the replica set configuration

## Quick Reference

```bash
# One-time initialization command
docker exec -it mongodb_container mongosh --eval "rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'localhost:27017'}]})"

# Verify status
docker exec -it mongodb_container mongosh --eval "rs.status()"

# Check if already initialized
docker exec -it mongodb_container mongosh --eval "rs.conf()"
```
