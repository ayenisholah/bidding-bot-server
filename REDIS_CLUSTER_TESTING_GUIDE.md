# Redis Cluster + Multi-Worker Testing Guide

## Overview

This guide covers testing the Redis Cluster migration and multi-worker architecture changes.

**Changes Summary:**
- Redis: Single instance → 3-node cluster (dev) / 6-node cluster (production)
- Workers: 1 main + 1 counterbid → 2 main + 2 counterbid
- Processing capacity: 128 concurrent jobs → 256 concurrent jobs
- Queue prefixes: Added hash tags `{main-queue}` and `{counterbid-queue}`

---

## Phase 1: Local Development Testing

### Prerequisites
```bash
cd /home/titan/projects/nfttools/bidding-bot-server
npm run build  # Should complete with zero errors ✅
```

### 1.1 Start Development Cluster

```bash
# Clean up any existing containers
docker compose -f compose.dev.yaml down -v

# Start the 3-node Redis Cluster
docker compose -f compose.dev.yaml up -d

# Watch cluster initialization
docker logs redis-cluster-init -f
# Expected: "Redis Cluster initialized successfully!"
```

### 1.2 Verify Cluster Formation

```bash
# Check cluster state
docker exec redis-node-1 redis-cli -p 7000 cluster info

# Expected output:
# cluster_state:ok
# cluster_slots_assigned:16384
# cluster_known_nodes:3

# View cluster topology
docker exec redis-node-1 redis-cli -p 7000 cluster nodes

# Expected: 3 master nodes with hash slot assignments
```

### 1.3 Test Single Instance Fallback

```bash
# Test without REDIS_NODES (should use localhost:6379 fallback)
unset REDIS_NODES
npm start

# Server should start with logs showing:
# "[Redis] Single Instance Configuration: host=localhost, port=6379"
```

### 1.4 Run with Cluster Configuration

```bash
# Set cluster nodes
export REDIS_NODES='[{"host":"localhost","port":7000},{"host":"localhost","port":7001},{"host":"localhost","port":7002}]'

# Start server
npm start
```

**Expected Server Logs:**
```
[Redis] Cluster Configuration: 3 nodes | env=development
[Redis] Created cluster connection main-worker-0 with 3 nodes
[Redis] Created cluster connection main-worker-1 with 3 nodes
[Redis] Created cluster connection counterbid-worker-0 with 3 nodes
[Redis] Created cluster connection counterbid-worker-1 with 3 nodes
[Worker] Main worker 0 initialized | concurrency=64 | queue={main-queue}BIDDING_BOT
[Worker] Main worker 1 initialized | concurrency=64 | queue={main-queue}BIDDING_BOT
[Worker] All 2 main workers initialized | total concurrency=128
[Worker] Counterbid worker 0 initialized | concurrency=64 (dynamic) | queue={counterbid-queue}COUNTERBID_QUEUE
[Worker] Counterbid worker 1 initialized | concurrency=64 (dynamic) | queue={counterbid-queue}COUNTERBID_QUEUE
[Worker] All 2 counterbid workers initialized | total concurrency=128
[ConcurrencyManager] Workers set - ready for dynamic allocation | main=2 | counterbid=2
[MemorySnapshot] Initialized with Redis and BullMQ queues | mainWorkers=2 | counterbidWorkers=2
```

### 1.5 Verify Worker Status via API

```bash
# Check worker health
curl http://localhost:3003/api/health/workers | jq

# Expected response:
{
  "status": "healthy",
  "workers": {
    "total": 4,
    "main": 2,
    "counterbid": 2,
    "healthy": 4,
    "details": [
      {"index": 0, "name": "main-worker-0", "running": true, "concurrency": 64, "healthy": true},
      {"index": 1, "name": "main-worker-1", "running": true, "concurrency": 64, "healthy": true},
      {"index": 2, "name": "counterbid-worker-0", "running": true, "concurrency": 64, "healthy": true},
      {"index": 3, "name": "counterbid-worker-1", "running": true, "concurrency": 64, "healthy": true}
    ]
  }
}
```

```bash
# Check metrics endpoint
curl http://localhost:3003/metrics | jq '.queues'

# Expected output:
{
  "main": {
    "name": "{main-queue}BIDDING_BOT",
    "concurrency": 128,  # 64 * 2 workers
    "workers": 2,
    ...
  },
  "counterbid": {
    "name": "{counterbid-queue}COUNTERBID_QUEUE",
    "concurrency": 128,  # 64 * 2 workers
    "workers": 2,
    ...
  }
}
```

### 1.6 Test Job Distribution

```bash
# Create a test task and observe job distribution
# Jobs should be distributed across Redis cluster nodes

# Check job distribution across nodes
docker exec redis-node-1 redis-cli -p 7000 dbsize
docker exec redis-node-2 redis-cli -p 7001 dbsize
docker exec redis-node-3 redis-cli -p 7002 dbsize

# All should have non-zero keys if tasks are running
```

### 1.7 Test Concurrency Allocation

```bash
# Monitor concurrency manager logs
tail -f debug-logs/system-*.log | grep "ConcurrencyAlloc"

# Expected patterns:
# Active=0, CB=128 (fixed, 2 workers), Main=128 (dynamic, 2 workers), Total=256
# Active=10, CB=128 (fixed, 2 workers), Main=115 (dynamic, 2 workers), Total=243
```

---

## Phase 2: Production Deployment Testing

### 2.1 Deploy to VPS

```bash
cd /home/titan/projects/nfttools/bidding-bot-installation

# Stop existing services
docker compose down

# Pull latest images (if using Docker Hub)
docker compose pull

# Start with 6-node cluster
docker compose up -d

# Watch cluster initialization
docker logs redis-cluster-init -f
# Expected: "Production Redis Cluster initialized!"
```

### 2.2 Verify Production Cluster Health

```bash
# Check master nodes
for port in 6379 6380 6381; do
  echo "=== Master on port $port ==="
  docker exec redis-master-1 redis-cli -p $port cluster info | grep cluster_state
  docker exec redis-master-1 redis-cli -p $port cluster nodes | grep master
done

# Expected output for each:
# cluster_state:ok
# <node-id> <ip>:<port> master - ...

# Check replica assignments
docker exec redis-master-1 redis-cli -p 6379 cluster nodes

# Expected: 3 masters + 3 replicas, each master has 1 replica
```

### 2.3 Test Automatic Failover

```bash
# Stop a master node (simulate failure)
docker stop redis-master-2

# Wait 10 seconds for failover
sleep 10

# Check cluster still healthy (replica should be promoted)
docker exec redis-master-1 redis-cli -p 6379 cluster info
# Expected: cluster_state:ok

docker exec redis-master-1 redis-cli -p 6379 cluster nodes | grep master
# Expected: 3 masters (one is the promoted replica on port 6383)

# Restart the stopped node
docker start redis-master-2

# Node should rejoin as replica
sleep 10
docker exec redis-master-1 redis-cli -p 6379 cluster nodes
```

### 2.4 Monitor Server Health

```bash
# Check server logs for errors
docker logs -f bidding-bot-server-beta-single | grep -i "error\|redis\|worker"

# Should see no Redis connection errors
# Should see all 4 workers healthy

# Check API health
curl http://localhost:3003/api/health/workers
curl http://localhost:3003/metrics
```

### 2.5 Load Testing (24-hour monitoring)

**Metrics to Monitor:**
1. **Memory usage**: Should remain stable ~5-6GB
2. **Worker health**: All 4 workers should stay healthy
3. **Queue depth**: Should process jobs efficiently (not accumulate)
4. **Redis connections**: Should maintain 9 connections (3 per queue type + events)
5. **Error rates**: Should not increase vs baseline

```bash
# Monitor memory usage
watch -n 60 'docker stats --no-stream | grep bidding-bot-server'

# Monitor worker health
watch -n 60 'curl -s http://localhost:3003/api/health/workers | jq ".workers.healthy"'

# Monitor queue depth
watch -n 60 'curl -s http://localhost:3003/metrics | jq ".queues.totals.waiting"'

# Check for Redis errors in logs
docker logs bidding-bot-server-beta-single --since 1h | grep -i "redis.*error"
```

---

## Phase 3: Functional Testing

### 3.1 Task Lifecycle Testing

**Test Scenario: Create → Start → Stop → Delete**

1. Create a new task via UI
2. Start the task
3. Verify jobs are being processed:
   ```bash
   curl http://localhost:3003/metrics | jq '.queues.main.active'
   # Should show non-zero active jobs
   ```
4. Stop the task
5. Verify jobs drain:
   ```bash
   curl http://localhost:3003/metrics | jq '.queues.main.waiting'
   # Should decrease to 0
   ```
6. Delete the task

**Expected Behavior:**
- No worker errors during lifecycle
- Clean pause/resume operations
- No orphaned jobs in Redis

### 3.2 Concurrency Adjustment Testing

**Test Scenario: Monitor dynamic concurrency allocation**

1. Start 5 tasks (no counterbids initially)
   ```bash
   # Should see: CB=128, Main=128, Total=256
   ```

2. Trigger counterbids (manually or wait for real ones)
   ```bash
   # As counterbid jobs increase:
   # 10 jobs: CB=128, Main=115, Total=243
   # 30 jobs: CB=128, Main=90, Total=218
   # 64 jobs: CB=128, Main=1, Total=129
   ```

3. Monitor logs:
   ```bash
   tail -f debug-logs/system-*.log | grep "ConcurrencyAlloc"
   ```

**Expected Behavior:**
- Counterbid concurrency stays fixed at 128
- Main concurrency decreases proportionally
- No concurrency allocation errors

### 3.3 Error Recovery Testing

**Test Scenario: Simulate worker failure**

1. Force a worker error (e.g., invalid job data)
2. Verify worker health recovery
3. Check health endpoint shows unhealthy worker
4. Verify automatic recovery attempts

**Expected Behavior:**
- Worker attempts reconnection
- Health monitoring detects failure
- System continues operating with remaining workers

---

## Phase 4: Performance Validation

### 4.1 Throughput Testing

**Baseline Comparison:**
- Before: 64 main + 64 counterbid = 128 concurrent
- After: 128 main + 128 counterbid = 256 concurrent

**Test:**
1. Create 10 high-frequency tasks
2. Monitor processing rate:
   ```bash
   curl http://localhost:3003/metrics | jq '.queues.processingRate'
   ```

**Expected Result:**
- Processing rate should be ~2x baseline (if not bottlenecked by API rate limits)

### 4.2 Latency Testing

**Lock Operations:**
- Before: 10-50ms (Redis-based)
- After: <1ms (LRU-based, from previous migration)

**Job Processing:**
- Should remain similar (~700-850ms P50 for counterbids)
- No degradation from cluster overhead

### 4.3 Memory Stability

**Monitor for 48 hours:**
```bash
# Every hour, check memory usage
docker stats --no-stream | grep bidding-bot-server

# Expected: Stable around 5-6GB, no continuous growth
```

---

## Success Criteria

### ✅ Must Pass:
1. **Compilation**: `npm run build` succeeds with zero errors
2. **Cluster Formation**: All nodes show `cluster_state:ok`
3. **Worker Initialization**: All 4 workers start healthy
4. **API Health**: `/api/health/workers` shows all workers healthy
5. **Job Processing**: Tasks execute successfully
6. **Concurrency Allocation**: Dynamic allocation works correctly
7. **Failover**: Cluster remains healthy when master node stops
8. **Memory Stability**: No memory leaks over 48 hours

### ⚠️ Warning Signs:
- Redis connection errors in logs
- Workers repeatedly restarting
- Queue depth continuously growing
- Memory usage increasing without bound
- Failover not completing within 10 seconds

### 🚨 Failure Conditions:
- Server won't start
- Workers not initializing
- Cluster not forming (stays in `cluster_state:fail`)
- Job processing completely stops
- Memory usage exceeds 8GB

---

## Troubleshooting

### Issue: Cluster won't form
```bash
# Check if ports are already in use
netstat -tulpn | grep -E "6379|6380|6381|7000|7001|7002"

# Clear old cluster data
docker compose down -v
docker compose up -d
```

### Issue: Workers not starting
```bash
# Check REDIS_NODES format (must be valid JSON)
echo $REDIS_NODES | jq .

# Verify Redis connectivity from server container
docker exec bidding-bot-server-beta-single redis-cli -h redis-master-1 -p 6379 ping
```

### Issue: High memory usage
```bash
# Check Redis memory
docker exec redis-master-1 redis-cli -p 6379 info memory | grep used_memory_human

# Check worker concurrency (shouldn't exceed RATE_LIMIT * 2 * 2)
curl http://localhost:3003/metrics | jq '.workers.total'
```

### Issue: Jobs not processing
```bash
# Check BullMQ connections
curl http://localhost:3003/api/health/redis | jq

# Check worker status
curl http://localhost:3003/api/health/workers | jq '.workers.details'

# Check queue depths
curl http://localhost:3003/metrics | jq '.queues.totals'
```

---

## Rollback Procedure

If critical issues occur:

### Option 1: Keep code, revert to single instance
```bash
# Remove REDIS_NODES env var
unset REDIS_NODES

# Use single Redis instance
export REDIS_HOST=localhost
export REDIS_PORT=6379

# Restart server
npm run build
npm start

# Code will auto-detect single instance mode
```

### Option 2: Full rollback
```bash
# Revert the commit
git log --oneline | head -5  # Find the commit hash
git revert <commit-hash>

# Rebuild
npm run build

# Restart services
docker compose down
docker compose up -d
```

---

## Post-Deployment Checklist

- [ ] All 4 workers healthy
- [ ] Redis cluster shows 3 masters (+ 3 replicas in prod)
- [ ] No Redis errors in logs (first hour)
- [ ] Queue processing normally
- [ ] Concurrency allocation working
- [ ] Memory usage stable (<6GB)
- [ ] API endpoints responding correctly
- [ ] Metrics showing correct worker counts
- [ ] Failover tested (production only)
- [ ] 24-hour monitoring scheduled
- [ ] Team notified of changes
- [ ] Documentation updated

---

## Additional Resources

**Redis Cluster Documentation:**
- https://redis.io/docs/management/scaling/

**BullMQ with Redis Cluster:**
- https://docs.bullmq.io/guide/connections

**Monitoring Commands:**
```bash
# Cluster health
redis-cli --cluster check <host>:<port>

# Cluster resharding (if needed)
redis-cli --cluster rebalance <host>:<port>

# View cluster info
redis-cli -c -h <host> -p <port> cluster info
redis-cli -c -h <host> -p <port> cluster nodes
```
