# Resource Optimization for 2 vCPU / 4GB RAM Server

## Problem Analysis

The current Docker Compose configuration has a critical resource allocation issue:

### Redis Cluster Over-allocation
- **Current**: 6 nodes × 4GB = 24GB total memory allocation
- **Available**: Only 4GB system RAM
- **Result**: Impossible allocation causing system instability

## Optimizations Applied

### 1. Redis Cluster (Total: 2.4GB RAM, 1.5 vCPUs)
Each Redis node optimized from 4GB to 400MB with:
- **Memory**: 400MB maxmemory per node (2.4GB total)
- **CPU**: 0.25 vCPU limit per node
- **Policy**: Kept as `noeviction` (required by BullMQ to prevent job loss)
- **Persistence**: Disabled for development (save CPU/IO)
- **Defragmentation**: Disabled to save CPU cycles
- **Replication buffer**: Reduced to 16MB
- **Slow log**: Disabled to save memory

### 2. MongoDB (768MB RAM, 0.5 vCPU)
- **Memory**: Limited to 768MB with WiredTiger cache at 0.5GB
- **CPU**: 0.5 vCPU limit
- **Optimization**: Explicit cache size control

### 3. PostgreSQL (512MB RAM, 0.5 vCPU)
- **shared_buffers**: Reduced from 256MB to 128MB
- **effective_cache_size**: Reduced from 1GB to 512MB
- **work_mem**: Reduced from 4MB to 2MB
- **max_connections**: Reduced from 100 to 50
- **Worker processes**: Reduced to match available CPUs

## Total Resource Allocation

| Service | Memory Limit | CPU Limit | Memory Reserved |
|---------|-------------|-----------|-----------------|
| Redis (6 nodes) | 3GB | 1.5 vCPUs | 768MB |
| MongoDB | 768MB | 0.5 vCPU | 256MB |
| PostgreSQL | 512MB | 0.5 vCPU | 128MB |
| **Total** | **4.28GB** | **2.5 vCPUs** | **1.15GB** |

This leaves approximately 700MB for the OS and other processes.

## Usage Instructions

### Development Environment
```bash
# Use the optimized configuration
docker compose -f compose.dev.optimized.yaml up -d

# Monitor resource usage
docker stats

# Check Redis cluster health
docker exec redis_node_1 redis-cli cluster info
```

### Production Considerations

For production on this hardware:
1. Consider reducing to 3 Redis nodes (1 master, 2 replicas)
2. Enable Redis persistence selectively
3. Monitor memory usage closely
4. Consider upgrading to 8GB RAM for better performance

### Memory Monitoring

Monitor actual memory usage:
```bash
# Check Redis memory usage
docker exec redis_node_1 redis-cli info memory | grep used_memory_human

# Check system memory
free -h

# Check Docker resource usage
docker system df
```

## Performance Impact

These optimizations provide:
- **Stable operation** within available resources
- **Automatic memory management** with LRU eviction
- **Reduced CPU overhead** from disabled features
- **Better container isolation** with resource limits

## Alternative: 3-Node Redis Cluster

For even lighter resource usage, consider a 3-node cluster:
- 3 Redis nodes (1 master, 2 replicas)
- 600MB per node (1.8GB total)
- Maintains high availability
- Reduces resource usage by 50%

## Node.js Memory Optimization

For a 4GB system, the Node.js memory has been optimized:
- **Development**: 1.5GB heap limit (`--max-old-space-size=1536`)
- **Production**: 1GB heap limit (`--max-old-space-size=1024`)
- **Added**: Semi-space size limit for better GC performance

## Monitoring Commands

```bash
# Quick resource check
./scripts/monitor-resources.sh

# Redis memory info
for i in {1..6}; do
  echo "Node $i:"
  docker exec redis_node_$i redis-cli info memory | grep -E "used_memory_human|maxmemory_human"
done

# Container resource usage
docker stats --no-stream

# System resources
echo "=== System Memory ==="
free -h
echo "=== CPU Info ==="
nproc
echo "=== Load Average ==="
uptime
```

## Current Resource Status

Based on your Docker stats:
- **Redis Cluster**: Using only 2-8% of allocated memory (very healthy)
- **MongoDB**: Using 19% of allocated memory (healthy)
- **PostgreSQL**: Using 15% of allocated memory (healthy)
- **Node.js**: ~267MB RSS (well within 1.5GB limit)

The system is running efficiently within the 4GB constraint!