# Resource Optimization Analysis for Bidding Bot Server

## System Constraints

- **Hardware**: 2 vCPUs Intel / 4GB RAM / 40GB Storage
- **Shared Resources**: Other applications running on the same server

## Current Resource Usage Analysis

### 1. Redis Cluster (Highest Resource Consumer)

**Current Configuration (compose.dev.yaml):**

- **6 Redis nodes** (3 masters + 3 replicas)
- **Each node allocated**: 4GB max memory
- **Total potential memory**: 24GB (6 nodes × 4GB)
- **Problem**: This exceeds total system RAM (4GB) by 6x!

**Additional Resource Consumption:**

- Each Redis node runs persistence (AOF + RDB snapshots)
- Active defragmentation enabled (CPU intensive)
- Saves every 60 seconds with 10,000+ changes
- Multiple network ports per node

### 2. Database Services

**MongoDB**: Standard container, minimal configuration
**PostgreSQL**:

- Configured for 256MB shared buffers
- 1GB effective cache size
- Max 100 connections

### 3. Node.js Server

- No explicit memory limit set
- Currently using ~147MB heap (84% of allocated ~175MB)
- Creates multiple Redis connections (main + BullMQ)

## Major Issues Identified

### 1. Redis Over-Allocation

- **Critical**: 24GB Redis allocation on 4GB system
- Will cause severe swapping and system instability
- Explains timeout issues and performance problems

### 2. Excessive Persistence

- Redis saving too frequently (every 60 seconds)
- Both AOF and RDB enabled on all nodes
- High I/O load on limited system

### 3. Connection Overhead

- Multiple Redis connections per service
- Health checks every 30 seconds
- Cluster refresh operations

## Optimization Recommendations

### 1. Redis Memory Optimization

```yaml
# Reduce to 512MB per node (3GB total for 6 nodes)
--maxmemory 512mb
--maxmemory-policy noeviction  # Change from noeviction

# Reduce persistence frequency
--save 3600 1      # Save after 1 hour if 1+ changes
--save 1800 100    # Save after 30 min if 100+ changes
--save 900 10000   # Save after 15 min if 10k+ changes

# Disable AOF on replica nodes
# Only enable on master nodes
```

### 2. Reduce Active Defragmentation

```yaml
# Less aggressive defrag to save CPU
--active-defrag-threshold-lower 20
--active-defrag-threshold-upper 50
--active-defrag-cycle-min 5
--active-defrag-cycle-max 25
```

### 3. PostgreSQL Optimization

```yaml
# Reduce for limited RAM
-c shared_buffers=128MB
-c effective_cache_size=512MB
-c work_mem=2MB
-c max_connections=50
```

### 4. Node.js Optimizations

- Set explicit heap limit: `--max-old-space-size=512`
- Reduce worker concurrency
- Increase job timeouts for SCHEDULE jobs

## Recommended compose.dev.yaml Changes

### Option 1: Keep 6 Nodes (Minimal Resources)

- Set each Redis node to 512MB max memory
- Disable AOF on replicas
- Reduce persistence frequency
- Limit PostgreSQL resources

### Option 2: Reduce to 3 Nodes (Better Performance)

- 3 Redis nodes with 1GB each
- No replicas in development
- Better resource utilization
- Simpler cluster management

## Impact on Timeout Issues

The current Redis over-allocation (24GB on 4GB system) is likely causing:

1. **Memory swapping**: Extreme slowdown when Redis uses more than available RAM
2. **CPU contention**: System spending time managing swap instead of processing
3. **I/O bottleneck**: Constant disk activity from swapping + persistence
4. **Network delays**: Cluster communication affected by system load

This explains why SCHEDULE jobs are timing out - they can't complete within 30 seconds due to system resource exhaustion.

## Immediate Actions

1. **Reduce Redis memory allocation** to 512MB per node
2. **Increase SCHEDULE job timeout** to 90 seconds
3. **Disable unnecessary persistence** on replica nodes
4. **Monitor actual memory usage** after changes

## Long-term Recommendations

1. **Use external Redis cluster** for production
2. **Upgrade server RAM** to at least 8GB
3. **Separate Redis from application server**
4. **Implement connection pooling** to reduce overhead
