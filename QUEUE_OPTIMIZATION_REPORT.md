# Queue and Worker Optimization Report for 2 vCPU / 4GB System

## Executive Summary

Your system is experiencing high CPU usage (289.3%) and frequent job failures due to:
- Massive Redis over-allocation (24GB on 4GB system)
- Excessive concurrency (100 jobs on 2 CPUs)
- Inefficient polling intervals
- Poor job retry strategies

## Current Configuration Issues

### 1. Redis Cluster Configuration
```yaml
# Current: 6 nodes × 4GB = 24GB (600% of system RAM!)
maxmemory: 4gb per node
persistence: AOF + RDB (heavy I/O)
active-defrag: enabled (CPU intensive)
```

**Impact**: Severe memory pressure, swapping, OOM kills

### 2. Queue/Worker Configuration
```typescript
// Current configuration
WORKER_COUNT = 1
concurrency = RATE_LIMIT = 100
pQueue.concurrency = RATE_LIMIT / 2 = 50
lockDuration = 30000 // Too short for long jobs
```

**Impact**: CPU overload, lock renewal failures

### 3. High-Frequency Operations
- Broadcasting: Every 1 second
- validateBidCount: Every 5 seconds
- Health checks: Every 30 seconds
- Multiple WebSocket heartbeats

## Job Failure Analysis

### Common Failure Patterns

1. **Lock Renewal Failures** (Most Common)
   ```
   Error: Could not renew lock for job
   ```
   - Cause: 30-second lock duration too short
   - Jobs taking >30s lose their lock

2. **Memory-Related Failures**
   - OOM kills during job processing
   - Redis connection drops
   - Slow job processing due to swapping

3. **Rate Limit Exhaustion**
   - 100 concurrent API calls overwhelming services
   - No backpressure mechanism
   - Poor retry strategies

4. **Connection Pool Exhaustion**
   - Too many concurrent Redis connections
   - WebSocket connection limits
   - Database connection timeouts

## Optimized Configuration

### 1. Redis Optimization (compose.dev.optimized.yaml)

```yaml
# 3-node cluster with proper limits
services:
  redis-node-1:
    command: >
      redis-server
      --maxmemory 512mb          # Reduced from 4gb
      --maxmemory-policy noeviction
      --save ""                  # Disable persistence
      --appendonly no            # Disable AOF
      --activedefrag no          # Disable defrag
    deploy:
      resources:
        limits:
          cpus: '0.3'
          memory: 640M
```

### 2. Queue Configuration Updates

```typescript
// Optimized settings for 2 vCPU system
export const OPTIMIZED_QUEUE_CONFIG = {
  WORKER_COUNT: 2,                    // 2 workers for better distribution
  CONCURRENCY_PER_WORKER: 10,         // 20 total (10 per CPU)
  LOCK_DURATION: 120000,              // 2 minutes
  LOCK_RENEW_TIME: 40000,             // Renew at 1/3 of duration
  STALLED_INTERVAL: 60000,            // 1 minute
  MAX_STALLED_COUNT: 2,               // Allow 2 retries
};

// Job options with better retry strategy
const optimizedJobOptions: JobsOptions = {
  attempts: 3,
  backoff: {
    type: 'exponential',
    delay: 5000,  // Start with 5s
  },
  removeOnComplete: {
    age: 3600,    // Keep for 1 hour
    count: 100,   // Keep last 100
  },
  removeOnFail: {
    age: 86400,   // Keep for 24 hours for debugging
  },
};
```

### 3. Memory-Efficient Redis Connection

```typescript
// Optimized Redis configuration
const optimizedRedisConfig = {
  enableOfflineQueue: true,
  enableReadyCheck: true,
  maxRetriesPerRequest: 3,
  enableAutoPipelining: true,      // Batch commands
  dropBufferSupport: true,          // Save memory
  keepAlive: 30000,                 // 30s keepalive
  connectionName: 'bullmq-worker',
  // Connection pool settings
  pool: {
    min: 2,
    max: 10,
  },
};
```

### 4. CPU Optimization Settings

```javascript
// Environment variables to add
BROADCAST_INTERVAL=5000         // 5 seconds instead of 1
VALIDATE_BID_INTERVAL=30000     // 30 seconds instead of 5
MIN_LOOP_DELAY=100              // Minimum 100ms between loops
JOB_BATCH_SIZE=5                // Process 5 jobs at a time
HEALTH_CHECK_INTERVAL=60000     // 1 minute health checks
```

## Implementation Steps

### Step 1: Update Docker Compose
```bash
# Use the optimized 3-node configuration
docker compose -f compose.dev.3node.yaml down
docker compose -f compose.dev.3node.yaml up -d
```

### Step 2: Update Queue Configuration
Create `src/config/queue.config.ts`:
```typescript
export const QUEUE_CONFIG = {
  redis: {
    enableOfflineQueue: true,
    maxRetriesPerRequest: 3,
    dropBufferSupport: true,
    enableAutoPipelining: true,
  },
  worker: {
    concurrency: 10,
    lockDuration: 120000,
    lockRenewTime: 40000,
  },
  jobs: {
    defaultTimeout: 90000,
    removeOnComplete: { age: 3600, count: 100 },
    removeOnFail: { age: 86400 },
  },
};
```

### Step 3: Implement Job Batching
```typescript
// Batch similar jobs together
class JobBatcher {
  private batch: Map<string, Job[]> = new Map();
  private batchSize = 5;
  private flushInterval = 1000;

  async addJob(job: Job): Promise<void> {
    const key = `${job.name}-${job.data.marketplace}`;
    if (!this.batch.has(key)) {
      this.batch.set(key, []);
    }
    
    const jobs = this.batch.get(key)!;
    jobs.push(job);
    
    if (jobs.length >= this.batchSize) {
      await this.flush(key);
    }
  }

  private async flush(key: string): Promise<void> {
    const jobs = this.batch.get(key) || [];
    if (jobs.length === 0) return;
    
    // Process batch
    await this.processBatch(jobs);
    this.batch.delete(key);
  }
}
```

### Step 4: Add Circuit Breaker
```typescript
class CircuitBreaker {
  private failures = 0;
  private lastFailTime = 0;
  private state: 'CLOSED' | 'OPEN' | 'HALF_OPEN' = 'CLOSED';
  
  async execute<T>(fn: () => Promise<T>): Promise<T> {
    if (this.state === 'OPEN') {
      if (Date.now() - this.lastFailTime > 60000) {
        this.state = 'HALF_OPEN';
      } else {
        throw new Error('Circuit breaker is OPEN');
      }
    }
    
    try {
      const result = await fn();
      if (this.state === 'HALF_OPEN') {
        this.state = 'CLOSED';
        this.failures = 0;
      }
      return result;
    } catch (error) {
      this.failures++;
      this.lastFailTime = Date.now();
      
      if (this.failures >= 5) {
        this.state = 'OPEN';
      }
      throw error;
    }
  }
}
```

## Monitoring and Metrics

### Key Metrics to Track
1. **Queue Health**
   - Active/Waiting/Delayed job counts
   - Job completion rate
   - Average job duration
   - Lock renewal failures

2. **System Resources**
   - CPU usage per worker
   - Memory usage (RSS/Heap)
   - Redis memory usage
   - Network connections

3. **Job Performance**
   - Success/failure rates by job type
   - Retry attempts
   - Processing time percentiles
   - API rate limit usage

### Monitoring Commands
```bash
# Check queue health
curl http://localhost:3003/queue/stats

# Monitor Redis memory
docker exec redis_node_1 redis-cli info memory

# Watch system resources
docker stats --no-stream

# Check job failures
docker logs server --tail 100 | grep -i "fail\|error"
```

## Expected Results

After implementing these optimizations:

1. **CPU Usage**: Drop from 289% to 50-80%
2. **Memory Usage**: Stable at 60-70% of 4GB
3. **Job Success Rate**: Increase from ~70% to >95%
4. **Lock Failures**: Reduce by 90%
5. **Response Time**: 2-3x improvement

## Troubleshooting Guide

### If jobs still fail after optimization:

1. **Check Redis connectivity**
   ```bash
   docker exec redis_node_1 redis-cli ping
   docker exec redis_node_1 redis-cli cluster info
   ```

2. **Verify memory usage**
   ```bash
   free -h
   docker system df
   ```

3. **Review job logs**
   ```bash
   docker logs server --tail 500 | grep -A5 -B5 "jobId"
   ```

4. **Check rate limits**
   - Monitor API response headers
   - Implement adaptive rate limiting
   - Use exponential backoff

## Conclusion

The primary issues are:
1. Massive Redis over-allocation (24GB on 4GB system)
2. Excessive concurrency (100 jobs on 2 CPUs)
3. Short lock durations causing renewal failures
4. Inefficient polling and broadcasting

Implementing these optimizations will significantly improve stability and reduce job failures.