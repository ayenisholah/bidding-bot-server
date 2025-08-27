# Server Optimization for 8GB RAM Configuration

## Resource Allocation

### Memory Distribution (Total: 8GB)
- **Server**: 4GB (50%)
  - Node.js heap: 3.5GB
  - Container overhead: 0.5GB
- **MongoDB**: 2GB (25%)
  - WiredTiger cache: 1.5GB
  - System overhead: 0.5GB
- **Redis**: 1GB (12.5%)
  - Max memory: 896MB
  - System overhead: 128MB
- **Client**: 384MB (4.7%)
  - Next.js heap: 256MB
  - Container overhead: 128MB
- **System Reserve**: ~640MB (8%)

### CPU Distribution (Total: 4 cores)
- **Server**: 2.5 cores (62.5%)
- **MongoDB**: 0.75 cores (18.75%)
- **Client**: 0.5 cores (12.5%)
- **Redis**: 0.25 cores (6.25%)

## Key Optimizations

### 1. Node.js Server
- Increased heap size to 3.5GB for better performance
- Removed problematic flags (--optimize-for-size, --gc-interval)
- Added UV_THREADPOOL_SIZE=8 for async operations
- MongoDB connection pool limited to 25 connections
- Health check dependencies ensure proper startup order

### 2. Redis Configuration
- Switched to Alpine image for smaller footprint
- LRU eviction policy to prevent OOM
- IO threads enabled for better performance
- Active defragmentation tuned for memory efficiency
- AOF persistence with everysec for durability

### 3. MongoDB Optimizations
- WiredTiger cache set to 1.5GB
- Snappy compression enabled
- Index prefix compression enabled
- Diagnostic data collection disabled
- Using specific version (7-jammy) for stability

### 4. Docker Enhancements
- CPU limits prevent resource starvation
- Swap limits provide emergency memory
- Ulimits increased for high connection counts
- Custom network subnet for isolation
- Volume optimization with local drivers

## Usage

1. Deploy the optimized configuration:
```bash
docker compose -f compose.yaml up -d
```

2. Monitor resource usage:
```bash
docker stats
```

3. Check service health:
```bash
docker compose ps
curl http://localhost:3003/health
```

## Performance Tips

1. **Memory Monitoring**: Keep total container memory below 7.5GB to maintain system stability
2. **Swap Usage**: Monitor swap usage - consistent swapping indicates memory pressure
3. **Connection Pools**: Adjust MONGO_MAX_POOL_SIZE based on concurrent load
4. **Redis Memory**: Monitor Redis memory usage with `redis-cli info memory`
5. **Garbage Collection**: Server uses --expose-gc for manual GC control if needed

## Troubleshooting

- **OOM Kills**: Check `dmesg` or Docker logs for out-of-memory events
- **Slow Performance**: Monitor CPU throttling with `docker stats`
- **Connection Errors**: Increase ulimits if hitting connection limits
- **Memory Leaks**: Use heap dumps with `--heapdump-on-oom` flag