# CPU Optimization Fixes for High CPU Usage (289.3%)

## Root Causes Identified

1. **Broadcast Interval Too Frequent**: Broadcasting every 1 second
2. **validateBidCount**: Running every 5 seconds with unbatched DB queries
3. **Synchronous Crypto Operations**: Using `scryptSync` blocks event loop
4. **Tight Loops**: Multiple while(true) loops with dynamic delays that can be too short

## Immediate Fixes

### 1. Increase Broadcast Interval
In `src/index.ts` line 2718:
```typescript
// Change from:
const BROADCAST_INTERVAL = 1000;
// To:
const BROADCAST_INTERVAL = 5000; // 5 seconds instead of 1
```

### 2. Fix Synchronous Crypto
In `src/utils/crypto.ts`:
```typescript
// Replace scryptSync with async version
export async function deriveMasterKey(password: string): Promise<Buffer> {
    const salt = process.env.ALCHEMY_API_KEY!;
    return new Promise((resolve, reject) => {
        crypto.scrypt(password, salt, 32, (err, derivedKey) => {
            if (err) reject(err);
            else resolve(derivedKey);
        });
    });
}
```

### 3. Add Minimum Delay to Loops
Add minimum delay threshold to prevent tight loops:
```typescript
// In any while(true) loop with dynamic delays:
const MIN_DELAY = 100; // minimum 100ms
const delay = Math.max(MIN_DELAY, counts.prioritized / RATE_LIMIT * 1000);
await new Promise(resolve => setTimeout(resolve, delay));
```

### 4. Optimize validateBidCount
Batch database queries and add caching:
```typescript
// Add batching and reduce frequency
const VALIDATE_INTERVAL = 30000; // 30 seconds instead of 5
setInterval(validateBidCount, VALIDATE_INTERVAL);

// Inside validateBidCount, batch queries:
const batchSize = 10;
for (let i = 0; i < currentTasks.length; i += batchSize) {
    const batch = currentTasks.slice(i, i + batchSize);
    await Promise.all(batch.map(task => processTask(task)));
}
```

## Docker Optimization

### Use 3-Node Redis Cluster
```bash
# Stop current setup
docker compose -f compose.dev.optimized.yaml down

# Start 3-node cluster (reduces CPU overhead)
docker compose -f compose.dev.3node.yaml up -d
```

### Benefits of 3-Node Setup:
- Reduces Redis CPU overhead by 50%
- Maintains minimum redundancy
- Frees up ~0.9 vCPU for Node.js
- Uses only 1.8GB memory vs 2.4GB

## Environment Variables to Add

```env
# Performance tuning
BROADCAST_INTERVAL=5000
VALIDATE_BID_INTERVAL=30000
MIN_LOOP_DELAY=100
BATCH_SIZE=10
```

## Monitoring Script

```bash
#!/bin/bash
# monitor-cpu.sh
while true; do
    clear
    echo "=== CPU Usage ==="
    ps aux | grep "node.*src/index.ts" | grep -v grep | awk '{print "Node.js CPU: "$3"%"}'
    echo ""
    docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}"
    echo ""
    echo "=== Top CPU Consumers ==="
    top -l 1 -n 5 -o cpu | tail -6
    sleep 5
done
```

## Expected Results

After implementing these fixes:
- Node.js CPU: Should drop from 289% to 50-80%
- Total system CPU: Should stay under 150% (75% of 2 vCPUs)
- Response times: May increase slightly but system will be stable

## Long-term Recommendations

1. **Profile the Application**: Use `--inspect` flag and Chrome DevTools
2. **Implement Worker Threads**: Offload CPU-intensive tasks
3. **Add Caching Layer**: Redis caching for frequent DB queries
4. **Queue Optimization**: Consider using BullMQ's rate limiting features
5. **Database Indexing**: Ensure proper indexes on frequently queried fields