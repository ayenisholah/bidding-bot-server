# startTaskInternal Resilience Implementation

This document describes the hardening measures implemented to ensure `startTaskInternal()` never breaks the perpetual scheduling loop due to transient failures.

## Problem Statement

The `startTaskInternal()` function has several potential failure points that could throw exceptions and break the perpetual `scheduleNextLoop()` cycle:

1. **`orderCache.initializeFromDatabase()`** - MongoDB connection failures
2. **`queue.addBulk()` / `blurQueue.addBulk()`** - Redis connection failures
3. **`decryptPrivateKey()`** - Decryption failures (already handled)

When these operations throw, the timer callback catches the error but the task loop could stall if not properly rescheduled.

## Solution Architecture

### Defense-in-Depth Strategy

```
┌─────────────────────────────────────────────────────────────────┐
│                    Failure Recovery Layers                       │
├─────────────────────────────────────────────────────────────────┤
│  Layer 1: Local Retry (1-7 seconds)                             │
│  ├── initializeOrderCacheWithRetry: 3 retries, 1s/2s/4s delays  │
│  └── addBulkWithRetry: 3 retries, 1s/2s/4s delays               │
├─────────────────────────────────────────────────────────────────┤
│  Layer 2: Timer Callback Error Handler                          │
│  └── Reschedules on any startTask() exception                   │
├─────────────────────────────────────────────────────────────────┤
│  Layer 3: MongoDB Watchdog (5s + exponential backoff)           │
│  └── Detects overdue schedules, fires missed tasks              │
└─────────────────────────────────────────────────────────────────┘
```

## Implementation Details

### 1. Non-Blocking Cache Initialization

**File**: `src/index.ts` (lines 126-146)

```typescript
async function initializeOrderCacheWithRetry(taskId: string, maxRetries = 3): Promise<void> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      await orderCache.initializeFromDatabase(taskId);
      debugLog('Cache', `Order cache initialized for task ${taskId} on attempt ${attempt}`);
      return;
    } catch (err) {
      const delay = Math.min(1000 * Math.pow(2, attempt - 1), 10000); // 1s, 2s, 4s... max 10s
      const errorMsg = err instanceof Error ? err.message : String(err);
      warnLog('Cache', `Order cache init failed for ${taskId}, attempt ${attempt}/${maxRetries}, retrying in ${delay}ms: ${errorMsg}`);
      if (attempt < maxRetries) {
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }
  errorLog('Cache', `Order cache init exhausted retries for ${taskId} - continuing with empty cache`);
}
```

**Usage** (fire-and-forget pattern):
```typescript
// Task starts immediately, cache populates in background
initializeOrderCacheWithRetry(task._id.toString()).catch(() => {
  // Silently ignore - retries and errors handled internally
});
```

**Behavior**:
- Task starts immediately without waiting for cache
- Cache initialization runs in background with automatic retry
- If all retries fail, task continues with empty cache
- Counterbid handlers fall back to direct MongoDB queries

### 2. BullMQ Queue Retry Logic

**File**: `src/index.ts` (lines 148-185)

```typescript
async function addBulkWithRetry(
  targetQueue: Queue,
  jobs: any[],
  queueName: string,
  taskId: string,
  maxRetries = 3
): Promise<void> {
  if (jobs.length === 0) return;

  const RETRYABLE_ERRORS = [
    'ECONNREFUSED',
    'ECONNRESET',
    'Connection lost',
    'Connection is closed',
    'Command timed out',
    'READONLY'
  ];

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      await targetQueue.addBulk(jobs);
      if (attempt > 1) {
        debugLog('Queue', `${queueName} addBulk succeeded on attempt ${attempt} for task ${taskId}`);
      }
      return;
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      const isRetryable = RETRYABLE_ERRORS.some(e => errorMsg.includes(e));

      if (isRetryable && attempt < maxRetries) {
        const delay = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
        warnLog('Queue', `${queueName} addBulk failed for task ${taskId} (attempt ${attempt}/${maxRetries}), retrying in ${delay}ms: ${errorMsg}`);
        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }

      // Non-retryable error or exhausted retries - re-throw
      errorLog('Queue', `${queueName} addBulk failed for task ${taskId} after ${attempt} attempts: ${errorMsg}`, err);
      throw err;
    }
  }
}
```

**Retryable Error Classification**:

| Error Type | Description | Retry? |
|-----------|-------------|--------|
| `ECONNREFUSED` | Redis server down | Yes |
| `ECONNRESET` | Connection reset by peer | Yes |
| `Connection lost` | Socket disconnected | Yes |
| `Connection is closed` | Redis connection closed | Yes |
| `Command timed out` | Slow Redis response | Yes |
| `READONLY` | Redis cluster failover | Yes |
| Job validation error | Invalid job data | No |
| Other errors | Unknown issues | No |

### 3. Wrapped addBulk Calls

All 8 `queue.addBulk()` calls in `startTaskInternal()` are now wrapped:

**For loop restarts (start=true)**:
```typescript
// Line 9886
await addBulkWithRetry(queue, openseaJobs, 'opensea', taskId);

// Line 9889
await addBulkWithRetry(blurQueue, blurJobs, 'blur', taskId);

// Line 9896 (delayed path)
await addBulkWithRetry(queue, magicedenJobs, 'magiceden', taskId);

// Line 9902 (non-delayed path)
await addBulkWithRetry(queue, magicedenJobs, 'magiceden', taskId);
```

**For deduplication path (start=false)**:
```typescript
// Line 9939
await addBulkWithRetry(queue, newOpenseaJobs, 'opensea', taskId);

// Line 9942
await addBulkWithRetry(blurQueue, newBlurJobs, 'blur', taskId);

// Line 9949 (delayed path)
await addBulkWithRetry(queue, newMagicedenJobs, 'magiceden', taskId);

// Line 9955 (non-delayed path)
await addBulkWithRetry(queue, newMagicedenJobs, 'magiceden', taskId);
```

## Failure Scenarios and Recovery

### Scenario 1: Transient MongoDB Failure

```
Timeline:
0ms     startTaskInternal() called
1ms     initializeOrderCacheWithRetry() starts (non-blocking)
2ms     Task continues executing immediately
10ms    Cache init attempt 1 fails (MongoDB down)
1010ms  Cache init attempt 2 fails
3010ms  Cache init attempt 3 fails
3011ms  Task continues with empty cache
        (counterbids query MongoDB directly as fallback)
```

### Scenario 2: Transient Redis Failure

```
Timeline:
0ms     addBulkWithRetry() called for opensea jobs
1ms     Attempt 1 fails (ECONNREFUSED)
1001ms  Attempt 2 fails (still down)
3001ms  Attempt 3 succeeds (Redis recovered)
3002ms  Jobs queued successfully
```

### Scenario 3: Persistent Redis Failure

```
Timeline:
0ms     addBulkWithRetry() called
1ms     Attempt 1 fails
1001ms  Attempt 2 fails
3001ms  Attempt 3 fails
3002ms  Exception thrown → caught by timer callback
3003ms  Timer callback reschedules with backoff
8003ms  MongoDB watchdog detects overdue schedule
8004ms  Watchdog retries startTask() with infinite backoff
...     Eventually Redis recovers, task resumes
```

## Monitoring and Debugging

### Log Prefixes

| Prefix | Description |
|--------|-------------|
| `[Cache]` | Order cache initialization logs |
| `[Queue]` | BullMQ addBulk operation logs |
| `[TimerOp]` | Timer scheduling/firing logs |
| `[Watchdog]` | MongoDB watchdog recovery logs |

### Key Log Messages

```
# Successful cache init after retry
[Cache] Order cache initialized for task abc123 on attempt 2

# Cache init exhausted retries
[Cache] Order cache init exhausted retries for abc123 - continuing with empty cache

# Queue retry in progress
[Queue] opensea addBulk failed for task abc123 (attempt 1/3), retrying in 1000ms: ECONNREFUSED

# Queue succeeded after retry
[Queue] opensea addBulk succeeded on attempt 2 for task abc123

# Queue failed all retries
[Queue] opensea addBulk failed for task abc123 after 3 attempts: ECONNREFUSED
```

## Performance Impact

| Scenario | Time to Recovery |
|----------|-----------------|
| Single retry success | 1-2 seconds |
| All local retries | 7 seconds max |
| Watchdog recovery | 5s + exponential backoff |
| Worst case (persistent failure) | Infinite retry with 5-minute cap |

## Related Files

- `src/index.ts` - Main implementation
- `src/models/taskSchedule.model.ts` - MongoDB watchdog schema
- `src/utils/task-runtime-cache.ts` - Runtime state caching
- `docs/BULLETPROOF-SCHEDULING.md` - Overall scheduling architecture

## Changelog

- **2025-12-13**: Initial implementation
  - Added `initializeOrderCacheWithRetry()` helper
  - Added `addBulkWithRetry()` helper
  - Made cache initialization non-blocking
  - Wrapped all 8 `addBulk()` calls with retry logic
