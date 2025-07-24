# SCHEDULE Job Timeout Root Cause Analysis

## Root Cause
The timeouts occur after ~15 minutes when CANCEL jobs run alongside SCHEDULE jobs due to:

1. **Queue Congestion**: Cancellation jobs are added without priority, competing with SCHEDULE jobs
2. **Blocking Wait Loop**: `processBulkJobs` has an infinite loop waiting for queue to drain below threshold
3. **Resource Contention**: When both CANCEL and SCHEDULE jobs run simultaneously, the queue never drains

## The Problem Flow
1. Task runs for 15 minutes, accumulating orders
2. New SCHEDULE job starts, needs to cancel stale orders
3. Hundreds of CANCEL jobs are queued (without priority)
4. `processBulkJobs` enters infinite wait loop: `while (prioritizedCount > DRAIN_THRESHOLD)`
5. Queue is congested with CANCEL jobs, never drops below threshold
6. SCHEDULE job times out after 180 seconds

## Critical Code Issues

### 1. Cancellation Jobs Without Priority
```javascript
// Current (BAD)
await queue.add(CANCEL_MAGICEDEN_BID, { ... });

// Should be
await queue.add(CANCEL_MAGICEDEN_BID, { ... }, {
  priority: CANCEL_BID_PRIORITY.MAGICEDEN
});
```

### 2. Infinite Wait Loop in processBulkJobs
```javascript
while (true) {
  const counts = await queue.getJobCounts();
  const prioritizedCount = counts.prioritized || 0;
  
  if (prioritizedCount <= DRAIN_THRESHOLD) {
    break;
  }
  // This can wait forever if queue is congested
  const delay = (prioritizedCount / RATE_LIMIT);
  await new Promise(resolve => setTimeout(resolve, delay * 1000));
}
```

## Implemented Fixes

### 1. Added Cancel Job Priority
- Created `CANCEL_BID_PRIORITY` with lower priority (1) than bid jobs (2)
- Updated cancellation job creation to use priority

### 2. Batch Processing for Large Token Sets
- Process tokens in batches of 50
- Process traits in batches of 20
- Small delay between batches to prevent queue overload

### 3. Still Needed: Fix processBulkJobs
Replace infinite loop with timeout-aware waiting:
```javascript
const MAX_WAIT_TIME = 30000; // 30 seconds max wait
const startWait = Date.now();

while (Date.now() - startWait < MAX_WAIT_TIME) {
  const counts = await queue.getJobCounts();
  const prioritizedCount = counts.prioritized || 0;
  
  if (prioritizedCount <= DRAIN_THRESHOLD) {
    break;
  }
  
  // Log warning if waiting too long
  if (Date.now() - startWait > 10000) {
    console.log(YELLOW + `Queue congested: ${prioritizedCount} jobs waiting` + RESET);
  }
  
  await new Promise(resolve => setTimeout(resolve, 1000));
}
```

## Why It Happens After 15 Minutes
- Tasks accumulate orders over time
- After 15 minutes, there might be 100-500 orders to cancel
- Each order becomes a CANCEL job
- Without priority, these flood the queue
- New SCHEDULE jobs get blocked by the congestion

## Complete Solution
1. ✅ Give CANCEL jobs lower priority
2. ✅ Process tokens/traits in smaller batches  
3. ⚠️ Add timeout to queue drain waiting
4. ⚠️ Update all CANCEL job creations to use priority
5. ⚠️ Consider separate queue for cancellations