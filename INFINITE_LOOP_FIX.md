# Infinite Loop Execution Fix

## Problem
Tasks were stopping after 3-4 loops due to scheduling lock issues. The scheduling lock was being acquired but not properly released in all code paths, causing a permanent lock that prevented future scheduling.

## Root Causes
1. **Lock Not Released Properly**: The scheduling lock was only released when the timeout fired, not immediately after scheduling
2. **Race Condition**: If an error occurred during scheduling, the lock remained acquired
3. **No Stale Lock Cleanup**: Locks could remain indefinitely if a task crashed

## Solution

### 1. Fixed Lock Management
- Modified `acquireLoopSchedulingLock` to track lock timestamps
- Added stale lock detection (locks older than 5 minutes are automatically released)
- Modified `releaseLoopSchedulingLock` to include debug logging

### 2. Fixed scheduleNextLoop Function
- Moved lock release to happen immediately after setTimeout is created
- Added try-catch-finally to ensure lock is always released
- Lock is now released in two places:
  - Immediately after scheduling (so other code can schedule if needed)
  - In finally block when timeout executes (redundant but safe)

### 3. Added Lock Cleanup
- Added periodic cleanup every minute to release stale locks
- Logs current lock states for debugging

### 4. Added Debug Endpoint
- Created `/debug/scheduling` endpoint to monitor:
  - Current lock states with age
  - Loop counters for each task
  - Active tasks and their status
  - Summary statistics

### 5. Improved Error Handling
- Added try-catch blocks in critical scheduling paths
- Ensured scheduling flags are always cleared
- Made cleanupTaskResources safe to call multiple times

## Testing
1. Start the server: `npm run dev`
2. Create a task with loopInterval = 0 (immediate rescheduling)
3. Run the monitoring script: `npx ts-node test-infinite-loops.ts`
4. Watch for increasing loop counts - this indicates infinite execution is working

## Key Changes Made

### In acquireLoopSchedulingLock:
```typescript
// Now tracks timestamp and checks for stale locks
const existingLock = loopSchedulingLocks.get(taskId);
if (existingLock && existingLock.locked) {
  if (now - existingLock.timestamp > LOCK_TIMEOUT_MS) {
    // Auto-release stale lock
  }
}
```

### In scheduleNextLoop:
```typescript
try {
  const timeoutId = setTimeout(...);
  taskTimeouts.set(taskId, timeoutId);
  
  // Release lock immediately after scheduling
  releaseLoopSchedulingLock(taskId);
} catch (error) {
  // Always release lock on error
  releaseLoopSchedulingLock(taskId);
}
```

### Periodic Cleanup:
```typescript
setInterval(() => {
  // Clean up locks older than 5 minutes
  loopSchedulingLocks.forEach((lock, taskId) => {
    if (lock.locked && now - lock.timestamp > LOCK_TIMEOUT_MS) {
      loopSchedulingLocks.delete(taskId);
    }
  });
}, 60000);
```

## Result
Tasks now continue scheduling indefinitely as requested. The lock is properly managed and stale locks are automatically cleaned up, preventing the scheduling from getting stuck after 3-4 loops.