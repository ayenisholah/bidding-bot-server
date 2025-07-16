# StopTask Function Improvement Guide

## Overview

The `stopTask` function is a critical component of the NFT bidding bot that handles task cancellation across multiple marketplaces. This document provides a comprehensive analysis of the function, its dependencies, and actionable improvements.

## Current Architecture

### Function Flow
```
stopTask(task, start, marketplace?)
├── markTaskAsAborted()
├── Update in-memory states
├── Cancel marketplace requests
│   ├── cancelOpenseaRequest()
│   ├── cancelMagicedenRequest()
│   └── cancelBlurRequest()
├── updateTaskStatus()
├── unsubscribeFromCollection()
├── removePendingAndWaitingBids()
└── cancelAllRelatedBids()
```

### Parent Functions That Call stopTask

1. **Task Loop Restart** (line 1560) - When restarting a task loop
2. **WebSocket Message Handler** (line 1709) - Via 'stop-task' message
3. **updateTaskHandler** (line 2515) - When stop flag is set
4. **deleteTask** (line 3194) - Before deleting a task
5. **stopAllRelatedJobs** (line 3566) - When stopping all jobs
6. **handleOutgoingMarketplace** (line 3636) - Removing a marketplace
7. **stopTasksHandler** (line 3657) - Bulk task stopping

## Critical Issues Identified

### 1. Memory Leaks
```javascript
// Problem: abortedTasks array grows indefinitely
const abortedTasks: string[] = [];

function markTaskAsAborted(taskId: string) {
  if (!abortedTasks.includes(taskId)) {
    abortedTasks.push(taskId);  // Never cleaned up!
  }
}
```

### 2. Performance Bottlenecks
```javascript
// Problem: O(n) operations throughout
const taskIndex = currentTasks.findIndex(t => t._id === task._id);
if (!abortedTasks.includes(taskId)) { ... }

// removePendingAndWaitingBids iterates through ALL jobs
const jobs = await Promise.all([
  queue.getJobs(['waiting', 'delayed', 'prioritized'])
]);
```

### 3. State Consistency Issues
- Multiple data structures tracking same information (activeTasks, currentTasks, taskStatus)
- No transaction boundaries - partial failures leave inconsistent state
- In-memory state lost on restart

### 4. Error Handling Problems
```javascript
// Silent failures in multiple places
try {
  await unsubscribeFromCollection(task);
} catch (error) {
  console.error(...);  // Error swallowed, process continues
}
```

### 5. Resource Management
- Unused variable: `WALLET_PRIVATE_KEY` decrypted but never used
- WebSocket unsubscribe waits 5 seconds arbitrarily
- No cleanup of taskCountdownTimers in error cases

## Recommended Improvements

### 1. Implement Proper State Management

```javascript
// Use Redis for persistent state
class TaskStateManager {
  private redis: Redis;
  
  async markAborted(taskId: string): Promise<void> {
    await this.redis.sadd('aborted_tasks', taskId);
    // Set TTL to auto-cleanup old entries
    await this.redis.expire('aborted_tasks', 86400); // 24 hours
  }
  
  async isAborted(taskId: string): Promise<boolean> {
    return Boolean(await this.redis.sismember('aborted_tasks', taskId));
  }
}
```

### 2. Optimize Performance

```javascript
// Replace arrays with Maps for O(1) lookups
const currentTasksMap = new Map<string, ITask>();
const taskStatusMap = new Map<string, TaskStatus>();

// Optimize queue job removal
async function removePendingAndWaitingBids(task: ITask, marketplace?: string) {
  const pattern = marketplace 
    ? `${task._id}-${marketplace}*`
    : `${task._id}-*`;
    
  // Use queue filtering instead of fetching all jobs
  await queue.removeJobs(pattern);
}
```

### 3. Add Transaction Support

```javascript
async function stopTask(task: ITask, start: boolean, marketplace?: string) {
  const session = await mongoose.startSession();
  
  try {
    await session.withTransaction(async () => {
      // All state changes within transaction
      await markTaskAsAborted(taskId, { session });
      await updateTaskStatus(task, start, marketplace, { session });
      // ... other operations
    });
  } finally {
    await session.endSession();
  }
}
```

### 4. Implement Proper Cleanup

```javascript
class TaskCleanupService {
  private cleanupInterval: NodeJS.Timeout;
  
  startCleanup() {
    this.cleanupInterval = setInterval(async () => {
      // Clean old aborted tasks
      await this.cleanOldAbortedTasks();
      // Clean orphaned timers
      await this.cleanOrphanedTimers();
      // Clean completed job states
      await this.cleanCompletedJobStates();
    }, 300000); // 5 minutes
  }
}
```

### 5. Add Comprehensive Error Handling

```javascript
async function stopTask(task: ITask, start: boolean, marketplace?: string) {
  const errors: Error[] = [];
  
  // Collect all errors instead of failing fast
  const operations = [
    { name: 'cancelRequests', fn: () => cancelMarketplaceRequests(task, marketplace) },
    { name: 'unsubscribe', fn: () => unsubscribeFromCollection(task) },
    { name: 'removeJobs', fn: () => removePendingAndWaitingBids(task, marketplace) },
    { name: 'cancelBids', fn: () => cancelAllRelatedBids(task, marketplace) }
  ];
  
  const results = await Promise.allSettled(
    operations.map(op => op.fn().catch(e => {
      errors.push(new Error(`${op.name} failed: ${e.message}`));
      throw e;
    }))
  );
  
  if (errors.length > 0) {
    await logger.logTaskError('stopTask partially failed', {
      taskId: task._id,
      errors: errors.map(e => e.message)
    });
  }
}
```

### 6. Improve WebSocket Handling

```javascript
async function unsubscribeFromCollection(task: ITask): Promise<void> {
  // Use exponential backoff instead of fixed wait
  const maxAttempts = 3;
  let attempt = 0;
  
  while (attempt < maxAttempts) {
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'unsubscribe',
        data: { slug: task.contract.slug, taskId: task._id }
      }));
      subscribedTasks.delete(task._id);
      return;
    }
    
    await new Promise(resolve => setTimeout(resolve, Math.pow(2, attempt) * 1000));
    attempt++;
  }
  
  throw new Error('WebSocket not available after retries');
}
```

### 7. Add Monitoring and Metrics

```javascript
// Track stop task performance
async function stopTask(task: ITask, start: boolean, marketplace?: string) {
  const startTime = Date.now();
  
  try {
    // ... existing logic
    
    // Emit metrics
    await metrics.recordTaskStop({
      taskId: task._id,
      marketplace,
      duration: Date.now() - startTime,
      success: true
    });
  } catch (error) {
    await metrics.recordTaskStop({
      taskId: task._id,
      marketplace,
      duration: Date.now() - startTime,
      success: false,
      error: error.message
    });
    throw error;
  }
}
```

## Implementation Priority

1. **High Priority**
   - Fix memory leaks in abortedTasks
   - Optimize removePendingAndWaitingBids performance
   - Add proper error propagation

2. **Medium Priority**
   - Implement Redis-based state persistence
   - Add transaction support for consistency
   - Improve WebSocket connection handling

3. **Low Priority**
   - Add comprehensive metrics
   - Implement cleanup service
   - Refactor to use dependency injection

## Testing Recommendations

1. **Unit Tests**
   - Test each child function in isolation
   - Mock external dependencies
   - Test error scenarios

2. **Integration Tests**
   - Test full stopTask flow
   - Test concurrent stops
   - Test marketplace-specific stops

3. **Load Tests**
   - Test with 1000+ tasks
   - Test rapid start/stop cycles
   - Monitor memory usage

## Conclusion

The stopTask function is critical but suffers from performance, reliability, and maintainability issues. Implementing these improvements will significantly enhance the system's robustness and scalability. Start with high-priority fixes that address memory leaks and performance bottlenecks, then progressively implement architectural improvements.