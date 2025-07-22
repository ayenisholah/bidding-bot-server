# Task State Management Migration Guide

This guide explains how to integrate the new task state management system that fixes the race conditions, state pollution, and stuck states issues.

## Overview of Changes

The new system introduces:
1. **TaskStateManager**: Centralized state management with mutex locks
2. **QueueEventHandlerManager**: Refactored queue event handlers with proper state isolation
3. **TaskOperationsManager**: Thread-safe task operations (start/stop)
4. **State Machine**: Enforced state transitions to prevent invalid states

## Key Improvements

### 1. Fixed Issues
- **Random Task Starts**: Tasks now have isolated completion tracking per marketplace
- **State Pollution**: All state changes are now atomic with distributed locks
- **Race Conditions**: Mutex locks prevent concurrent operations on the same task
- **Stuck States**: State machine enforces valid transitions and prevents stuck states
- **Memory Leaks**: Proper timer cleanup and resource management

### 2. New Features
- Thread-safe state transitions
- Version-based optimistic locking
- State change listeners for monitoring
- Automatic resource cleanup
- Better error handling and recovery

## Migration Steps

### Step 1: Update Imports

Replace the old imports in `src/index.ts`:

```typescript
// Old imports
import { stopTask, startTask } from './somewhere';
import { taskStatus, activeTasks, parentJobs, completedTasks } from './somewhere';

// New imports
import { TaskStateManager } from './utils/task-state-manager';
import { QueueEventHandlerManager } from './utils/queue-event-handlers';
import { TaskOperationsManager } from './functions/task-operations';
```

### Step 2: Initialize Managers

Add this initialization code after creating the lock manager:

```typescript
// Create state manager
const taskStateManager = new TaskStateManager(lockManager);

// Create operations manager
const taskOperationsManager = new TaskOperationsManager(taskStateManager, lockManager);

// Create queue event handler manager (after queue is initialized)
const queueEventManager = new QueueEventHandlerManager(
  taskStateManager,
  queueEvents,
  enabledMarketplaces
);
```

### Step 3: Replace Global Variables

Replace all references to global state variables:

```typescript
// Old way
taskStatus[taskId] = { status: 'RUNNING', ... };
activeTasks.set(taskId, task);
parentJobs.set(parentId, job);
completedTasks[taskId] = { opensea: true, ... };

// New way
await taskStateManager.transitionState(taskId, 'RUNNING');
taskStateManager.setActiveTask(taskId, task);
taskStateManager.setParentJob(parentId, job);
await taskStateManager.markMarketplaceComplete(taskId, 'opensea');
```

### Step 4: Update Task Operations

Replace stopTask and startTask calls:

```typescript
// Old way
await stopTask(task, false, marketplace);
await startTask(task, true);

// New way
await taskOperationsManager.stopTask(task, false, marketplace);
await taskOperationsManager.startTask(task, true);
```

### Step 5: Update State Access

Replace direct state access with manager methods:

```typescript
// Old way
const status = taskStatus[taskId];
const task = activeTasks.get(taskId);
const job = parentJobs.get(parentId);

// New way
const status = taskStateManager.getTaskState(taskId);
const task = taskStateManager.getActiveTasks().get(taskId);
const job = taskStateManager.getParentJobs().get(parentId);
```

### Step 6: Update Queue Event Handlers

Remove the old `initializeQueueEventHandlers()` function and its calls. The new QueueEventHandlerManager handles this automatically.

### Step 7: Update WebSocket Broadcasting

Update the broadcastBidRates function to use the state manager:

```typescript
function broadcastBidRates() {
  bidRates = getAllBidRates();

  // Get all task states
  const allTaskStates: any = {};
  taskStateManager.getAllTaskStatus().forEach((state, taskId) => {
    allTaskStates[taskId] = state;
    
    // Enforce STOPPED status for completed cancellations
    if (state.cancellationPercentage >= 100 && state.status !== 'STOPPED') {
      console.log(`Broadcast enforcement: Setting task ${taskId} to STOPPED`);
      taskStateManager.transitionState(taskId, 'STOPPED');
    }
  });

  const message = JSON.stringify({
    type: 'bidRatesUpdate',
    data: { 
      bidRates, 
      bidCounts: bidStats, 
      skipCounts: taskOperationsManager.getSkipStats(), 
      errorCounts: taskOperationsManager.getErrorStats(), 
      floorPrices, 
      bestOffers, 
      warningBids: taskOperationsManager.getWarningBids(), 
      warningMessage, 
      rps, 
      taskStatus: allTaskStates, 
      ownBidCounts 
    }
  });

  clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}
```

### Step 8: Update Task Cleanup

Update the cleanupAbortedTasks function:

```typescript
// Old way
function cleanupAbortedTasks() {
  // manual cleanup logic
}

// New way
setInterval(() => {
  taskOperationsManager.cleanupAbortedTasks();
}, 30 * 60 * 1000); // 30 minutes
```

### Step 9: Add State Change Monitoring (Optional)

You can now monitor state changes:

```typescript
// Monitor state changes for a specific task
taskStateManager.onStateChange(taskId, (oldState, newState) => {
  console.log(`Task ${taskId} transitioned from ${oldState} to ${newState}`);
  
  // Trigger actions based on state changes
  if (newState === 'COMPLETED') {
    // Handle completion
  } else if (newState === 'STOPPED') {
    // Handle stop
  }
});
```

## Testing the Migration

1. **Test Task Start/Stop**: Verify tasks can be started and stopped without race conditions
2. **Test Concurrent Operations**: Try starting/stopping the same task from multiple requests
3. **Test State Transitions**: Verify invalid state transitions are prevented
4. **Test Memory Usage**: Monitor for memory leaks from timers
5. **Test Recovery**: Kill the server during operations and verify clean recovery

## Rollback Plan

If issues arise, you can rollback by:
1. Restoring the original `index.ts` file
2. Removing the new files in `utils/` and `functions/`
3. Restarting the server

## Common Issues and Solutions

### Issue: Lock timeout errors
**Solution**: Increase lock timeout in TaskStateManager constructor or specific operations

### Issue: State transition rejected
**Solution**: Check STATE_TRANSITIONS in task-state-manager.ts and add valid transitions

### Issue: Missing state after restart
**Solution**: Implement state persistence to Redis if needed

### Issue: WebSocket not updating
**Solution**: Ensure broadcastBidRates is using taskStateManager.getAllTaskStatus()

## Performance Considerations

- Lock operations add ~1-5ms overhead per operation
- State transitions are atomic but may queue under high load
- Consider implementing state persistence for recovery
- Monitor Redis memory usage for lock keys

## Future Enhancements

1. **State Persistence**: Save task states to Redis for recovery
2. **Event Sourcing**: Log all state transitions for debugging
3. **Metrics**: Add prometheus metrics for state transitions
4. **Health Checks**: Monitor for stuck locks or invalid states