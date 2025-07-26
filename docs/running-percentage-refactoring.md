# Running Percentage Refactoring Guide

## Overview

This document outlines the refactoring plan for moving `runningPercentage` calculation from the queue event handlers to individual process functions, enabling better error tracking and more accurate progress reporting.

## Current Implementation Analysis

### Current State
1. **Location**: `runningPercentage` is calculated in `initializeQueueEventHandlers()` (lines 2497-2515)
2. **Trigger**: Calculated when jobs complete in the queue event handler
3. **Scheduling**: Task scheduling happens when all marketplaces complete and `runningPercentage > 99%`
4. **Tracking**: Uses `parentJobs` Map to track job counts per marketplace
5. **Broadcasting**: `taskStatus` is sent via WebSocket in `broadcastBidRates()` for client updates

### Issues with Current Implementation
- Percentage only updates after job completion
- Errors are not properly reflected in the progress
- Calculation is centralized in queue handlers rather than where work happens
- Potential race conditions between job completion and percentage updates
- Total job count is not accurately set when jobs are created

## Proposed Architecture

### Summary of New Approach
The key insight is that `processBulkJobs` currently overwrites any existing `parentJobs` entry. By modifying it to check if an entry exists first, we can set up accurate job counts in the scheduled bid functions while maintaining backward compatibility.

### Core Changes
1. Modify `processBulkJobs` to only set parentJobs if it doesn't already exist
2. Set up parentJobs in scheduled bid functions BEFORE calling `processBulkJobs`
3. Remove `runningPercentage` calculation from `initializeQueueEventHandlers`
4. Add percentage calculation to each process function
5. Make bid functions throw errors instead of catching internally
6. Keep `taskStatus` structure unchanged for WebSocket compatibility
7. Create centralized helpers for percentage calculation and task scheduling

### Benefits of This Approach
- **Accurate job counting**: Total jobs are calculated where jobs are created
- **Backward compatibility**: `updateTaskData` and other callers still work
- **Better visibility**: Job setup happens at the source
- **No race conditions**: parentJobs is set before any processing starts

## Implementation Steps

### Phase 1: Update processBulkJobs to Check Existing parentJobs

#### 1.1 Modify processBulkJobs
Update the function to only set parentJobs if it doesn't already exist:

```typescript
async function processBulkJobs(jobs: Job[], createKey = false) {
  if (!jobs?.length) return;

  const firstJob = jobs[0];
  const taskId = firstJob.data._id;
  const marketplace = firstJob.name.split('_')[0] || 'UNKNOWN';
  const bulkJobId = `${firstJob.data._id}-${marketplace}`.toLowerCase();
  const task = activeTasks.get(taskId);

  // Only set parentJobs if it doesn't already exist
  if (!parentJobs.has(bulkJobId)) {
    parentJobs.set(bulkJobId, {
      parentId: bulkJobId,
      totalJobs: jobs.length,
      completedJobs: 0,
      failedJobs: 0,
      startTime: Date.now(),
      taskId,
      jobName: firstJob.name
    });
  }
  
  // ... rest of the function
}
```

### Phase 2: Update Scheduled Bid Functions for Job Counting

#### 2.1 processOpenseaScheduledBid
Update the function to set up parentJobs BEFORE calling processBulkJobs:

```typescript
async function processOpenseaScheduledBid(task: ITask, jobId?: string) {
  // ... existing setup code ...
  
  const parentJobId = `${currentTask._id}-opensea`.toLowerCase();
  
  if (tokenBid) {
    const tokens = currentTokenIds[task._id].opensea
      .filter(token => !isNaN(Number(token)) && String(token).toLowerCase().indexOf('bot') === -1)
      ?? [];
    
    // Set up parent job BEFORE calling processBulkJobs
    parentJobs.set(parentJobId, {
      parentId: parentJobId,
      totalJobs: tokens.length,
      completedJobs: 0,
      failedJobs: 0,
      startTime: Date.now(),
      taskId: currentTask._id,
      jobName: OPENSEA_TOKEN_BID,
    });
    
    const jobs = tokens.map((token) => ({
      name: OPENSEA_TOKEN_BID,
      data: { /* ... */ },
      opts: { priority: TOKEN_BID_PRIORITY.OPENSEA }
    }));
    
    await processBulkJobs(jobs, true);
  } else if (traitBid && collectionDetails.trait_offers_enabled) {
    // Set up parent job BEFORE calling processBulkJobs
    parentJobs.set(parentJobId, {
      parentId: parentJobId,
      totalJobs: traits.length,
      completedJobs: 0,
      failedJobs: 0,
      startTime: Date.now(),
      taskId: currentTask._id,
      jobName: OPENSEA_TRAIT_BID,
    });
    
    const traitJobs = traits.map((trait) => ({
      name: OPENSEA_TRAIT_BID,
      data: { /* ... */ },
      opts: { priority: TRAIT_BID_PRIORITY.OPENSEA }
    }));
    
    await processBulkJobs(traitJobs, true);
  } else if (collectionBid) {
    // Collection bid - already sets parentJobs correctly (1 job)
    parentJobs.set(parentJobId, {
      parentId: parentJobId,
      totalJobs: 1,
      completedJobs: 0,
      failedJobs: 0,
      startTime: Date.now(),
      taskId: currentTask._id,
      jobName: OPENSEA_COLLECTION_BID,
    });
    
    await queue.add(OPENSEA_COLLECTION_BID, { /* ... */ });
  }
}
```

#### 2.2 Apply Same Pattern to Other Scheduled Functions
- `processMagicedenScheduledBid` - Set parentJobs before processBulkJobs calls
- `processBlurScheduledBid` - Set parentJobs before processBulkJobs calls

### Phase 3: Update Bid Functions

#### 3.1 Modify Error Handling
Update `bidOnOpensea`, `bidOnMagiceden`, and `bidOnBlur` to throw errors:

```typescript
// Before
export async function bidOnOpensea(...) {
  try {
    // ... bid logic ...
  } catch (error) {
    // Log error
    console.error(error);
    // Error is swallowed
  }
}

// After
export async function bidOnOpensea(...) {
  try {
    // ... bid logic ...
  } catch (error) {
    // Log error
    await logger.logOpenseaBidError(...);
    // Re-throw for process function to handle
    throw error;
  }
}
```

### Phase 4: Create Helper Functions

**Important**: Keep taskStatus structure unchanged for WebSocket compatibility

#### 4.1 Update Running Percentage Helper
```typescript
function updateTaskRunningPercentage(taskId: string) {
  const task = activeTasks.get(taskId);
  if (!task || !taskStatus[taskId]) return;
  
  // Get selected marketplaces that are also enabled
  const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
  const enabledAndSelectedMarketplaces = selectedMarketplaces.filter(m =>
    enabledMarketplaces.map(em => em.toLowerCase()).includes(m)
  );
  
  // Sum totals across all marketplaces
  let totalJobs = 0;
  let completedJobs = 0;
  let failedJobs = 0;
  
  for (const marketplace of enabledAndSelectedMarketplaces) {
    const marketplaceParentId = `${taskId}-${marketplace}`.toLowerCase();
    const marketplaceParentJob = parentJobs.get(marketplaceParentId);
    if (marketplaceParentJob) {
      totalJobs += marketplaceParentJob.totalJobs;
      completedJobs += marketplaceParentJob.completedJobs;
      failedJobs += marketplaceParentJob.failedJobs;
    }
  }
  
  const runningPercentage = totalJobs > 0 ? ((completedJobs + failedJobs) / totalJobs) * 100 : 0;
  taskStatus[taskId].runningPercentage = Math.min(runningPercentage, 100);
  
  // Check if task is complete and schedule next iteration
  if (runningPercentage >= 100) {
    checkTaskCompletionAndScheduleNext(taskId);
  }
}
```

#### 4.2 Task Completion and Scheduling Helper
```typescript
async function checkTaskCompletionAndScheduleNext(taskId: string) {
  const task = activeTasks.get(taskId);
  if (!task || !task.running) return;
  
  // Get enabled marketplaces for this task
  const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
  const availableMarketplaces = selectedMarketplaces.filter(m =>
    enabledMarketplaces.map(em => em.toLowerCase()).includes(m)
  );
  
  // Check if all marketplaces are complete
  const allMarketsComplete = availableMarketplaces.every(m =>
    completedTasks[taskId]?.[m as 'opensea' | 'magiceden' | 'blur']
  );
  
  if (allMarketsComplete && taskStatus[taskId].runningPercentage >= 100) {
    // Execute completion logic and schedule next iteration
    await handleTaskCompletionAndScheduleNext(taskId);
  }
}
```

### Phase 5: Update Process Functions

**Note**: Since we're setting parentJobs in scheduled bid functions and processBulkJobs now checks if it exists, the job counting will be accurate.

#### 5.1 Process Function Pattern
Apply this pattern to all process functions:

```typescript
async function processOpenseaCollectionBid(data: { task: ITask, collectionDetails: any }) {
  const taskId = data.task._id;
  const marketplace = 'opensea';
  
  try {
    // Existing validation and setup
    if (!enabledMarketplaces.length) return;
    const { task, collectionDetails } = data;
    const currentTask = activeTasks.get(task._id);
    if (!currentTask || !currentTask?.running) return;
    
    // Get parent job for tracking
    const parentId = `${taskId}-${marketplace}`.toLowerCase();
    const parentJob = parentJobs.get(parentId);
    
    try {
      // Call bid function (will throw on error)
      await bidOnOpensea(
        task._id,
        bidCount,
        currentTask.wallet,
        privateKey,
        // ... other parameters
      );
      
      // Update completed count on success
      if (parentJob) {
        parentJob.completedJobs++;
      }
    } catch (bidError) {
      // Update failed count on bid error
      if (parentJob) {
        parentJob.failedJobs++;
      }
      console.error(`OpenSea collection bid failed for ${task._id}:`, bidError.message);
      
      // Optional: Add specific error handling
      if (bidError instanceof InsufficientBalanceError) {
        // Handle insufficient balance
      }
    }
    
    // Always update running percentage after bid attempt
    updateTaskRunningPercentage(taskId);
    
  } catch (error) {
    console.error(`Process function error for task ${taskId}:`, error);
  }
}
```

#### 5.2 Functions to Update
Apply the pattern to these functions:
1. `processOpenseaCollectionBid`
2. `processMagicedenCollectionBid`
3. `processOpenseaTraitBid`
4. `processOpenseaTokenBid`
5. `processMagicedenTraitBid`
6. `processMagicedenTokenBid`
7. `processBlurTraitBid`
8. `processBlurCollectionBid`

### Phase 6: Update Queue Event Handler

#### 6.1 Remove Running Percentage Logic
In `initializeQueueEventHandlers`, remove lines 2497-2515:

```typescript
// REMOVE THIS BLOCK
// Calculate total jobs across all selected and enabled marketplaces for this task
let totalJobs = 0;
let completedJobs = 0;
let failedJobs = 0;
const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
const enabledAndSelectedMarketplaces = selectedMarketplaces.filter(m =>
  enabledMarketplaces.map(em => em.toLowerCase()).includes(m)
);

for (const marketplace of enabledAndSelectedMarketplaces) {
  const marketplaceParentId = `${taskId}-${marketplace}`.toLowerCase();
  const marketplaceParentJob = parentJobs.get(marketplaceParentId);
  if (marketplaceParentJob) {
    totalJobs += marketplaceParentJob.totalJobs;
    completedJobs += marketplaceParentJob.completedJobs;
    failedJobs += marketplaceParentJob.failedJobs;
  }
}
const runningPercentage = ((completedJobs + failedJobs) / totalJobs) * 100
taskStatus[taskId].runningPercentage = Math.min(runningPercentage, 100)
```

#### 6.2 Remove Task Completion Check
Remove the task completion and scheduling logic (lines 2530-2677) from the queue event handler.

### Phase 7: Move Task Completion Logic

Create a dedicated function for task completion:

```typescript
async function handleTaskCompletionAndScheduleNext(taskId: string) {
  const task = activeTasks.get(taskId);
  if (!task) return;
  
  console.log(GREEN + `Task ${taskId} completed for all marketplaces` + RESET);
  
  // Handle trait and token removal cancellations
  const wallet = await walletCache.getWalletById(task.walletId);
  const WALLET_PRIVATE_KEY = decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
  
  let totalCancellationJobs = 0;
  
  if (traitsToRemove[taskId]?.length > 0) {
    const traitJobs = await cancelRemovedTraitBids(traitsToRemove[taskId], task._id, WALLET_PRIVATE_KEY, currencySymbol);
    totalCancellationJobs += traitJobs;
  }
  
  if (removedTokens[taskId]?.opensea?.length > 0 || removedTokens[taskId]?.magiceden?.length > 0) {
    const tokenJobs = await cancelRemovedTokenBids(removedTokens[taskId], task._id, WALLET_PRIVATE_KEY, currencySymbol);
    totalCancellationJobs += tokenJobs;
  }
  
  // Update status and cleanup
  taskStatus[taskId].status = 'COMPLETED';
  delete traitsToRemove[taskId];
  delete removedTokens[taskId];
  
  // Wait before scheduling next loop
  await new Promise(resolve => setTimeout(resolve, 1000));
  
  // Schedule next iteration if task is still running
  const currentTask = activeTasks.get(taskId);
  if (currentTask && currentTask.running) {
    // ... existing scheduling logic ...
  }
}
```

## Benefits

### 1. Real-time Progress Updates
- Progress updates immediately after each bid attempt
- No delay between job completion and UI update

### 2. Error Tracking
- Failed bids are properly counted in the progress
- Better visibility into task health

### 3. Cleaner Architecture
- Progress tracking happens where the actual work is performed
- Reduces coupling between queue handlers and business logic

### 4. Improved Scheduling
- Task completion triggers next iteration immediately
- No dependency on queue event timing

### 5. Accurate Marketplace Totals
- Properly sums jobs across selected AND enabled marketplaces
- Handles marketplace-specific configurations

## Testing Considerations

### Test Scenarios
1. Single marketplace task completion
2. Multi-marketplace task with different completion times
3. Tasks with bid failures
4. Tasks with mixed success/failure rates
5. Marketplace enable/disable during task execution

### Monitoring
- Log percentage updates with timestamps
- Track job success/failure rates per marketplace
- Monitor task scheduling intervals

## Key Implementation Notes

### Updated processBulkJobs Behavior
The `processBulkJobs` function now checks if parentJobs already exists:
```typescript
// Only set parentJobs if it doesn't already exist
if (!parentJobs.has(bulkJobId)) {
  parentJobs.set(bulkJobId, {
    parentId: bulkJobId,
    totalJobs: jobs.length,
    completedJobs: 0,
    failedJobs: 0,
    startTime: Date.now(),
    taskId,
    jobName: firstJob.name
  });
}
```

This means:
1. Scheduled bid functions set parentJobs BEFORE calling processBulkJobs
2. processBulkJobs respects the existing entry and doesn't override it
3. Other callers (like updateTaskData) still work as processBulkJobs creates parentJobs if needed

### WebSocket Compatibility
The `broadcastBidRates` function (line 3031) sends taskStatus to clients:
```typescript
const message = JSON.stringify({
  type: 'bidRatesUpdate',
  data: { bidRates, bidCounts: bidStats, skipCounts: skipStats, errorCounts: errorStats, floorPrices, bestOffers, warningBids, warningMessage, rps, taskStatus, ownBidCounts }
});
```

Therefore, we MUST NOT change the taskStatus structure.

## Migration Steps

1. **Backup**: Create backups of current implementation
2. **Update processBulkJobs**: Add check for existing parentJobs entry
3. **Update Scheduled Functions**: Add parentJobs setup before processBulkJobs calls in:
   - processOpenseaScheduledBid
   - processMagicedenScheduledBid
   - processBlurScheduledBid
4. **Helper Functions**: Implement updateTaskRunningPercentage and checkTaskCompletionAndScheduleNext
5. **Process Functions**: Update one process function at a time to calculate runningPercentage
6. **Bid Functions**: Update to throw errors instead of catching them
7. **Queue Handler**: Remove runningPercentage calculation and task completion logic
8. **Testing**: Test each marketplace individually
9. **Integration Testing**: Test multi-marketplace scenarios with updateTaskData
10. **Monitoring**: Add logging for debugging parentJobs setup
11. **Rollback Plan**: Keep old code commented for quick rollback

## Potential Issues and Solutions

### Issue 1: Race Conditions
**Problem**: Multiple process functions updating percentage simultaneously
**Solution**: Use atomic operations or locks if needed

### Issue 2: Memory Leaks
**Problem**: Completed task data not being cleaned up
**Solution**: Ensure proper cleanup in completion handler

### Issue 3: Scheduling Conflicts
**Problem**: Multiple scheduling attempts for the same task
**Solution**: Use scheduling locks (already implemented)

### Phase 8: Task Completion Tracking in Process Functions

Since we're moving runningPercentage calculation to process functions, we must also track marketplace completion there:

```typescript
// Add to each process function after updating runningPercentage
async function processOpenseaCollectionBid(data: { task: ITask, collectionDetails: any }) {
  // ... existing code ...
  
  // Update running percentage
  updateTaskRunningPercentage(taskId);
  
  // Check if this marketplace is complete
  const parentJob = parentJobs.get(parentId);
  if (parentJob && parentJob.completedJobs + parentJob.failedJobs === parentJob.totalJobs) {
    // Mark this marketplace as complete
    if (!completedTasks[taskId]) {
      completedTasks[taskId] = {
        opensea: false,
        magiceden: false,
        blur: false
      };
    }
    completedTasks[taskId][marketplace as 'opensea' | 'magiceden' | 'blur'] = true;
    console.log(GREEN + `Task ${taskId} completed for ${marketplace}` + RESET);
    
    // Check if all marketplaces are complete
    checkTaskCompletionAndScheduleNext(taskId);
  }
}
```

### Phase 9: Additional Helper Functions for Scheduling

#### 9.1 Enhanced Task Completion and Scheduling
```typescript
async function checkTaskCompletionAndScheduleNext(taskId: string) {
  const task = activeTasks.get(taskId);
  if (!task || !task.running) return;
  
  // Get enabled marketplaces for this task
  const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
  const availableMarketplaces = selectedMarketplaces.filter(m =>
    enabledMarketplaces.map(em => em.toLowerCase()).includes(m)
  );
  
  // Check if all marketplaces are complete
  const allMarketsComplete = availableMarketplaces.every(m =>
    completedTasks[taskId]?.[m as 'opensea' | 'magiceden' | 'blur']
  );
  
  if (allMarketsComplete && taskStatus[taskId].runningPercentage >= 100) {
    console.log(GREEN + `Task ${taskId} completed for all marketplaces` + RESET);
    
    // Handle trait/token removal cancellations
    await handleTaskCleanup(taskId);
    
    // Update task status
    taskStatus[taskId].status = 'COMPLETED';
    
    // Clear removal tracking
    delete traitsToRemove[taskId];
    delete removedTokens[taskId];
    
    // Wait before scheduling next loop (unless loopInterval is 0)
    if (task.loopInterval !== 0) {
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    // Schedule next loop
    await scheduleNextLoop(taskId);
  }
}
```

#### 9.2 Loop Scheduling with Immediate Rescheduling Support
```typescript
async function scheduleNextLoop(taskId: string) {
  const task = activeTasks.get(taskId);
  if (!task || !task.running) return;
  
  let loopInterval = getExpiry(task.loopInterval);
  
  // Handle immediate rescheduling (loopInterval = 0)
  if (loopInterval === 0) {
    console.log(YELLOW + `Task ${taskId} has loopInterval of 0, rescheduling immediately` + RESET);
    loopInterval = 0.1; // 100ms delay to prevent blocking
  } else if (loopInterval < 0) {
    console.log(YELLOW + `Task ${taskId} has negative loopInterval (${loopInterval}), using minimum interval` + RESET);
    loopInterval = 1; // Minimum 1 second for negative values
  }
  
  // Check loop counter limits
  const currentLoop = (taskLoopCounters.get(taskId) || 0) + 1;
  if (currentLoop > MAX_LOOP_COUNT) {
    console.error(RED + `Task ${taskId} exceeded maximum loop count (${MAX_LOOP_COUNT}). Stopping task.` + RESET);
    await stopTask(task, false);
    return;
  }
  
  taskLoopCounters.set(taskId, currentLoop);
  console.log(GREEN + `Scheduling task ${taskId} to run again in ${loopInterval} seconds (Loop #${currentLoop})` + RESET);
  
  // Set up next loop time
  const nextLoopTime = new Date(Date.now() + loopInterval * 1000);
  const secondsUntilNextLoop = Math.floor((nextLoopTime.getTime() - Date.now()) / 1000);
  
  taskStatus[taskId].status = 'SCHEDULED';
  taskStatus[taskId].waitingTime = secondsUntilNextLoop;
  taskStatus[taskId].nextLoopTime = nextLoopTime;
  
  // Clear existing timers
  clearTaskTimers(taskId);
  
  // Set up countdown timer (skip for immediate rescheduling)
  if (loopInterval > 1) {
    const countdownTimer = setInterval(() => {
      const remainingSeconds = taskStatus[taskId]?.nextLoopTime ?
        Math.max(0, Math.floor((taskStatus[taskId].nextLoopTime.getTime() - Date.now()) / 1000)) : 0;
      
      if (taskStatus[taskId]) {
        taskStatus[taskId].waitingTime = remainingSeconds;
      }
      
      if (remainingSeconds <= 0) {
        clearInterval(countdownTimer);
        taskCountdownTimers.delete(taskId);
      }
    }, 1000);
    
    taskCountdownTimers.set(taskId, countdownTimer);
  }
  
  // Validate task is still active
  if (!activeTasks.has(taskId)) {
    console.log(YELLOW + `Task ${taskId} no longer active, skipping loop scheduling` + RESET);
    cleanupTaskResources(taskId);
    return;
  }
  
  // Acquire scheduling lock
  if (!acquireLoopSchedulingLock(taskId)) {
    console.log(YELLOW + `Task ${taskId} already has a pending loop schedule, skipping duplicate scheduling` + RESET);
    return;
  }
  
  // Schedule the next iteration
  const timeoutId = setTimeout(async () => {
    console.log(YELLOW + `[Loop Timer] Timeout fired for task ${taskId}, loop #${currentLoop}` + RESET);
    try {
      releaseLoopSchedulingLock(taskId);
      
      const currentTask = activeTasks.get(taskId);
      if (currentTask?.running) {
        console.log(YELLOW + `[Loop ${currentLoop}] Starting next iteration for task ${taskId}` + RESET);
        await startTask(currentTask, true);
      } else {
        console.log(YELLOW + `Task ${taskId} no longer running, cleaning up...` + RESET);
        cleanupTaskResources(taskId);
      }
    } catch (error) {
      console.error(RED + `Error in loop scheduling for task ${taskId}: ` + RESET, error);
      cleanupTaskResources(taskId);
    }
  }, loopInterval * 1000);
  
  taskTimeouts.set(taskId, timeoutId);
}
```

#### 9.3 Timer Management Utilities
```typescript
function clearTaskTimers(taskId: string) {
  // Clear existing timeout
  const existingTimeout = taskTimeouts.get(taskId);
  if (existingTimeout) {
    clearTimeout(existingTimeout);
    taskTimeouts.delete(taskId);
  }
  
  // Clear existing countdown timer
  const existingCountdownTimer = taskCountdownTimers.get(taskId);
  if (existingCountdownTimer) {
    clearInterval(existingCountdownTimer);
    taskCountdownTimers.delete(taskId);
  }
}

async function handleTaskCleanup(taskId: string) {
  const task = activeTasks.get(taskId);
  if (!task) return;
  
  const wallet = await walletCache.getWalletById(task.walletId);
  const WALLET_PRIVATE_KEY = decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
  const { currencySymbol } = await calculateBidPrice(task, 0, "opensea");
  
  let totalCancellationJobs = 0;
  
  // Cancel removed trait bids
  if (traitsToRemove[taskId]?.length > 0) {
    const traitJobs = await cancelRemovedTraitBids(traitsToRemove[taskId], task._id, WALLET_PRIVATE_KEY, currencySymbol);
    totalCancellationJobs += traitJobs;
  }
  
  // Cancel removed token bids
  if (removedTokens[taskId]?.opensea?.length > 0 || removedTokens[taskId]?.magiceden?.length > 0) {
    const tokenJobs = await cancelRemovedTokenBids(removedTokens[taskId], task._id, WALLET_PRIVATE_KEY, currencySymbol);
    totalCancellationJobs += tokenJobs;
  }
  
  return totalCancellationJobs;
}
```

#### 9.4 Reset Completed Tasks on Start
Add to scheduled bid functions:
```typescript
// In processOpenseaScheduledBid, processMagicedenScheduledBid, processBlurScheduledBid
// Reset completed tasks for this task at the beginning
if (!completedTasks[taskId]) {
  completedTasks[taskId] = {
    opensea: false,
    magiceden: false,
    blur: false
  };
}
// Reset this marketplace's completion status
completedTasks[taskId][marketplace as 'opensea' | 'magiceden' | 'blur'] = false;
```

## Important Notes on Running Percentage Calculation

### Including Failed Jobs
The runningPercentage calculation MUST include both completed and failed jobs:

```typescript
const runningPercentage = totalJobs > 0 ? ((completedJobs + failedJobs) / totalJobs) * 100 : 0;
```

This ensures that:
- Tasks with errors still show progress
- Tasks complete even if some bids fail
- Users can see the true progress including failures

### Handling Immediate Rescheduling (loopInterval = 0)
When loopInterval is 0:
- Task should reschedule immediately after completion
- Use a minimal delay (100ms) to prevent blocking
- Skip the 1-second wait normally added before scheduling
- Skip countdown timer setup for performance

## Additional Considerations

### Thread Safety
Since multiple process functions can complete simultaneously:
- Consider using locks for completedTasks updates
- Ensure atomic operations for marketplace completion tracking
- Prevent race conditions in scheduling

### Error Recovery
- If scheduling fails, properly clean up resources
- Log all scheduling errors for debugging
- Ensure task doesn't get stuck in limbo

### Memory Management
- Clear completedTasks entries when task stops
- Clean up all timers and locks on task removal
- Prevent memory leaks from abandoned task data

### WebSocket Updates
- Ensure taskStatus updates are broadcast when scheduling changes
- Send completion notifications to connected clients
- Update UI state to reflect scheduling status

## Conclusion

This refactoring improves the accuracy and responsiveness of task progress tracking while maintaining backward compatibility and system stability. The modular approach allows for gradual implementation and easy rollback if issues arise. The addition of proper completion tracking and flexible rescheduling (including immediate rescheduling with loopInterval = 0) ensures tasks can run continuously at maximum efficiency when needed.