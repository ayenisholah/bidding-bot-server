# Fix for Tasks Stuck at RUNNING Status

## Problem
Tasks with `runningPercentage = 100%` are stuck at `taskStatus.status = 'RUNNING'` instead of transitioning to `COMPLETED` → `SCHEDULED`.

## Root Causes

### 1. Nested Conditions Issue
The completion logic requires TWO separate conditions to be met:
```typescript
if (parentJob.completedJobs + parentJob.failedJobs === parentJob.totalJobs) {
  // Mark marketplace complete
  completedTasks[taskId][marketplace] = true;
  
  if (allMarketsComplete && runningPercentage > 99) {
    // Only here does it transition to COMPLETED
  }
}
```

**Problem**: `runningPercentage` can reach 100% but the nested condition might not trigger if:
- Not all individual marketplace parent jobs have completed
- The `completedTasks` object isn't properly updated
- Race conditions between different marketplace completions

### 2. Missing Fallback Logic
There's no fallback to check if `runningPercentage = 100%` and transition to `COMPLETED` regardless of the marketplace completion tracking.

### 3. Global State Issues
The `completedTasks` global object can become inconsistent with the actual job completion state.

## Solution

### Option 1: Add Fallback Completion Check

Add this logic after the existing completion logic (around line 1836):

```typescript
// FALLBACK: If runningPercentage is 100% but task hasn't transitioned to COMPLETED
if (runningPercentage >= 100 && taskStatus[taskId].status === 'RUNNING') {
  console.log(YELLOW + `[FALLBACK] Task ${taskId} reached 100% but didn't complete normally, forcing completion` + RESET);
  
  // Force completion transition
  const wallet = await Wallet.findById(task.walletId);
  const WALLET_PRIVATE_KEY = await decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
  const { currencySymbol } = await calculateBidPrice(task, 0, "magiceden");

  // Handle cleanup
  if (traitsToRemove[taskId]?.length > 0) {
    await cancelRemovedTraitBids(traitsToRemove[taskId], task._id, WALLET_PRIVATE_KEY, currencySymbol);
  }
  
  if (removedTokens[taskId]?.opensea?.length > 0 || removedTokens[taskId]?.magiceden?.length > 0) {
    await cancelRemovedTokenBids(removedTokens[taskId], task._id, WALLET_PRIVATE_KEY, currencySymbol);
  }

  // Transition to completed
  taskStatus[taskId].status = 'COMPLETED';
  delete traitsToRemove[taskId];
  delete removedTokens[taskId];
  await new Promise(resolve => setTimeout(resolve, 1000));

  // Schedule next loop if task is still running
  if (task && task.running) {
    const loopInterval = getExpiry(task.loopInterval);
    const nextLoopTime = new Date(Date.now() + loopInterval * 1000);
    const secondsUntilNextLoop = Math.floor((nextLoopTime.getTime() - Date.now()) / 1000);

    console.log(GREEN + `[FALLBACK] Scheduling task ${taskId} to run again in ${loopInterval} seconds` + RESET);
    taskStatus[taskId].status = 'SCHEDULED';
    taskStatus[taskId].waitingTime = secondsUntilNextLoop;
    taskStatus[taskId].nextLoopTime = nextLoopTime;

    // Set up countdown and timeout (same as existing logic)
    const countdownTimer = setInterval(() => {
      const remainingSeconds = taskStatus[taskId].nextLoopTime ? Math.max(0, Math.floor((taskStatus[taskId].nextLoopTime.getTime() - Date.now()) / 1000)) : 0
      taskStatus[taskId].waitingTime = remainingSeconds;
      if (remainingSeconds <= 0) {
        clearInterval(countdownTimer);
      }
    }, 1000);

    const existingTimeout = taskTimeouts.get(taskId);
    if (existingTimeout) {
      clearTimeout(existingTimeout);
    }

    const timeoutId = setTimeout(async () => {
      const currentTask = activeTasks.get(taskId);
      if (currentTask?.running) {
        await startTask(currentTask, true);
      } else if (currentTask) {
        await stopTask(currentTask, false);
      }
    }, loopInterval * 1000);

    taskTimeouts.set(taskId, timeoutId);
  }
  
  return; // Exit early since we handled completion
}
```

### Option 2: Fix the Original Logic

**Problem in the original logic**: The check `allMarketsComplete` might be failing due to inconsistent `completedTasks` state.

Replace the completion check (around line 1770):

```typescript
// OLD: Unreliable check
const allMarketsComplete = availableMarketplaces.every(m =>
  completedTasks[taskId]?.[m as 'opensea' | 'magiceden' | 'blur']
);

// NEW: More reliable check based on actual job completion
const allMarketsComplete = availableMarketplaces.every(marketplace => {
  const marketplaceParentId = `${taskId}-${marketplace}`.toLowerCase();
  const marketplaceParentJob = parentJobs.get(marketplaceParentId);
  
  if (!marketplaceParentJob) {
    console.warn(`[Completion] No parent job found for ${taskId}-${marketplace}`);
    return true; // Assume complete if no job exists
  }
  
  const isComplete = (marketplaceParentJob.completedJobs + marketplaceParentJob.failedJobs) >= marketplaceParentJob.totalJobs;
  console.log(`[Completion] ${taskId}-${marketplace}: ${marketplaceParentJob.completedJobs + marketplaceParentJob.failedJobs}/${marketplaceParentJob.totalJobs} = ${isComplete}`);
  return isComplete;
});
```

### Option 3: Add Monitoring and Auto-Recovery

Add this monitoring function to detect and fix stuck tasks:

```typescript
// Add this as a periodic check (every 30 seconds)
function detectAndFixStuckTasks() {
  Object.keys(taskStatus).forEach(taskId => {
    const status = taskStatus[taskId];
    const task = activeTasks.get(taskId);
    
    // Detect stuck RUNNING tasks
    if (status.status === 'RUNNING' && status.runningPercentage >= 100 && task?.running) {
      const stuckTime = Date.now() - (status.lastUpdate || 0);
      
      if (stuckTime > 30000) { // Stuck for more than 30 seconds
        console.warn(`[RECOVERY] Detected stuck task ${taskId} at 100% for ${stuckTime}ms, forcing completion`);
        
        // Force transition to completed
        status.status = 'COMPLETED';
        
        // Trigger the scheduling logic
        setTimeout(async () => {
          const currentTask = activeTasks.get(taskId);
          if (currentTask?.running) {
            await startTask(currentTask, true);
          }
        }, 1000);
      }
    }
  });
}

// Add to your intervals
setInterval(detectAndFixStuckTasks, 30000); // Every 30 seconds
```

## Implementation Steps

1. **Immediate fix**: Add Option 1 (fallback logic) right after line 1836
2. **Better fix**: Implement Option 2 (fix original logic) 
3. **Monitoring**: Add Option 3 (auto-recovery) for ongoing monitoring

## Testing

After implementing the fix:
1. Start multiple tasks
2. Let them complete a loop
3. Check that `runningPercentage = 100%` tasks transition to `COMPLETED` then `SCHEDULED`
4. Monitor logs for "FALLBACK" messages (should be minimal after fix)

## Debug Endpoint

Add this endpoint to help identify stuck tasks:

```typescript
app.get('/debug/stuck-tasks', (req, res) => {
  const stuckTasks = [];
  
  Object.keys(taskStatus).forEach(taskId => {
    const status = taskStatus[taskId];
    const task = activeTasks.get(taskId);
    
    if (status.status === 'RUNNING' && status.runningPercentage >= 100) {
      stuckTasks.push({
        taskId,
        status: status.status,
        runningPercentage: status.runningPercentage,
        taskRunning: task?.running || false,
        lastUpdate: status.lastUpdate,
        completedTasks: completedTasks[taskId]
      });
    }
  });
  
  res.json({ stuckTasks });
});
```