# Collection Bid Stuck Percentage Analysis

## Problem Summary

Collection bids can get stuck at 0% completion due to a race condition between job execution and parent job creation.

## Root Cause

1. **Early Return Pattern**: When `enabledMarketplaces.length` is 0 or task is not running, collection bid processors call `completeCollectionBidJob()` immediately:
   ```typescript
   // In processOpenseaCollectionBid, processBlurCollectionBid, processMagicedenCollectionBid
   if (!enabledMarketplaces.length) {
     completeCollectionBidJob(taskId, marketplace);
     return;
   }
   ```

2. **Silent Failure**: The `completeCollectionBidJob` function silently does nothing if parent job doesn't exist:
   ```typescript
   function completeCollectionBidJob(taskId: string, marketplace: string, success: boolean = true) {
     const parentId = `${taskId}-${marketplace}`.toLowerCase();
     const parentJob = parentJobs.get(parentId);
     
     if (parentJob) {  // <-- Silent failure if parent job doesn't exist
       // Update tracking...
     }
   }
   ```

3. **Parent Job Creation Timing**: Parent jobs are created in the scheduling functions (OPENSEA_SCHEDULE, BLUR_SCHEDULE, MAGICEDEN_SCHEDULE) BEFORE the collection bid job is queued:
   ```typescript
   // In schedule functions
   else if (collectionBid) {
     const parentJobId = `${currentTask._id}-${marketplace}`.toLowerCase()
     
     parentJobs.set(parentJobId, {
       parentId: parentJobId,
       totalJobs: 1,
       completedJobs: 0,
       failedJobs: 0,
       startTime: Date.now(),
       taskId: currentTask._id,
       jobName: COLLECTION_BID_JOB_NAME
     })
     
     await queue.add(COLLECTION_BID_JOB_NAME, { task: newTask }, options);
   }
   ```

## Problematic Scenarios

1. **No Enabled Marketplaces**: If `enabledMarketplaces` is empty when job runs, it calls `completeCollectionBidJob` before any parent job exists
2. **Task Stopped**: If task is stopped between scheduling and execution, similar issue occurs
3. **Marketplace Not Selected**: If marketplace is removed from selection between scheduling and execution

## Solution Approaches

### Option 1: Create Parent Job in completeCollectionBidJob if Missing
```typescript
function completeCollectionBidJob(taskId: string, marketplace: string, success: boolean = true) {
  const parentId = `${taskId}-${marketplace}`.toLowerCase();
  let parentJob = parentJobs.get(parentId);
  
  // Create parent job if it doesn't exist
  if (!parentJob) {
    parentJob = {
      parentId: parentId,
      totalJobs: 1,
      completedJobs: 0,
      failedJobs: 0,
      startTime: Date.now(),
      taskId: taskId,
      jobName: `${marketplace.toUpperCase()}_COLLECTION_BID`
    };
    parentJobs.set(parentId, parentJob);
  }
  
  // Continue with normal logic...
}
```

### Option 2: Don't Call completeCollectionBidJob on Early Returns
Simply return without calling `completeCollectionBidJob` when there are no enabled marketplaces or task isn't running.

### Option 3: Add Logging for Missing Parent Jobs
At minimum, log when parent job is missing to help diagnose stuck tasks:
```typescript
if (parentJob) {
  // existing logic
} else {
  console.warn(`[${marketplace}] Parent job not found for task ${taskId} - cannot update completion tracking`);
}
```

## Verification

To verify this is the issue:
1. Check logs for tasks stuck at 0% - look for early returns due to empty `enabledMarketplaces`
2. Check if `parentJobs.get(parentId)` returns undefined for stuck tasks
3. Monitor if stuck tasks have the pattern of being scheduled but then having marketplaces disabled

## Related Code Locations

- `processOpenseaCollectionBid` (line ~9100)
- `processBlurCollectionBid` (line ~9540)  
- `processMagicedenCollectionBid` (line ~11515)
- `completeCollectionBidJob` (line ~2439)
- `updateTaskRunningPercentage` (line ~2336)
- Schedule functions that create parent jobs for collection bids