# Running Percentage > 100% Fix

## Problem
The running percentage was exceeding 100% (reaching values like 365.8%) because the `parentJobs` Map entries were not being cleared between loops. This caused job counters to accumulate indefinitely across all loop iterations.

## Root Cause
- Each loop was incrementing the same `parentJob` counters
- MagicEden showed totalJobs=1 but had processed 312+ jobs
- OpenSea showed totalJobs=116 but counters kept growing with each loop
- The `completeCollectionBidJob` function was incrementing counters without checking if they belonged to the current loop

## Issues This Caused
1. **Incorrect Progress Reporting**: Users saw confusing percentages like 365.8%
2. **Memory Leak**: Counters grew indefinitely with each loop
3. **Potential Integer Overflow**: After many loops, numbers could exceed safe ranges
4. **Logic Errors**: Task completion logic relied on percentages being <= 100%

## Solution Implemented
Added code to clear `parentJobs` entries at the start of each scheduled bid function:

### In processOpenseaScheduledBid, processMagicedenScheduledBid, and processBlurScheduledBid:
```typescript
// Clear parentJobs for this task at the start of each loop
// This ensures we start fresh and don't have stale data from previous loops
const marketplacesToClear = ['opensea', 'magiceden', 'blur'];
for (const marketplace of marketplacesToClear) {
  const parentId = `${taskId}-${marketplace}`.toLowerCase();
  if (parentJobs.has(parentId)) {
    console.log(`[${marketplace}] Clearing parentJob from previous loop for task ${taskId}`);
    parentJobs.delete(parentId);
  }
}
```

## How It Works
1. At the start of each loop, before creating new jobs, the code clears any existing parentJob entries
2. This ensures each loop starts with fresh counters (completedJobs=0, failedJobs=0)
3. The percentage calculation now accurately reflects the current loop's progress (0-100%)
4. The existing overflow protection in `completeCollectionBidJob` provides additional safety

## Result
- Running percentages now stay within 0-100% range
- Each loop has accurate progress tracking
- No more memory leak from accumulating counters
- Clear and understandable progress reporting for users

## Testing
To verify the fix:
1. Run a task with loopInterval=0 (immediate rescheduling)
2. Monitor the logs for running percentage values
3. Confirm percentages reset to 0% at the start of each loop
4. Verify percentages never exceed 100%