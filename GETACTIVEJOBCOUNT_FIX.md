# getActiveJobCount TypeError Fix

## Problem
The `getActiveJobCount` function was throwing a TypeError:
```
[getActiveJobCount] Error counting active jobs for task 6881f92dc2516a94e8ff5de2: TypeError: Cannot read properties of undefined (reading 'name')
    at /Users/titan/projects/nfttools/bidding-bot/server/src/index.ts:2421:15
```

## Root Cause
The error occurred because:
1. `queue.getJobs(['active', 'waiting'])` could return jobs that are `null` or `undefined`
2. Jobs might be removed/completed between when we get the list and when we filter them
3. Some jobs might not have the expected properties due to race conditions

## Solution Implemented

### 1. Added Null/Undefined Checks
Updated the filter operation to check if the job exists before accessing its properties:
```typescript
const taskJobs = activeJobs.filter(job =>
  job && job.name === jobName && job.data && job.data._id === taskId
);
```

### 2. Optimized the Function
The function was also optimized for better performance:

#### Before:
- Always checked all 8 job types regardless of selected marketplaces
- Called `queue.getJobCounts()` inside the loop for each job type
- Called `queue.getJobs()` multiple times

#### After:
- Only checks job types for selected marketplaces
- Calls `queue.getJobCounts()` once
- Calls `queue.getJobs()` once
- Early returns if no jobs in queue or no marketplaces selected
- Checks if task exists before proceeding

### 3. Performance Improvements
1. **Reduced API calls**: From potentially 8+ calls to 2 calls maximum
2. **Early returns**: Exits early if no active jobs or no marketplaces
3. **Marketplace filtering**: Only checks relevant job types
4. **Single job retrieval**: Gets all jobs once instead of per job type

## Benefits
1. **Prevents crashes**: Null checks prevent TypeErrors
2. **Better performance**: Fewer queue operations
3. **More accurate**: Only counts jobs for selected marketplaces
4. **Maintainable**: Cleaner code structure

## Testing
To verify the fix:
1. Run tasks with various marketplace combinations
2. Monitor for TypeError exceptions
3. Check that job counts are accurate
4. Verify task completion still waits for all jobs