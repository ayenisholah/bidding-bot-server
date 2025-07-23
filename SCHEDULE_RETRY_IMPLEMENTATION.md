# SCHEDULE Job Retry Implementation

## Summary
Implemented differentiated retry behavior to ensure SCHEDULE jobs (OPENSEA_SCHEDULE, MAGICEDEN_SCHEDULE, BLUR_SCHEDULE) always retry indefinitely while all other jobs fail immediately without retries. This prevents tasks from getting stuck when SCHEDULE jobs fail.

## Problem
- SCHEDULE jobs coordinate task execution and must succeed for tasks to progress
- Other jobs (bids, cancellations) should fail fast to prevent memory buildup
- Previously, all jobs followed the same retry policy which could leave tasks stuck

## Implementation

### 1. Updated createJobOptions Function
**File**: `/src/index.ts` (lines 1908-1923)

```typescript
function createJobOptions(jobId?: string, timeoutMs: number = 60000, jobName?: string): JobsOptions {
  const isScheduleJob = jobName && [OPENSEA_SCHEDULE, MAGICEDEN_SCHEDULE, BLUR_SCHEDULE].includes(jobName);
  
  return {
    ...(jobId ? { jobId } : {}),
    removeOnComplete: true,
    removeOnFail: true,
    lifo: false,
    attempts: isScheduleJob ? -1 : 1, // Unlimited for SCHEDULE, no retries for others
    backoff: {
      type: 'exponential',
      delay: 8000,
    },
  };
}
```

### 2. Updated Timeout Error Handling
**File**: `/src/index.ts` (lines 1354-1392)

- Checks if job is a SCHEDULE type
- SCHEDULE jobs: Log and throw error for retry
- Other jobs: Log and return failure immediately

### 3. Updated General Error Handling
**File**: `/src/index.ts` (lines 1394-1463)

- Added isScheduleJob check for all error types
- Non-SCHEDULE jobs: Call moveToFailed() and return without throwing
- SCHEDULE jobs: Always throw error to trigger BullMQ retry

### 4. Updated Job Creation Calls

- **Individual job creation** (line 3887): Pass job name to createJobOptions
- **Marketplace updates** (lines 5899-5918): Use createJobOptions with job name
- **Bulk job creation** (lines 4126-4157): Map jobs with createJobOptions

## Behavior After Implementation

### SCHEDULE Jobs (Task Orchestration)
- **Attempts**: Unlimited (-1)
- **On timeout**: Retry indefinitely
- **On error**: Always retry
- **Purpose**: Ensure tasks never get stuck

### Other Jobs (Bids, Cancellations)
- **Attempts**: 1 (no retries)
- **On timeout**: Fail immediately
- **On error**: Fail immediately
- **Purpose**: Prevent memory buildup from retries

## Expected Console Output

### SCHEDULE Job Timeout:
```
[Timeout] SCHEDULE job 12345 timed out - will retry indefinitely to prevent task from getting stuck
```

### Regular Job Timeout:
```
[Timeout] Job 67890 timed out - failing immediately (no retries for non-SCHEDULE jobs)
```

## Benefits

1. **Task Reliability**: Tasks will never get stuck due to failed SCHEDULE jobs
2. **Memory Safety**: Bid/cancel jobs fail fast without consuming memory on retries
3. **Clear Separation**: Different retry policies for orchestration vs execution
4. **Better Observability**: Console logs clearly indicate retry behavior

## Monitoring

- SCHEDULE jobs will show high attempt counts in logs
- Non-SCHEDULE jobs will show max 1 attempt
- Health monitor timeout stats will show which job types are timing out
- Tasks should progress even if individual bid jobs fail