# Hybrid Job Tracking Implementation

## Problem
Tasks were getting stuck at 99.01% completion because `parentJob.totalJobs` was set based on preprocessing counts before jobs were actually added to the queue. Some jobs were filtered out during `processBulkJobs`, creating a mismatch where `completedJobs` could never reach `totalJobs`.

## Solution: Hybrid Tracking Approach

### 1. Updated BulkJobParent Interface
Added new fields to track both expected and actual job counts:
```typescript
interface BulkJobParent {
  parentId: string;
  totalJobs: number;
  completedJobs: number;
  startTime: number;
  taskId: string;
  jobName: string;
  // Optional bid tracking fields (only used for cancellation jobs)
  totalBids?: number;        // Total number of bids to cancel
  completedBids?: number;    // Number of successfully cancelled bids
  // Hybrid tracking fields for accurate job counting
  expectedJobs?: number;      // Jobs expected from preprocessing
  actualJobs?: number;        // Jobs actually added to queue
  allJobsQueued?: boolean;    // Flag when queueing is complete
}
```

### 2. Modified processBulkJobs
- Tracks actual jobs added per marketplace
- Updates parent jobs with `actualJobs` count after all chunks processed
- Sets `allJobsQueued = true` when done
- Calls `updateTaskRunningPercentage()` after marking all jobs as queued
- Adjusts `totalJobs` to match `actualJobs` when there's a mismatch

### 3. Updated Marketplace Processing Functions

#### OpenSea (Token, Trait, Collection)
- Sets both `expectedJobs` and `totalJobs` to preprocessing count initially
- processBulkJobs handles actual tracking for token/trait bids
- Collection bids set `actualJobs = 1` and `allJobsQueued = true` immediately

#### Blur (Trait, Collection)
- Trait bids: Sets both `expectedJobs` and `totalJobs` to preprocessing count
- Collection bid: Sets `expectedJobs = 1`, `totalJobs = 1`, then `actualJobs = 1` and `allJobsQueued = true`
- Calls `updateTaskRunningPercentage()` after collection bid is queued

#### MagicEden (Token, Trait, Collection)
- Token/Trait bids: Sets both `expectedJobs` and `totalJobs` to preprocessing count
- Collection bid: Sets `expectedJobs = 1`, `totalJobs = 1`, then `actualJobs = 1` and `allJobsQueued = true`
- Calls `updateTaskRunningPercentage()` after collection bid is queued

### 4. Updated Core Functions

#### updateTaskRunningPercentage
- Uses `actualJobs` instead of `totalJobs` when `allJobsQueued = true`
- Logs when using actualJobs for transparency
- Maintains accurate progress during both queueing and processing phases

#### checkAllParentJobsComplete
- Uses `actualJobs` when available and `allJobsQueued = true`
- Falls back to `totalJobs` during queueing phase

#### reconcileTaskStatuses
- Uses hybrid tracking to determine actual job counts
- Fixes inconsistencies using the most accurate count available

## Benefits
1. **Accurate Progress During Queueing**: Shows reasonable percentages based on expected jobs
2. **Accurate Final Counts**: Uses actual queued jobs for completion detection
3. **No More Stuck Tasks**: Tasks complete at exactly 100% when all actual jobs are done
4. **Transparent Tracking**: Logs show when switching from expected to actual counts

## Testing
The server builds successfully with these changes. The hybrid approach ensures:
- Progress percentages are meaningful during job queueing
- Tasks complete properly when all actually queued jobs finish
- No phantom jobs cause stuck percentages