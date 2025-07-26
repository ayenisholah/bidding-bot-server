# Collection Bid Status Fix

## Problem Summary

Collection bids were stuck at "QUEUEING" status because of inconsistent status management between collection bids and trait/token bids.

### Root Cause

1. **Trait/Token Bids**: Use `processBulkJobs` which properly manages status transitions:
   - Sets status to `QUEUEING` while adding jobs to queue
   - Sets status to `RUNNING` after all jobs are queued
   - Jobs then execute and update percentages

2. **Collection Bids**: Bypassed this flow:
   - Added single job directly to queue
   - **Immediately** set status to `RUNNING` (before job executed)
   - Job would execute but status was already RUNNING
   - Result: Status stuck at QUEUEING visually, never transitioning properly

## The Fix

Made collection bids consistent with trait/token bid flow:

### 1. Schedule Phase Changes
- **Before**: Set status to `RUNNING` immediately when scheduling
- **After**: Set status to `QUEUEING` when adding collection bid to queue

### 2. Processing Phase Changes
- Added status transition from `QUEUEING` to `RUNNING` when collection bid actually starts processing
- This matches the behavior of `processBulkJobs`

### 3. Error Handling
- Ensured all code paths call `completeCollectionBidJob` to properly update tracking
- MagicEden already had proper completion calls
- OpenSea and Blur were updated to ensure consistency

## Implementation Details

### Changed Files
- `src/index.ts`: 
  - `processOpenseaScheduledBid`: Line ~9062
  - `processBlurScheduledBid`: Line ~9526
  - `processMagicedenScheduledBid`: Line ~11491
  - `processOpenseaCollectionBid`: Added status transition
  - `processBlurCollectionBid`: Added status transition
  - `processMagicedenCollectionBid`: Added status transition and proper error handling

### Status Flow After Fix

1. **Schedule Phase**:
   - Task starts → Schedule job runs
   - Sets up parentJobs tracking
   - Adds collection bid job to queue
   - Sets status to `QUEUEING` ✓

2. **Processing Phase**:
   - Collection bid job starts processing
   - Transitions status from `QUEUEING` to `RUNNING` ✓
   - Executes bid logic
   - Calls `completeCollectionBidJob` on success/failure
   - Updates running percentage

3. **Completion Phase**:
   - All marketplaces complete
   - Status transitions to `COMPLETED` or `SCHEDULED` for next loop

## Benefits

1. **Consistent Status Management**: Collection bids now follow same status flow as trait/token bids
2. **Accurate Progress Tracking**: Running percentage correctly reflects actual progress
3. **Better User Experience**: Status accurately shows what the system is doing
4. **Easier Debugging**: Consistent behavior across all bid types

## Testing

To verify the fix:
1. Start a collection bid task
2. Watch status transitions: STARTED → QUEUEING → RUNNING → COMPLETED/SCHEDULED
3. Verify running percentage updates correctly
4. Check that all marketplaces complete properly