# Debugging Cancellation Percentage Issue

## Summary of Logging Added

I've added comprehensive logging throughout the cancellation flow to help debug why the cancellation percentage stays at 0% when `stopTask` is called with a marketplace parameter:

### 1. In `stopTask` function (lines 8433-8448):
- Logs when cancellation tracking is initialized with total bid count
- Logs marketplace-specific totals for global cancellations
- Logs the tracking status after initialization

### 2. In `updateTaskCancellationPercentage` function (lines 7959-7966):
- Logs when the function is called with parameters
- Shows all active trackings with their keys, totals, completed counts, and percentages
- Logs which percentage calculation method is used (aggregated vs regular)
- Logs the final percentage value being set

### 3. In `cancellation-tracker.ts` utility:
- `initializeTracking`: Logs when tracking is initialized with key and total bids
- `recordCompletion`: Logs when completions are recorded with current progress
- `getPercentage`: Logs percentage calculations and results

### 4. In marketplace-specific cancellation functions:
- **`cancelOpenseaBids`** (line 10224): Logs start of cancellation and job creation details
- **`cancelMagicedenBids`** (lines 10149-10205): Logs cancellation start, tracker status, batch processing, and job creation
- **`cancelBlurBids`** (lines 10211-10235): Logs cancellation start, tracker status, and job creation

### 5. In bulk cancellation processing functions:
- **`bulkCancelOpenseaBid`** (lines 17546-17548): Logs when completion is recorded for each order
- **`bulkCancelMagicedenBid`** (lines 17571-17609): Logs batch processing and completion recording
- **`bulkCancelBlurBid`** (lines 17719-17755): Logs individual order processing and completion recording

## Key Areas to Watch in Logs:

1. **Initialization**: Check if `cancellationTracker.initializeTracking` is called with the correct marketplace parameter
2. **Job Creation**: Verify that cancellation jobs are actually being created for the marketplace
3. **Completion Recording**: Ensure `recordCompletion` is being called when jobs complete
4. **Percentage Calculation**: Watch the `getPercentage` logs to see if the tracker has the right data

## Potential Issues to Look For:

1. **Marketplace Key Mismatch**: The tracker might be initialized with one key but completions recorded with another
2. **Timing Issue**: Completions might be recorded before tracking is initialized
3. **Total Count Mismatch**: The total bid count might not match the actual number of orders being cancelled
4. **Async Race Condition**: The percentage might be calculated before any completions are recorded

With these comprehensive logs, you should be able to trace the entire flow and identify exactly where the cancellation percentage calculation is failing when `stopTask` is called with a marketplace parameter.