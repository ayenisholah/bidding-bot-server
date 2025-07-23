# Timeout Retry Fix - Implementation Report

## Summary
Updated the timeout error handling to allow 1 retry for jobs that timeout after 30 seconds, addressing the issue where Magic Eden jobs were timing out without any retry attempts.

## Changes Made

### Modified Timeout Error Handling
**File**: `/src/index.ts` (lines 1365-1401)

**Before**: Jobs that timed out would immediately fail without retries
**After**: Jobs that timeout are allowed 1 retry (total 2 attempts)

### Implementation Details

1. **Added retry logic for timeout errors**:
   - `maxTimeoutAttempts = 2` (1 initial attempt + 1 retry)
   - Checks if current attempt is the last allowed attempt
   - Updates console logging to show attempt count

2. **Conditional retry behavior**:
   - If not the last attempt: Throws error to trigger BullMQ retry
   - If last attempt: Returns failure without throwing (prevents further retries)

3. **Maintained existing protections**:
   - 30-second timeout remains unchanged (prevents memory buildup)
   - AbortController still cancels long-running operations
   - Memory monitoring continues to delay jobs when heap > 80%

## Expected Behavior

### First Timeout (Attempt 1/2):
```
[Timeout] Job 687eb980ea18832391540715-magiceden-Body-Puffy Jacket Cosmic-good-vibes-club timed out (attempt 1/2) - will retry once
```
Job will be retried after exponential backoff delay (8 seconds)

### Second Timeout (Attempt 2/2):
```
[Timeout] Job 687eb980ea18832391540715-magiceden-Body-Puffy Jacket Cosmic-good-vibes-club timed out (attempt 2/2) - max attempts reached
```
Job will fail permanently without further retries

## Benefits

1. **Improved success rate**: Transient network issues or temporary API slowness won't cause immediate failures
2. **Maintained memory safety**: 30-second timeout still prevents memory exhaustion
3. **Balanced approach**: Single retry gives jobs a second chance without excessive retries
4. **Clear visibility**: Console logs show attempt progress

## Configuration

- Initial timeout: 30 seconds
- Max attempts for timeout errors: 2 (1 initial + 1 retry)
- Backoff delay: 8 seconds (exponential)
- Regular job attempts: 3 (unchanged for non-timeout errors)

## Monitoring

Watch for:
- Reduced permanent failures for Magic Eden jobs
- Jobs succeeding on retry after initial timeout
- Memory usage remaining stable despite retries
- Timeout stats in health monitor showing retry patterns