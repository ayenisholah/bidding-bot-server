# Queue Performance Fixes - Implementation Plan

## Overview
This document outlines fixes for two critical issues affecting the bidding bot's queue system:
1. Jobs getting stuck when waiting jobs appear
2. Repeated failed attempts on "Trading is disabled" tokens

## Issues Identified

### Issue 1: Queue Congestion from Waiting Jobs
- **Problem**: When waiting jobs accumulate, the queue stops processing
- **Symptoms**: 
  - Active jobs: 102, Waiting jobs: 10+, but no job processing
  - Memory continuously increasing
  - Workers appear blocked

### Issue 2: Trading Disabled Token Errors
- **Problem**: Tokens with "Trading is disabled" errors repeatedly fail
- **Symptoms**:
  - Continuous `[ERROR] 0x_SERVER_OPENSEA_BID_OPENSEA_BIDDING_AxiosError` logs
  - Same tokens attempted multiple times
  - Queue congestion from failed retry attempts

## Root Causes

### 1. **Dual Rate Limiting Conflict**
- Both `limiter` (Bottleneck) and `pQueue` (p-queue) are competing
- Jobs can get stuck between the two rate limiters

### 2. **LIFO Processing**
- Jobs processed Last-In-First-Out causes older jobs to get stuck
- New jobs jump ahead of waiting jobs

### 3. **No Caching for Disabled Tokens**
- Failed tokens are removed from `currentTokenIds` but jobs already queued still attempt
- No mechanism to prevent future attempts on known disabled tokens

### 4. **Memory Leaks**
- Tracking objects (`completedTasks`, `taskStatus`, etc.) never cleaned up
- Caches grow indefinitely

### 5. **Insufficient Lock Duration**
- Worker locks expire before complex operations complete
- Jobs get marked as stalled prematurely

## Planned Fixes

### Fix 1: Trading Disabled Token Cache
**New File**: `/src/utils/trading-disabled-cache.ts`
- Cache tokens that fail with "Trading is disabled" for 1 hour
- Prevent any job from attempting to bid on cached tokens
- Automatic TTL-based expiration

### Fix 2: Update processOpenseaTokenBid
**File**: `/src/index.ts`
- Add early check for cached disabled tokens
- Log skips and update error stats
- Cache tokens when "Trading is disabled" error occurs

### Fix 3: Worker Configuration Updates
**File**: `/src/index.ts`
- Change from LIFO to FIFO processing
- Increase lock duration: 60s → 120s
- Increase lock renewal: 20s → 30s
- Add job timeout handling

### Fix 4: Memory Cleanup System
**File**: `/src/index.ts`
- Periodic cleanup of tracking objects every 5 minutes
- Remove data for inactive tasks
- Force garbage collection when available

### Fix 5: Enhanced Queue Cleanup
**File**: `/src/index.ts`
- Check for and remove jobs for disabled tokens
- Resume paused queues automatically
- More aggressive cleanup interval (30s)

### Fix 6: Monitoring Enhancements
**File**: `/src/index.ts`
- Display disabled token cache statistics
- Show cache hit rates
- Better visibility into queue health

## Implementation Order

1. Create trading disabled cache utility
2. Import cache and add checks to token bid processing
3. Update error handling to populate cache
4. Fix worker configuration
5. Add memory cleanup system
6. Enhance queue cleanup function
7. Add monitoring statistics

## Expected Outcomes

- **Reduced Queue Congestion**: Disabled tokens won't clog the queue
- **Better Memory Management**: Automatic cleanup prevents unbounded growth
- **Improved Processing Order**: FIFO ensures fair job processing
- **Faster Failure Detection**: Jobs timeout instead of hanging indefinitely
- **Better Visibility**: Clear monitoring of disabled tokens and queue health

## Testing Strategy

1. Test with known disabled tokens
2. Monitor queue depths before/after implementation
3. Verify memory usage stabilizes
4. Confirm no valid jobs are skipped
5. Check cache expiration works correctly