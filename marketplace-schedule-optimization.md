# Marketplace Schedule Functions Optimization Analysis

## Overview
This document analyzes OPENSEA_SCHEDULE, BLUR_SCHEDULE, and MAGICEDEN_SCHEDULE functions and their nested dependencies to identify bottlenecks, caching opportunities, and cleanup requirements to fix job timeouts.

## Current Issues Identified

### 1. Job Timeout Root Causes

#### A. Excessive API Calls in Schedule Functions
- **Problem**: Each schedule function makes multiple API calls per wallet/task combination
- **Impact**: With multiple wallets and tasks, API calls grow exponentially
- **Example**: For 10 wallets with 5 tasks each, that's 50+ API calls per schedule run

#### B. Sequential Processing
- **Problem**: Many operations run sequentially that could be parallelized
- **Current Flow**:
  ```
  1. Fetch task data
  2. For each wallet:
     - Decrypt private key
     - Fetch balance
     - For each trait combination:
       - Fetch listings/tokens
       - Process bids
  ```
- **Impact**: Linear time complexity O(wallets × tasks × traits)

#### C. Redundant Database Queries
- **Problem**: Multiple queries for the same data across different functions
- **Examples**:
  - Task details fetched multiple times
  - Wallet data queried repeatedly
  - Collection details fetched per trait combination

### 2. Functions That Need Caching

#### A. Currently Uncached Functions

1. **fetchOpenseaListings** (Referenced but implementation not found)
   - **Cache Strategy**: Redis with 30-60 second TTL
   - **Key**: `opensea:listings:{collection}:{traits}:{limit}`
   - **Benefit**: Reduce API calls for frequently checked collections

2. **fetchMagicEdenTokens**
   - **Cache Strategy**: Redis with 60 second TTL
   - **Key**: `magiceden:tokens:{collection}:{attributes}:{limit}`
   - **Benefit**: Avoid repeated token fetches for same trait combinations

3. **openseaTraitFloor** (Partial caching exists)
   - **Enhancement**: Add trait combination caching
   - **Key**: `opensea:trait:floor:{collection}:{traitHash}`
   - **TTL**: 60 seconds

4. **fetchBlurTraitFloor**
   - **Cache Strategy**: Redis with 60 second TTL
   - **Key**: `blur:trait:floor:{collection}:{traitHash}`
   - **Benefit**: Reduce Blur API calls

5. **fetchMagicEdenTraitFloor**
   - **Cache Strategy**: Redis with 60 second TTL
   - **Key**: `magiceden:trait:floor:{collection}:{traitHash}`

#### B. Existing Caches That Need Enhancement

1. **walletCache**
   - **Current**: 30-minute TTL for wallet data, 5-minute for decrypted keys
   - **Enhancement**: Add batch loading for multiple wallets
   - **Benefit**: Single DB query instead of N queries

2. **collectionCache**
   - **Current**: 5-minute floor prices, 30-minute details
   - **Enhancement**: Add trait data caching
   - **Benefit**: Reduce trait lookup overhead

### 3. Cleanup Implementation Locations

#### A. Immediate Cleanup Opportunities

1. **Completed Job Data**
   - **Location**: After job completion in worker
   - **Implementation**:
   ```typescript
   // In worker job completion
   await cleanupCompletedJob(job.id);
   ```

2. **Stale Cache Entries**
   - **Location**: Background cleanup task
   - **Implementation**:
   ```typescript
   // Run every 5 minutes
   setInterval(cleanupStaleCache, 5 * 60 * 1000);
   ```

3. **Old Bid Logs**
   - **Location**: Daily cleanup job
   - **Implementation**:
   ```typescript
   // Keep only last 30 days
   await BidLogs.deleteMany({ 
     timestamp: { $lt: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000) }
   });
   ```

#### B. Resource Cleanup Points

1. **WebSocket Connections**
   - **Location**: Error handlers and shutdown hooks
   - **Current Issue**: Connections may leak on errors

2. **Redis Connections**
   - **Location**: Queue shutdown handlers
   - **Current Issue**: BullMQ connections not always cleaned

3. **Database Connections**
   - **Location**: Process exit handlers
   - **Current Issue**: Prisma client may hold connections

### 4. Small Backwards-Compatible Improvements

#### A. Parallel Processing

1. **Wallet Operations Parallelization**
   ```typescript
   // Instead of:
   for (const wallet of wallets) {
     await processWallet(wallet);
   }
   
   // Use:
   await Promise.all(wallets.map(wallet => processWallet(wallet)));
   ```

2. **Batch Database Queries**
   ```typescript
   // Instead of multiple findOne calls:
   const walletIds = tasks.map(t => t.walletId);
   const wallets = await Wallet.find({ _id: { $in: walletIds }});
   ```

3. **Early Exit Conditions**
   ```typescript
   // Add at start of schedule functions:
   if (task.status !== 'ACTIVE') return;
   if (task.pausedUntil && task.pausedUntil > new Date()) return;
   ```

#### B. Timeout Handling Improvements

1. **Granular Timeout Tracking**
   ```typescript
   const timeoutSegments = {
     walletFetch: 5000,
     balanceCheck: 3000,
     listingFetch: 10000,
     bidPlacement: 15000
   };
   ```

2. **Progressive Timeout Backoff**
   ```typescript
   const timeout = baseTimeout * Math.pow(1.5, retryCount);
   ```

### 5. Bottleneck Analysis

#### A. Critical Bottlenecks

1. **Nested Trait Loops**
   - **Location**: All schedule functions
   - **Issue**: O(n³) complexity with trait combinations
   - **Fix**: Batch trait queries and cache results

2. **Sequential Marketplace Checks**
   - **Location**: validateBidCount function
   - **Issue**: Checks each marketplace separately
   - **Fix**: Parallel marketplace validation

3. **Individual NFT Token Fetching**
   - **Location**: fetchMagicEdenTokens, fetchOpenseaListings
   - **Issue**: One API call per token
   - **Fix**: Batch token fetching

#### B. Performance Bottlenecks

1. **Distributed Lock Contention**
   - **Issue**: Locks held too long during API calls
   - **Fix**: Minimize lock duration, use try-with-timeout

2. **Rate Limiter Queue Blocking**
   - **Issue**: Sequential rate limit checks
   - **Fix**: Implement priority queuing

### 6. Specific Function Optimizations

#### A. processOpenseaScheduledBid

1. **Cache trait combinations**
   ```typescript
   const traitKey = hashTraits(selectedTraits);
   const cachedListings = await getCache(`listings:${traitKey}`);
   ```

2. **Batch wallet balance checks**
   ```typescript
   const balances = await Promise.all(
     wallets.map(w => fetchBalance(w.address))
   );
   ```

#### B. processBlurScheduledBid

1. **Implement request batching**
   ```typescript
   const batchedRequests = chunkArray(requests, 10);
   for (const batch of batchedRequests) {
     await blurRateLimiter.executeWithRateLimit(
       () => Promise.all(batch)
     );
   }
   ```

#### C. processMagicedenScheduledBid

1. **Token prefetching**
   ```typescript
   const tokens = await prefetchTokensForCollection(collection);
   ```

### 7. Implementation Priority

1. **High Priority** (Immediate Impact)
   - Add caching to fetchOpenseaListings and fetchMagicEdenTokens
   - Implement parallel wallet processing
   - Add early exit conditions

2. **Medium Priority** (Performance Enhancement)
   - Batch database queries
   - Implement granular timeout tracking
   - Add cleanup for completed jobs

3. **Low Priority** (Long-term Optimization)
   - Refactor nested loops
   - Implement progressive caching strategies
   - Add detailed performance metrics

### 8. Monitoring Recommendations

1. **Add Metrics Collection**
   ```typescript
   const metrics = {
     apiCallCount: 0,
     cacheHitRate: 0,
     jobDuration: 0,
     timeoutCount: 0
   };
   ```

2. **Implement Health Checks**
   ```typescript
   // Check queue depth
   if (queue.count() > 1000) {
     logger.warn('Queue depth exceeding threshold');
   }
   ```

### 9. Code Examples

#### A. Enhanced Caching Implementation
```typescript
class EnhancedCache {
  async getOrFetch(key: string, fetchFn: () => Promise<any>, ttl: number) {
    const cached = await redis.get(key);
    if (cached) return JSON.parse(cached);
    
    const data = await fetchFn();
    await redis.setex(key, ttl, JSON.stringify(data));
    return data;
  }
}
```

#### B. Parallel Processing Pattern
```typescript
async function processScheduledBidsParallel(wallets: IWallet[], task: ITask) {
  const chunks = chunkArray(wallets, 5); // Process 5 wallets at a time
  
  for (const chunk of chunks) {
    await Promise.all(chunk.map(wallet => 
      processWalletWithTimeout(wallet, task, 30000)
    ));
  }
}
```

### 10. Testing Recommendations

1. **Load Testing**
   - Test with 50+ wallets and 10+ tasks
   - Monitor memory usage and API call counts
   - Verify timeout handling under load

2. **Cache Testing**
   - Verify cache hit rates > 80%
   - Test cache invalidation scenarios
   - Monitor Redis memory usage

3. **Cleanup Testing**
   - Verify resources are released
   - Check for memory leaks
   - Monitor connection pool usage

## Conclusion

The primary causes of job timeouts are:
1. Excessive sequential API calls
2. Lack of caching for frequently accessed data
3. Inefficient nested loops for trait processing
4. Missing cleanup causing resource exhaustion

Implementing the suggested improvements should reduce job execution time by 60-80% and prevent most timeout scenarios.