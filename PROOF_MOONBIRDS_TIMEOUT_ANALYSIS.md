# Proof-Moonbirds Collection Timeout Analysis

## Issue Summary
The proof-moonbirds collection is causing timeouts in the NFT bidding bot due to the combination of multiple factors related to trait-based bidding and API request handling.

## Root Causes Identified

### 1. Excessive Lock TTL
- The collection fetching lock TTL is set to **36000 seconds (10 hours)** in `src/functions/collection.ts`
- This extremely long lock duration can cause subsequent requests to wait indefinitely
- If a request fails or times out while holding the lock, other requests are blocked for up to 10 hours

```typescript
const lockManager = new DistributedLockManager({
  lockPrefix: '{collection}:fetch:',
  defaultTTLSeconds: 36000  // 10 hours - TOO LONG!
});
```

### 2. Exponential API Calls with OR Operator
When using the `traitsOperator.value === "or"` setting:
- The system makes **separate API calls for each trait value combination**
- With Moonbirds having **9 trait categories** and **126 total traits**, this results in potentially hundreds of API calls
- Each call triggers pagination if there are more than 100 items

Example: If selecting 5 traits with 3 values each = 15 separate API calls minimum

### 3. Recursive Pagination Without Limits
The `fetchOpenSeaCollectionItems` function:
- Recursively fetches all pages without any pagination limits
- For large collections or complex trait queries, this can result in many sequential API calls
- No timeout handling for the entire pagination process

### 4. Redis Cluster Issues
From the logs:
```
Error reading from Redis cache for Blur stats (proof-moonbirds): Too many Cluster redirections
```
- Redis cluster is experiencing redirection issues
- This adds latency to cache operations
- Failed cache reads force API calls instead of using cached data

## Collection Characteristics

Proof-Moonbirds has:
- **10,000 total NFTs**
- **9 trait categories**
- **126 unique trait values** distributed as:
  - Background: 10 variations
  - Beak: 3 variations
  - Body: 17 variations
  - Eyes: 11 variations
  - Eyewear: 12 variations
  - Feathers: 18 variations
  - Headwear: 37 variations (largest category)
  - Outerwear: 8 variations

## Performance Impact

1. **Memory Usage**: Fetching all 10,000 token IDs into memory
2. **Network Overhead**: Hundreds of API calls for OR-based trait queries
3. **Lock Contention**: 10-hour locks block other operations
4. **Rate Limiting**: Excessive requests hit OpenSea rate limits

## Recommendations

### Immediate Fixes

1. **Reduce Lock TTL**
```typescript
defaultTTLSeconds: 300  // 5 minutes instead of 10 hours
```

2. **Add Timeout to Collection Fetching**
```typescript
const FETCH_TIMEOUT = 30000; // 30 seconds
const itemsPromise = fetchOpenSeaCollectionItems(variable);
const items = await Promise.race([
  itemsPromise,
  new Promise((_, reject) => 
    setTimeout(() => reject(new Error('Collection fetch timeout')), FETCH_TIMEOUT)
  )
]);
```

3. **Limit Pagination**
```typescript
const MAX_PAGES = 10; // Limit to 1000 items
if (nextCursor && allTokenIds.length < MAX_PAGES * 100) {
  return fetchOpenSeaCollectionItems(variables, updatedTokenIds, nextCursor);
}
```

### Long-term Solutions

1. **Batch Trait Queries**: Instead of individual API calls per trait, batch multiple traits into fewer requests
2. **Progressive Loading**: Load first N items quickly, then continue loading in background
3. **Smarter Caching**: Cache trait-specific results separately
4. **Queue Optimization**: Process trait queries in a queue with concurrency limits
5. **Fix Redis Cluster**: Resolve the cluster redirection issues

## Monitoring Recommendations

1. Add metrics for:
   - API call count per collection
   - Time spent in locks
   - Pagination depth
   - Cache hit/miss rates

2. Set alerts for:
   - Lock wait times > 30 seconds
   - API calls > 50 per task
   - Redis cluster errors

## Conclusion

The proof-moonbirds timeouts are caused by a perfect storm of:
- Large collection size (10,000 NFTs)
- Complex trait structure (126 traits across 9 categories)
- Inefficient OR-based querying (separate API call per trait)
- Excessive lock duration (10 hours)
- Redis cluster issues

The combination of these factors creates a situation where tasks can get stuck waiting for locks or making hundreds of API calls, leading to timeouts.