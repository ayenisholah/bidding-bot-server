# MAGICEDEN_SCHEDULE Timeout Analysis

## Root Cause Analysis

### 1. **Hardcoded 90-Second Worker Timeout**
```javascript
// Line 1318 in index.ts
const jobTimeout = 90000; // 90 seconds for all jobs
```

All jobs have a hardcoded 90-second timeout at the worker level, regardless of their complexity.

### 2. **Heavy Operations in processMagicedenScheduledBid**

The function performs multiple time-consuming operations sequentially:

#### a) **Wallet Decryption** (Line 10188)
```javascript
const WALLET_PRIVATE_KEY: string = decryptPrivateKey(wallets?.encryptedPrivateKey as string)
```

#### b) **Token Fetching** (Line 10216)
```javascript
const bottomListings = amount ? await fetchMagicEdenTokens(currentTask._id, currentTask.contract.contractAddress, amount) ?? [] : []
```
This fetches tokens from MagicEden API which can be slow for large collections.

#### c) **Trait-based Token Fetching** (Lines 10235-10271)
For trait-based bidding, it makes multiple API calls:
```javascript
await pQueue.addAll(
  expandedAttributes.map((attribute) => async () => {
    const items = await fetchOpenSeaCollectionItems(variable);
    tokens.push(...items);
  })
)
```

#### d) **Stale Token Cancellation** (Lines 10316-10330)
```javascript
for (let i = 0; i < magicedenTokensToCancel.length; i += 500) {
  const batchOrders = magicedenTokensToCancel.slice(i, i + 500);
  const extractedOrderIds = await extractMagicedenOrderHash(batchOrders)
  await queue.add(CANCEL_MAGICEDEN_BID, { ... });
}
```

#### e) **Bulk Job Processing** (Line 10359)
```javascript
await processBulkJobs(jobs, true);
```

### 3. **Performance Bottlenecks**

1. **Sequential Processing**: Many operations are done sequentially instead of in parallel
2. **API Rate Limits**: External API calls to MagicEden and OpenSea can be slow
3. **Large Data Sets**: Collections with many tokens/traits require processing large arrays
4. **Database Queries**: Multiple database queries for orders and cancellations

### 4. **Specific Issues for Collection "good-vibes-club"**

Based on the error message, this specific collection might have:
- Large number of tokens
- Complex trait combinations
- Many stale orders requiring cancellation
- API rate limiting from MagicEden

## Recommendations

### 1. **Increase Worker Timeout**
```javascript
// For SCHEDULE jobs, use a longer timeout
const jobTimeout = isScheduleJob ? 180000 : 90000; // 3 minutes for schedule jobs
```

### 2. **Optimize Operations**

#### a) **Parallelize Independent Operations**
```javascript
const [walletData, bottomListings, collectionStats] = await Promise.all([
  walletCache.getWalletById(currentTask.walletId),
  amount ? fetchMagicEdenTokens(...) : Promise.resolve([]),
  fetchMagicEdenCollectionStats(...)
]);
```

#### b) **Batch Database Operations**
```javascript
// Instead of multiple queries, use a single aggregated query
const orderStats = await prisma.order.groupBy({
  by: ['identifier'],
  where: { taskId: task._id, marketplace: 'magiceden' },
  _count: true
});
```

#### c) **Implement Caching**
- Cache collection stats for 5 minutes
- Cache trait data for 10 minutes
- Cache token listings for 2 minutes

### 3. **Add Progress Tracking**
```javascript
console.log(`[MAGICEDEN_SCHEDULE] Progress: Fetching tokens (${Date.now() - checkpointStart}ms)`);
// After each major operation
```

### 4. **Implement Circuit Breaker**
For collections that consistently timeout:
```javascript
if (taskCircuitBreaker.isOpen(task._id, 'magiceden')) {
  console.log(`Circuit breaker open for ${task._id}, skipping`);
  return;
}
```

### 5. **Split Large Operations**
For collections with many tokens, split into smaller chunks:
```javascript
if (tokenIds.length > 100) {
  // Process in chunks of 100
  for (let i = 0; i < tokenIds.length; i += 100) {
    const chunk = tokenIds.slice(i, i + 100);
    await processTokenChunk(chunk);
  }
}
```

## Immediate Actions

1. **Monitor Specific Collections**: Add logging to identify which collections timeout most often
2. **Implement Retry Logic**: Use exponential backoff for API calls
3. **Add Timeout Warnings**: Log when operations take longer than expected
4. **Collection-Specific Limits**: Set lower token limits for problematic collections

## Long-term Solutions

1. **Refactor to Event-Driven Architecture**: Break down monolithic functions
2. **Implement Job Queuing**: Use separate queues for different operation types
3. **Add Distributed Processing**: Split work across multiple workers
4. **Implement Streaming**: Process tokens as they arrive instead of waiting for all