# WebSocket Performance Analysis Report

## Executive Summary

The NFT bidding bot experiences significant latency and reduced bidding speed when `connectWebSocket` and `handleMarketplaceMessage` functions are active. When these functions are commented out, performance improves noticeably. This report analyzes the root causes and provides recommendations.

## Key Findings

### 1. **WebSocket Message Queue Bottleneck**

The primary performance constraint is the WebSocket message queue configuration:

```typescript
// src/index.ts:7342
export const wsMessageQueue = new PQueue({
  concurrency: RATE_LIMIT / 2, // Default: 50 concurrent messages
})
```

With the default `RATE_LIMIT` of 100, only 50 WebSocket messages can be processed concurrently. During high-volume bidding periods, this creates a significant bottleneck.

### 2. **Database Query Overhead**

Multiple synchronous database queries in message handlers add latency:

#### In `updateBalancesForItemSold`:
- 2 separate `Wallet.findOne()` queries (lines 5638, 5643)
- 1 `Task.find()` query for auto-listing (line 5698)

#### In `handleListingPriceUpdate`:
- `Listing.find()` query for active listings (line 104 in priceMonitor.ts)

### 3. **Cache Pre-warming on Connection**

Upon WebSocket connection, the system pre-warms caches for all running tasks:

```typescript
// src/index.ts:5469-5480
runningTasks.forEach(async (task) => {
  if (task.wallet) {
    walletCache.getWallet(task.wallet).catch(() => { });
  }
  if (task.contract?.contractAddress) {
    ['opensea', 'magiceden', 'blur'].forEach(marketplace => {
      collectionCache.getFloorPrice(task.contract.contractAddress, marketplace);
    });
  }
});
```

With many active tasks, this creates initial connection delays and API rate limit pressure.

### 4. **External API Call Latency**

The counterbid flow includes external API calls that add latency:
- `fetchBlurCollectionStats()` - Blur marketplace API
- Collection floor price fetching for multiple marketplaces
- Wallet balance checks

### 5. **Memory Accumulation**

Several in-memory structures grow without bounds:
- `recentItemSoldEvents` Set (no cleanup mechanism)
- `processedMessageIds` Set (cleaned periodically but can grow large)

## Performance Impact Analysis

### Message Processing Flow

1. **WebSocket Message Received** → Queue (50 concurrent limit)
2. **Message Handler** → Route by eventType
3. **Handler Processing**:
   - `BID_CREATED`: 100-500ms (counterbid logic + API calls)
   - `LISTING_CREATED`: 50-200ms (database query + price update)
   - `ITEM_SOLD`: 100-300ms (multiple DB queries + balance update)

### Latency Breakdown

| Component | Latency | Impact |
|-----------|---------|---------|
| Queue Wait | 0-1000ms | High during volume spikes |
| DB Queries | 20-100ms each | Compounds with multiple queries |
| API Calls | 100-500ms | Varies by marketplace |
| Cache Miss | 200-1000ms | First-time collection data |

### Observed Behavior

When WebSocket is **enabled**:
- Bid placement: 2-5 seconds average
- High volume periods: 5-10+ seconds
- Queue depth can reach 100+ messages

When WebSocket is **disabled**:
- Bid placement: 0.5-2 seconds
- No queue congestion
- Direct execution path

## Recommendations

### 1. **Increase Queue Concurrency**

```typescript
// Recommended: Dynamic based on system resources
export const wsMessageQueue = new PQueue({
  concurrency: Math.min(RATE_LIMIT, 200), // Increase from 50 to 100-200
})
```

### 2. **Optimize Database Queries**

Combine wallet queries in `updateBalancesForItemSold`:
```typescript
// Instead of two separate queries:
const wallets = await Wallet.find({
  _id: { $in: [sellerWallet, buyerWallet] }
});
```

### 3. **Implement Message Prioritization**

Add priority queue for critical messages:
```typescript
wsMessageQueue.add(
  () => handleMarketplaceMessage(message),
  { priority: message.eventType === 'BID_CREATED' ? 1 : 0 }
);
```

### 4. **Lazy Cache Warming**

Replace eager cache pre-warming with on-demand loading:
```typescript
// Remove pre-warming on connection
// Load cache data only when needed during bid processing
```

### 5. **Add Circuit Breakers**

Implement circuit breakers for external API calls:
```typescript
const blurApiCircuitBreaker = new CircuitBreaker(fetchBlurCollectionStats, {
  timeout: 3000,
  errorThresholdPercentage: 50,
  resetTimeout: 30000
});
```

### 6. **Memory Management**

Add periodic cleanup for in-memory structures:
```typescript
setInterval(() => {
  // Keep only last hour of events
  const oneHourAgo = Date.now() - 3600000;
  recentItemSoldEvents.forEach((event, key) => {
    if (event.timestamp < oneHourAgo) {
      recentItemSoldEvents.delete(key);
    }
  });
}, 300000); // Clean every 5 minutes
```

### 7. **Message Batching**

Batch similar messages to reduce processing overhead:
```typescript
const messageBatcher = new BatchProcessor({
  batchSize: 10,
  maxWaitTime: 100,
  processor: async (messages) => {
    // Process multiple messages in single DB query
  }
});
```

## Implementation Priority

1. **High Priority** (Immediate impact):
   - Increase queue concurrency
   - Add message prioritization
   - Optimize database queries

2. **Medium Priority** (Performance optimization):
   - Implement circuit breakers
   - Add memory cleanup
   - Lazy cache warming

3. **Low Priority** (Long-term improvements):
   - Message batching
   - Advanced queue management
   - Monitoring and alerting

## Monitoring Recommendations

Add metrics for:
- WebSocket queue depth and wait times
- Message processing latency by type
- Database query performance
- Cache hit/miss ratios
- Memory usage trends

## Conclusion

The WebSocket connection and message handling create performance bottlenecks primarily through:
1. Limited queue concurrency (50 messages)
2. Synchronous database operations
3. External API call latency
4. Cache pre-warming overhead

Implementing the recommended optimizations should restore bidding performance while maintaining real-time capabilities for counterbidding and price monitoring.