# OpenSea Functions Optimization Plan

## Overview
This document outlines the optimization plan for all OpenSea-related functions to improve speed and memory performance. The optimizations focus on:
- Adding Redis caching with proper key patterns
- Using Promise.all for parallel operations
- Implementing early returns
- Making non-blocking operations where possible
- Caching sensitive market data with appropriate TTLs

## Redis Key Pattern
All Redis keys will follow the cluster-friendly pattern: `{hashslot}:key` to ensure keys are distributed properly across the Redis cluster.

## Caching Strategy

### 1-Minute TTL (Sensitive Market Data)
- `getOpenseaFloor()` - Floor prices change frequently
- `fetchOpenseaListings()` - Active listings change rapidly
- `openseaTraitFloor()` - Trait floor prices are volatile
- `fetchERC20Balance()` - Wallet balances change with transactions

### 1-Hour TTL (Static Collection Data)
- `getCollectionDetails()` - Collection metadata rarely changes
- `fetchOpenSeaCollectionItems()` - Collection items are relatively static

### No Caching (Real-time Bidding Data)
- `fetchItemOffers()` - Offers need real-time accuracy
- `fetchOpenseaOffers()` - Current offers must be up-to-date

### Memory-Only Caching
- Wallet data and decrypted private keys (security sensitive)

## Function-by-Function Optimization Plan

### 1. processOpenseaScheduledBid (src/index.ts)

**Current Issues:**
- Sequential operations that could be parallelized
- No caching for collection details and floor prices
- Redundant marketplace checks

**Optimizations:**
```typescript
// Early return consolidation
if (!enabledMarketplaces.map(m => m.toLowerCase()).includes("opensea")) return;
const currentTask = activeTasks.get(task._id);
if (!currentTask?.running || !currentTask.selectedMarketplaces.map(m => m.toLowerCase()).includes("opensea")) return;

// Parallel operations using Promise.all
const [wallets, collectionDetails, bottomListings] = await Promise.all([
  walletCache.getWalletById(currentTask.walletId),
  cachedGetCollectionDetails(currentTask.contract.slug, currentTask.contract.contractAddress),
  autoIds ? cachedFetchOpenseaListings(currentTask._id, currentTask.contract.slug, amount) : Promise.resolve([])
]);

// Non-blocking operations
Promise.resolve().then(() => {
  taskStatus[currentTask._id].processingPercentage = (1 / 4) * 100;
});
```

### 2. processOpenseaCollectionBid (src/index.ts)

**Current Issues:**
- Sequential wallet and floor price fetching
- No caching for floor prices
- Multiple database queries that could be batched

**Optimizations:**
```typescript
// Parallel data fetching
const [wallets, floor_price, orders] = await Promise.all([
  walletCache.getWalletById(task.walletId),
  cachedGetOpenseaFloor(currentTask.contract.slug),
  prisma.order.findMany({
    where: {
      slug: currentTask.contract.slug,
      taskId: currentTask._id,
      marketplace: "opensea",
      identifier: 'collection',
    },
    orderBy: { createdAt: 'desc' }
  })
]);

// Early return for floor price
if (!floor_price) return;

// Non-blocking queue operations
if (remainingOrders.length > 0) {
  queue.addBulk(cancelData); // Remove await
}
```

### 3. processOpenseaTraitBid (src/index.ts)

**Current Issues:**
- Sequential floor price and trait floor fetching
- No caching for trait floor prices
- Redundant trait existence checks

**Optimizations:**
```typescript
// Early trait validation
const traitExists = traitsArray.some(t => t.type === parsedTrait.type && t.value === parsedTrait.value);
if (!traitExists) return;

// Parallel floor price fetching
const [floor_price, traitFloor] = await Promise.all([
  cachedGetOpenseaFloor(slug),
  currentTask?.stopOptions?.traitPrices && identifier in currentTask?.stopOptions?.traitPrices
    ? cachedOpenseaTraitFloor(slug, { traitType: type, values: [value] })
    : Promise.resolve(undefined)
]);
```

### 4. processOpenseaTokenBid (src/index.ts)

**Current Issues:**
- Sequential operations
- No caching for floor prices
- Synchronous trading disabled check

**Optimizations:**
```typescript
// Early return for trading disabled
if (tradingDisabledCache.isDisabled(asset.contractAddress, +asset.tokenId, 'opensea')) {
  logBidError(_id, "TRADING DISABLED (CACHED)", message, "skipped", "opensea"); // Non-blocking
  return;
}

// Parallel operations
const [floor_price, orders] = await Promise.all([
  cachedGetOpenseaFloor(slug),
  prisma.order.findMany({
    where: {
      slug: slug,
      taskId: _id,
      marketplace: "opensea",
      identifier: asset.tokenId.toString()
    },
    orderBy: { createdAt: 'desc' }
  })
]);
```

### 5. bidOnOpensea (src/marketplace/opensea/bid.ts)

**Current Issues:**
- Sequential balance check and offer building
- No caching for wallet balances
- Synchronous fee calculations

**Optimizations:**
```typescript
// Early validation
const task = activeTasks.get(taskId);
if (!task?.running || !task.selectedMarketplaces.map(m => m.toLowerCase()).includes("opensea")) return;

// Cached balance check
const wethBalance = await cachedFetchERC20Balance(wallet_address, currency);

// Parallel operations for collection offers
if (!asset) {
  const [buildPayloadResult, counter] = await Promise.all([
    buildOffer(taskId, buildPayload),
    SEAPORT_CONTRACT.getCounter(wallet_address)
  ]);
}

// Non-blocking tracking
trackBidRate('opensea', taskId); // Remove await
```

### 6. fetchOpenseaListings (src/marketplace/opensea/bid.ts)

**Current Issues:**
- No caching for listings
- Sequential pagination
- Array operations in loop

**Optimizations:**
```typescript
export async function fetchOpenseaListings(taskId: string, collectionSlug: string, limit?: number) {
  if (!limit) return;
  
  // Check Redis cache first
  const cacheKey = `{opensea:listings}:${collectionSlug}:${limit}`;
  const redis = getRedis();
  
  try {
    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached);
  } catch (error) {
    // Continue without cache
  }
  
  // Existing logic...
  const uniqueTokenIds = await fetchListingsFromAPI(collectionSlug, limit);
  
  // Cache with 1-minute TTL
  try {
    await redis.setex(cacheKey, 60, JSON.stringify(uniqueTokenIds));
  } catch (error) {
    // Continue without caching
  }
  
  return uniqueTokenIds;
}
```

### 7. openseaTraitFloor (src/marketplace/opensea/bid.ts)

**Current Issues:**
- No caching for trait floor prices
- Single API call without retry logic

**Optimizations:**
```typescript
export async function openseaTraitFloor(slug: string, trait: { traitType: string; values: string[] }) {
  const cacheKey = `{opensea:trait-floor}:${slug}:${trait.traitType}:${trait.values.join(',')}`;
  const redis = getRedis();
  
  try {
    const cached = await redis.get(cacheKey);
    if (cached) return parseFloat(cached);
  } catch (error) {
    // Continue without cache
  }
  
  // Existing API call...
  const floor = await fetchTraitFloorFromAPI(slug, trait);
  
  // Cache with 1-minute TTL
  if (floor !== undefined) {
    try {
      await redis.setex(cacheKey, 60, floor.toString());
    } catch (error) {
      // Continue without caching
    }
  }
  
  return floor;
}
```

### 8. getOpenseaFloor (src/functions/collection.ts)

**Current Issues:**
- No Redis caching
- In-memory cache only

**Optimizations:**
```typescript
export async function getOpenseaFloor(slug: string) {
  const cacheKey = `{opensea:floor}:${slug}`;
  const redis = getRedis();
  
  try {
    const cached = await redis.get(cacheKey);
    if (cached) return parseFloat(cached);
  } catch (error) {
    // Continue without cache
  }
  
  // Existing API call...
  const floor = await fetchFloorFromAPI(slug);
  
  // Cache with 1-minute TTL
  if (floor !== undefined) {
    try {
      await redis.setex(cacheKey, 60, floor.toString());
    } catch (error) {
      // Continue without caching
    }
  }
  
  return floor;
}
```

### 9. getCollectionDetails (src/functions/collection.ts)

**Current Issues:**
- In-memory cache only
- No Redis caching for persistence

**Optimizations:**
```typescript
export async function getCollectionDetails(slug: string, contractAddress?: string) {
  // Check memory cache first
  if (collectionCache[slug]) return collectionCache[slug];
  
  const cacheKey = `{opensea:collection}:${slug}`;
  const redis = getRedis();
  
  // Check Redis cache
  try {
    const cached = await redis.get(cacheKey);
    if (cached) {
      const details = JSON.parse(cached);
      collectionCache[slug] = details; // Update memory cache
      return details;
    }
  } catch (error) {
    // Continue without cache
  }
  
  // Use existing lock mechanism and API call...
  const details = await fetchCollectionDetailsFromAPI(slug);
  
  // Cache in Redis with 1-hour TTL
  try {
    await redis.setex(cacheKey, 3600, JSON.stringify(details));
  } catch (error) {
    // Continue without caching
  }
  
  return details;
}
```

### 10. fetchOpenSeaCollectionItems (src/functions/collection.ts)

**Current Issues:**
- No caching
- Sequential GraphQL queries

**Optimizations:**
```typescript
export async function fetchOpenSeaCollectionItems(variables: CollectionItemsQueryVariables) {
  const cacheKey = `{opensea:items}:${crypto.createHash('md5').update(JSON.stringify(variables)).digest('hex')}`;
  const redis = getRedis();
  
  try {
    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached);
  } catch (error) {
    // Continue without cache
  }
  
  // Existing GraphQL query...
  const items = await fetchItemsFromGraphQL(variables);
  
  // Cache with 1-hour TTL
  try {
    await redis.setex(cacheKey, 3600, JSON.stringify(items));
  } catch (error) {
    // Continue without caching
  }
  
  return items;
}
```

### 11. fetchERC20Balance (src/utils/balance.ts)

**Current Issues:**
- No caching for balance checks
- Multiple calls for same wallet/token

**Optimizations:**
```typescript
export async function fetchERC20Balance(walletAddress: string, tokenAddress: string) {
  const cacheKey = `{balance}:${walletAddress}:${tokenAddress}`;
  const redis = getRedis();
  
  try {
    const cached = await redis.get(cacheKey);
    if (cached) return parseFloat(cached);
  } catch (error) {
    // Continue without cache
  }
  
  // Existing balance check...
  const balance = await fetchBalanceFromBlockchain(walletAddress, tokenAddress);
  
  // Cache with 1-minute TTL
  try {
    await redis.setex(cacheKey, 60, balance.toString());
  } catch (error) {
    // Continue without caching
  }
  
  return balance;
}
```

## General Optimization Patterns

### 1. Early Returns
- Consolidate validation checks at the beginning of functions
- Return immediately on invalid conditions
- Avoid nested if statements

### 2. Promise.all Usage
- Identify independent async operations
- Group them in Promise.all for parallel execution
- Handle errors appropriately with try-catch

### 3. Non-blocking Operations
- Remove unnecessary awaits for operations that don't need immediate results
- Use Promise.resolve().then() for fire-and-forget operations
- Queue operations don't need to be awaited unless checking result

### 4. Redis Caching Implementation
```typescript
// Helper function for Redis operations
async function getCachedOrFetch<T>(
  cacheKey: string, 
  ttl: number, 
  fetchFn: () => Promise<T>
): Promise<T> {
  const redis = getRedis();
  
  try {
    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached);
  } catch (error) {
    console.error(`Redis get error for ${cacheKey}:`, error);
  }
  
  const result = await fetchFn();
  
  try {
    await redis.setex(cacheKey, ttl, JSON.stringify(result));
  } catch (error) {
    console.error(`Redis set error for ${cacheKey}:`, error);
  }
  
  return result;
}
```

### 5. Memory-Only Caching for Sensitive Data
- Keep wallet instances in memory Map
- Cache decrypted private keys in memory with TTL
- Never persist sensitive data to Redis

## Implementation Priority

1. **High Priority** (Biggest performance impact)
   - Add Redis caching to getOpenseaFloor, fetchOpenseaListings, openseaTraitFloor
   - Parallelize operations in processOpenseaScheduledBid
   - Cache getCollectionDetails and fetchOpenSeaCollectionItems

2. **Medium Priority** (Moderate performance impact)
   - Optimize processOpenseaCollectionBid and processOpenseaTraitBid
   - Add balance caching with short TTL
   - Make queue operations non-blocking

3. **Low Priority** (Minor performance impact)
   - Consolidate early returns
   - Remove unnecessary awaits
   - Optimize array operations

## Testing Strategy

1. **Performance Testing**
   - Measure response times before and after optimization
   - Monitor Redis hit rates
   - Check memory usage patterns

2. **Load Testing**
   - Test with high concurrent task loads
   - Verify cache invalidation works correctly
   - Ensure no race conditions with parallel operations

3. **Error Handling**
   - Test Redis connection failures
   - Verify fallback to non-cached operations
   - Ensure no data inconsistency

## Monitoring and Metrics

1. **Cache Metrics**
   - Hit/miss rates for each cache key pattern
   - Average TTL remaining
   - Cache eviction rates

2. **Performance Metrics**
   - Function execution times
   - API call rates
   - Queue processing times

3. **Error Metrics**
   - Redis connection errors
   - Cache operation failures
   - API timeout rates