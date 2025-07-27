# Collection Cache Redis Migration Guide

## Overview
The collection cache has been updated to use Redis as a persistent L2 cache while maintaining an in-memory L1 cache for performance.

## Changes Required

### 1. All cache methods are now async

The following methods now return Promises and need to be awaited:

```typescript
// Before
const floor = collectionCache.getFloorPrice(collection, marketplace);

// After
const floor = await collectionCache.getFloorPrice(collection, marketplace);
```

### 2. Methods to Update

All occurrences of these methods need to be updated with `await`:

- `collectionCache.getFloorPrice()` → `await collectionCache.getFloorPrice()`
- `collectionCache.setFloorPrice()` → `await collectionCache.setFloorPrice()`
- `collectionCache.getCollectionDetails()` → `await collectionCache.getCollectionDetails()`
- `collectionCache.setCollectionDetails()` → `await collectionCache.setCollectionDetails()`
- `collectionCache.getCollectionStats()` → `await collectionCache.getCollectionStats()`
- `collectionCache.setCollectionStats()` → `await collectionCache.setCollectionStats()`
- `collectionCache.invalidateCollection()` → `await collectionCache.invalidateCollection()`

### 3. Redis Key Patterns

The cache now uses Redis cluster-friendly key patterns with hash tags:

- Floor prices: `{floor:[marketplace]}:[collection]`
  - Example: `{floor:opensea}:azuki`
- Collection details: `{collection:details}:[slug]`
  - Example: `{collection:details}:bayc`
- Collection stats: `{collection:stats}:[slug]`
  - Example: `{collection:stats}:doodles`

### 4. TTL Configuration

- Floor prices: 5 minutes (300 seconds)
- Collection details: 30 minutes (1800 seconds)
- Collection stats: 30 minutes (1800 seconds)

### 5. Error Handling

The cache implementation includes graceful fallback:
- If Redis is unavailable, the cache falls back to memory-only operation
- All Redis errors are logged but don't fail the operation
- The application continues to work even if Redis is down

### 6. Files to Update

Based on the grep results, these locations need to be updated:

1. **src/index.ts** - Multiple locations:
   - Line 4108: `collectionCache.getCollectionDetails()`
   - Line 6995: `collectionCache.getFloorPrice()`
   - Lines 7489, 7628, 7752: `collectionCache.getFloorPrice()` for MagicEden
   - Line 7963: `collectionCache.getCollectionDetails()`
   - Lines 8005, 8161, 8310: `collectionCache.getFloorPrice()` for OpenSea
   - Line 8509: `collectionCache.getFloorPrice()` for Blur
   - All corresponding `setFloorPrice()` calls

### 7. Example Migration

```typescript
// Before
let floor_price = collectionCache.getFloorPrice(task.contract.contractAddress, 'magiceden');

if (floor_price === null) {
  floor_price = await collectionCache.fetchWithDedup(
    `magiceden-floor-${task.contract.contractAddress}`,
    async () => {
      const price = Number(await fetchMagicEdenCollectionStats(task.contract.contractAddress));
      collectionCache.setFloorPrice(task.contract.contractAddress, 'magiceden', price);
      return price;
    }
  );
}

// After
let floor_price = await collectionCache.getFloorPrice(task.contract.contractAddress, 'magiceden');

if (floor_price === null) {
  floor_price = await collectionCache.fetchWithDedup(
    `magiceden-floor-${task.contract.contractAddress}`,
    async () => {
      const price = Number(await fetchMagicEdenCollectionStats(task.contract.contractAddress));
      await collectionCache.setFloorPrice(task.contract.contractAddress, 'magiceden', price);
      return price;
    }
  );
}
```

## Benefits

1. **Persistence**: Cache survives server restarts
2. **Cluster-wide sharing**: All server instances share the same cache
3. **Reduced API calls**: Significant reduction in external API requests
4. **Performance**: L1 memory cache provides sub-millisecond access
5. **Scalability**: Redis cluster handles distributed caching

## Monitoring

You can monitor cache performance using:
- `collectionCache.getStats()` - Returns cache statistics
- Redis MONITOR command to see cache operations
- Application logs for cache misses and errors