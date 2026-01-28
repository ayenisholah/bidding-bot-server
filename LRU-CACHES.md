# LRU Caches Documentation

This document lists all LRU (Least Recently Used) caches in the bidding-bot-server, including their maximum size and TTL (Time To Live) configurations.

**Last Updated**: 2026-01-28

---

## Global State Caches
**File**: `src/state/global-state.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `processedOwnBids` | 100,000 | 60s | Deduplication of own bid events (prevents duplicate counting within 60s window) |
| `collectionFloorCache` | 1,000 | 5min | Per-collection floor price state with refresh tracking for non-blocking /api/floor |

**Total Memory**: ~200KB max (processedOwnBids: 80KB + collectionFloorCache: 200KB)

---

## OpenSea Marketplace Caches

### OpenSea Bid Caches
**File**: `src/marketplace/opensea/bid.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `insufficientBalanceLogCache` | 1,000 | 5s | Deduplication of insufficient balance error logs |
| `traitFloorLRU` | 10,000 | 60s | Hot cache for trait floor prices during counterbidding |
| `contractResolutionLRU` | 5,000 | 2h | Token ID → contract address resolution |
| `multiContractSlugLRU` | 500 | 5min | Multi-contract collection detection |
| `contractMapLRU` | 500 | 2h | Full token→contract mapping for collections |
| `walletCache` | 50 | 60s | Cached wallet instances (hashed private keys) |

**Total Memory**: ~25MB typical

### OpenSea API Caches
**File**: `src/marketplace/opensea/cache.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `collectionDetailsLRU` | 500 | 24h | Collection metadata (name, description, etc.) |
| `floorPriceLRU` | 500 | 5min | Collection floor prices |
| `balanceLRU` | 100 | 5min | Wallet WETH balances |
| `traitFloorLRU` | 20,000 | 5min | Trait-specific floor prices |

**Total Memory**: ~30MB max (collectionDetailsLRU: 10MB + traitFloorLRU: 20MB)

---

## MagicEden Marketplace Caches
**File**: `src/marketplace/magiceden/bid.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `insufficientBalanceLogCache` | 1,000 | 5s | Deduplication of insufficient balance error logs |
| `walletCache` | 50 | 60s | Cached wallet instances |

**Total Memory**: ~5KB

---

## Blur Marketplace Caches
**File**: `src/marketplace/blur/bid.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `insufficientBalanceLogCache` | 1,000 | 5s | Deduplication of insufficient balance error logs |

**Total Memory**: ~1KB

---

## Utility Caches

### WebSocket Manager
**File**: `src/utils/websocket-manager.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `forwardingLatencySamples` | 1,000 | 1h | Proxy→Server latency samples for performance monitoring |

**Total Memory**: ~120KB

### Logs Cache
**File**: `src/utils/logs-cache.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `cache` (BidLogEntry) | 100,000 | 24h (dynamic per task) | Task bid logs with messageHash deduplication |

**Total Memory**: ~200KB typical (2000 logs × 100 bytes per task)

### Approval Cache
**File**: `src/utils/approval-cache.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `cache` (ApprovalCacheEntry) | 50,000 | 24h | ERC20 approval status cache (wallet-token-marketplace combos) |

**Total Memory**: ~66KB max

### Balance Helpers
**File**: `src/utils/balance-helpers.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `loggedBalanceErrors` | 1,000 | 30min | Deduplication of balance error logs |

**Total Memory**: ~1KB

### Rarity Cache
**File**: `src/utils/rarity-cache.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `cache` (RarityData) | 5,000 | 24h | Token rarity mappings (tokenIds by rarity rank) |

**Total Memory**: ~5-10MB

### Token Trait Cache
**File**: `src/utils/token-trait-cache.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `hotCache` (TokenTraitData) | 100,000 | 24h | Token trait data from OpenSea GraphQL |

**Total Memory**: ~5-10KB typical (depends on activity)

### Marketplace Approval Cache
**File**: `src/utils/marketplace-approval-cache.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `cache` (MarketplaceApprovalEntry) | 1,000 | No TTL | Marketplace approval status (persists until restart) |

**Total Memory**: ~66KB max

### Wallet Cache
**File**: `src/utils/wallet-cache.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `cache` (CachedWallet) | 100 | No TTL | Wallet metadata (address, name, encryptedPrivateKey) |

**Total Memory**: ~1MB max

### Trading Disabled Cache
**File**: `src/utils/trading-disabled-cache.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `cache` (TradingDisabledInfo) | 5,000 | 4h | Tokens with "Trading is disabled" errors from OpenSea |

**Total Memory**: ~5KB

### Listing Price Cache
**File**: `src/utils/listing-price-cache.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `priceCache` (ListingPriceData) | 10,000 | 30min | Listing prices for price monitoring |

**Total Memory**: ~10MB

### Crypto / Decryption Cache
**File**: `src/utils/crypto.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `decryptedKeyCache` | 1,000 | No TTL | Decrypted private keys (eliminates 50-300ms scryptSync blocking) |

**Total Memory**: ~66KB max

### Collection Cache (Simplified)
**File**: `src/utils/collection-cache.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `collectionDetailsCache` | 500 | 7d | Collection metadata (replaces Redis-based cache) |
| `floorPriceCache` | 1,000 | 5min | Floor prices with TTL jitter (13.5-16.5min spread) |

**Total Memory**: ~5MB

### Order Cache
**File**: `src/utils/order-cache.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `orderLRU` (CachedOrder) | 100,000 | No TTL | Active order cache with manual expiration via expiresAt field |

**Total Memory**: ~20MB typical

---

## Service Caches

### Counterbid Tracking Service
**File**: `src/services/counterbid-tracking.service.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `pendingCounterbids` | 10,000 | 60s | Track tokens with pending counterbid jobs |
| `marketplaceTopOfferCache` | 10,000 | 5min | Marketplace top offer cache for counterbid decision-making |
| `executionLatencySamples` | 1,000 | 1h | Execution latency samples for performance monitoring |

**Total Memory**: ~160KB

### Task State Persistence Service
**File**: `src/services/task-state-persistence.service.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `taskStatusCache` | 500 | 24h | Task status cache for crash recovery |
| `loopCountCache` | 500 | 24h | Loop counter cache for crash recovery |
| `lastScheduleCache` | 500 | 24h | Last schedule time cache for crash recovery |
| `nextLoopCache` | 1,500 | 24h | Next loop time cache (500 tasks × 3 marketplaces) |
| `taskStartedCache` | 500 | 24h | Task start time cache for crash recovery |

**Total Memory**: ~1-2MB total

### Floor Price Warmer Service
**File**: `src/services/floor-price-warmer.service.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `warmingSlugs` | 100 | 15min | Collections to keep warm (stops after 15min of inactivity) |

**Total Memory**: ~10KB

---

## Index / Main Application Caches
**File**: `src/index.ts`

| Cache Name | Max Size | TTL | Purpose |
|------------|----------|-----|---------|
| `processedMessages` | 100,000 | 60s | WebSocket message deduplication (Layer 3) |
| `shutdownStateCache` | 10 | 24h | Graceful shutdown state coordination |
| `seenOrderHashes` | 100,000 | 60s | Order hash deduplication (Layer 3) |
| `errorLogCache` | 5,000 | 5s | Error log deduplication (prevents log flooding) |

**Total Memory**: ~16MB (processedMessages: 8MB + seenOrderHashes: 8MB)

---

## Summary Statistics

### Total Caches: 47

### By Category:
- **Global State**: 2 caches
- **OpenSea**: 10 caches
- **MagicEden**: 2 caches
- **Blur**: 1 cache
- **Utilities**: 14 caches
- **Services**: 9 caches
- **Main Application**: 4 caches

### Memory Footprint:
- **Typical Usage**: ~100-200MB total across all caches
- **Maximum Theoretical**: ~1.3GB (if all caches at max capacity simultaneously)
- **Largest Caches**:
  1. `orderLRU` - 100K entries (20MB typical)
  2. `processedMessages` - 100K entries (8MB)
  3. `seenOrderHashes` - 100K entries (8MB)
  4. `logs-cache` - 100K entries (200KB typical, task-dependent)
  5. `tokenTraitCache.hotCache` - 100K entries (5-10KB typical)

### TTL Distribution:
- **No TTL (persists until restart)**: 5 caches
- **< 1 minute**: 10 caches
- **1-5 minutes**: 10 caches
- **5-30 minutes**: 4 caches
- **1-24 hours**: 15 caches
- **> 24 hours**: 3 caches

### Size Distribution:
- **< 1,000 entries**: 15 caches
- **1,000-10,000 entries**: 17 caches
- **10,000-100,000 entries**: 10 caches
- **100,000+ entries**: 5 caches

---

## Performance Characteristics

### Cache Hit Rates (Expected):
- **Hot path caches** (counterbidding): >95% hit rate
- **API result caches** (collection details, floor prices): >85% hit rate
- **Deduplication caches**: 100% effectiveness within TTL window

### Latency Improvements:
- **LRU cache access**: <1ms (in-memory lookup)
- **vs Redis network**: 10-50ms saved per operation (90-98% faster)
- **vs MongoDB**: 50-200ms saved per operation (95-99% faster)
- **vs API calls**: 500-2500ms saved per operation (99%+ faster)

### Memory Management:
- **Automatic TTL expiration**: All caches with TTL auto-purge expired entries
- **LRU eviction**: Oldest entries evicted when max capacity reached
- **Manual cleanup**: Some caches have explicit cleanup on task completion
- **No memory leaks**: All caches bounded with predictable memory footprint

---

## Notes

1. **TTL Values**: All TTL values are in milliseconds in code but displayed in human-readable format above
2. **No TTL**: Caches with `ttl: 0` persist until server restart or manual eviction
3. **Update Behavior**: Most caches use `updateAgeOnGet: false` to avoid extending TTL on reads
4. **Stale Entries**: Some caches allow stale entries (`allowStale: true`) as fallback during API failures
5. **Migration**: Many of these caches replaced previous Redis-based implementations for better performance

---

## Maintenance

When modifying cache configurations:
1. Ensure max size is appropriate for expected usage (2-5x safety buffer recommended)
2. Set TTL based on data volatility and memory constraints
3. Consider cache hit rate vs memory trade-off
4. Update this documentation when adding/removing caches
5. Monitor cache statistics via health endpoints for optimization opportunities
