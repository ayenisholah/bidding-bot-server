# Bidding Function Optimization Analysis

## 1. API Request Caching Opportunities

### Collection Data Caching

**Current State**: Collection details are fetched repeatedly across multiple functions
- **Location**: `/Users/titan/projects/nfttools/bidding-bot/server/src/index.ts`
  - Line 8254: `getCollectionDetails()` in `processOpenseaScheduledBid`
  - Multiple calls across different marketplace functions

**Optimization**: Already implemented in-memory cache in `collection.ts` (line 15-16, 38-41)
```javascript
const collectionCache: { [key: string]: ICollectionDetails } = {};
```

### Floor Price Caching

**Current State**: Floor prices are fetched individually for each marketplace
- **Locations**:
  - Line 8553: `getOpenseaFloor()` in `processOpenseaCollectionBid`
  - Line 10401: `fetchMagicEdenCollectionStats()` in `processMagicedenCollectionBid`
  - Line 8908: `fetchBlurCollectionStats()` in `processBlurCollectionBid`

**Optimization**: Already implemented Redis caching with 30-second TTL in `collection.ts`:
- Lines 142-211: `getOpenseaFloor()` with Redis cache
- Lines 214-257: `getBlurFloor()` with Redis cache
- Lines 259-302: `getMagicEdenFloor()` with Redis cache

### User/Wallet Data Caching

**Current State**: Wallet data fetched repeatedly
- **Locations**:
  - Line 8550: `Wallet.findById()` in `processOpenseaCollectionBid`
  - Line 10399: `Wallet.findById()` in `processMagicedenCollectionBid`
  - Line 8905: `Wallet.findById()` in `processBlurCollectionBid`
  - Line 8618, 10461: `Wallet.find({})` for all wallets

**Optimization Needed**: Implement wallet caching
```javascript
// Add wallet cache with TTL
const walletCache = new Map();
const WALLET_CACHE_TTL = 300000; // 5 minutes

async function getCachedWallet(walletId) {
  const cached = walletCache.get(walletId);
  if (cached && cached.expires > Date.now()) {
    return cached.data;
  }
  const wallet = await Wallet.findById(walletId);
  walletCache.set(walletId, {
    data: wallet,
    expires: Date.now() + WALLET_CACHE_TTL
  });
  return wallet;
}
```

## 2. Timer and Connection Cleanup

### WebSocket Connection Management

**Current State**: WebSocket connections managed centrally but some cleanup issues
- **Location**: Line 1906-1908, 1942-1944 in SIGINT/SIGTERM handlers

**Optimization**: Already implemented but ensure proper cleanup in all exit scenarios

### Interval Timers

**Current State**: Multiple intervals running
- Line 11078: `setInterval(validateBidCount, 5000)`
- Queue cleanup interval (referenced in lines 1899-1902, 1936-1938)

**Optimization Needed**: Track all intervals for proper cleanup
```javascript
const activeIntervals = new Set();

// When creating interval
const intervalId = setInterval(validateBidCount, 5000);
activeIntervals.add(intervalId);

// In cleanup
activeIntervals.forEach(id => clearInterval(id));
activeIntervals.clear();
```

## 3. Sequential to Parallel Operations

### Bid Cancellation Operations

**Status**: Already optimized with Promise.all

### Collection Bid Processing

**Current State**: Sequential operations that could be parallelized
- **Location**: `processOpenseaCollectionBid` (lines 8541-8740)
  - Line 8550: Wallet fetch
  - Line 8553: Floor price fetch
  - Line 8584-8594: Order fetch

**Optimization**:
```javascript
// Parallelize independent operations
const [wallet, floor_price, orders] = await Promise.all([
  Wallet.findById(task.walletId),
  getOpenseaFloor(currentTask.contract.slug),
  prisma.order.findMany({
    where: {
      slug: currentTask.contract.slug,
      taskId: currentTask._id,
      marketplace: OPENSEA.toLowerCase(),
      identifier: 'collection',
    },
    orderBy: { createdAt: 'desc' }
  })
]);
```

### Scheduled Bid Token Fetching

**Current State**: Sequential trait token fetching with OR operator
- **Location**: Lines 8295-8315 in `processOpenseaScheduledBid`
- Uses `pQueue.addAll()` for parallel processing

**Status**: Already optimized with parallel queue

### Counter Bid Functions

**Current State**: Sequential operations
- **Locations**:
  - Lines 9594-9596: `openseaCollectionCounterBid` - Floor price fetch before bid
  - Lines 9638-9639: `magicedenCollectionCounterBid` - Floor price fetch before bid
  - Lines 9705-9706: `blurCollectionCounterBid` - Floor price fetch before bid

**Optimization**: These must remain sequential as floor price is needed for validation

## 4. Repeated Operations Optimization

### Duplicate Order Cancellation Logic

**Current State**: Repeated cancellation code blocks
- **Locations**:
  - Lines 8600-8608: OpenSea remaining orders cancellation
  - Lines 8662-8670: OpenSea all orders cancellation
  - Lines 10445-10449: MagicEden remaining orders cancellation
  - Lines 10514-10518: MagicEden all orders cancellation

**Optimization**: Extract to reusable function
```javascript
async function cancelMarketplaceOrders(orders, marketplace, privateKey, taskId) {
  if (orders.length === 0) return;
  
  switch(marketplace) {
    case 'opensea':
      const cancelData = orders.map(order => ({
        name: CANCEL_OPENSEA_BID,
        data: { privateKey, orderId: order.orderHash, taskId, orderKey: order.id }
      }));
      await queue.addBulk(cancelData);
      break;
    case 'magiceden':
      const orderKeys = orders.map(order => order.id);
      const extractedOrderIds = await extractMagicedenOrderHash(orders);
      await queue.add(CANCEL_MAGICEDEN_BID, { 
        orderIds: extractedOrderIds, 
        privateKey, 
        orderKeys, 
        taskId 
      });
      break;
    // ... blur case
  }
}
```

### Bid Statistics Initialization

**Current State**: Repeated initialization of skipStats and errorStats
- **Locations**:
  - Lines 8564-8570: OpenSea skipStats init
  - Lines 10536-10542: MagicEden skipStats init
  - Lines 8729-8735: OpenSea errorStats init
  - Lines 10551-10557: MagicEden errorStats init

**Optimization**: Create initialization helper
```javascript
function initializeTaskStats(taskId) {
  if (!skipStats[taskId]) {
    skipStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
  }
  if (!errorStats[taskId]) {
    errorStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
  }
  if (!floorPrices[taskId]) {
    floorPrices[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
  }
}
```

### Floor Price Validation

**Current State**: Repeated floor price > offer price validation
- **Locations**:
  - Lines 8680-8691: OpenSea collection bid
  - Lines 10499-10510: MagicEden collection bid
  - Lines 8952-8963: Blur collection bid
  - Lines 9601-9612: OpenSea counter bid
  - Lines 9646-9657: MagicEden counter bid

**Optimization**: Extract validation function
```javascript
async function validateFloorPriceLimit(offerEth, floorPrice, currencySymbol, taskId, slug, marketplace) {
  if (currencySymbol.toLowerCase() === 'weth' && offerEth > floorPrice) {
    const message = `⚠️ Offer price ${offerEth} ${currencySymbol} for ${slug} exceeds floor price ${floorPrice} ${currencySymbol} FOR ${marketplace.toUpperCase()}.Stopping bid...`;
    
    warningBids[taskId] = { opensea: true, magiceden: true, blur: true };
    warningMessage[taskId] = { opensea: message, magiceden: message, blur: message };
    
    await logBidError(taskId, "OFFER EXCEEDS FLOOR PRICE", message, "warning", marketplace);
    console.log(YELLOW + '-'.repeat(137) + RESET);
    console.log(YELLOW + message + RESET);
    console.log(YELLOW + '-'.repeat(137) + RESET);
    
    return true; // validation failed
  }
  return false; // validation passed
}
```

## 5. Memory Optimization

### Clear Completed Task Resources

**Current State**: Task resources cleanup in `cleanupTaskResources` (lines 2064-2084)
- Already clears loop counters, timeouts, and countdown timers

**Additional Optimization**: Clear task-specific caches
```javascript
function cleanupTaskResources(taskId: string): void {
  // Existing cleanup...
  
  // Clear task-specific cached data
  delete skipStats[taskId];
  delete errorStats[taskId];
  delete floorPrices[taskId];
  delete warningBids[taskId];
  delete warningMessage[taskId];
  delete currentTokenIds[taskId];
  delete currentTraits[taskId];
  delete taskStatus[taskId];
  delete completedTasks[taskId];
}
```

## 6. Rate Limiting Optimization

### Consolidate API Calls

**Current State**: Multiple API calls for the same data
- Floor prices fetched separately per marketplace
- Collection details fetched multiple times

**Optimization**: Use `getAllFloorPrices()` from collection.ts (lines 304-320)
```javascript
// Instead of individual calls:
const floor_price_opensea = await getOpenseaFloor(slug);
const floor_price_blur = await getBlurFloor(slug);
const floor_price_magiceden = await getMagicEdenFloor(slug);

// Use single parallel call:
const { opensea, blur, magiceden } = await getAllFloorPrices(slug);
```

## Summary of Key Optimizations

1. **Implement wallet caching** - High impact, reduces database queries
2. **Extract repeated validation logic** - Medium impact, improves maintainability
3. **Use getAllFloorPrices for parallel fetching** - High impact, reduces API calls by 66%
4. **Implement proper resource cleanup** - Medium impact, prevents memory leaks
5. **Track and cleanup all intervals** - Low impact, prevents resource leaks
6. **Extract order cancellation logic** - Medium impact, reduces code duplication

Most critical optimizations are already implemented (collection caching, floor price caching with Redis). The main opportunities are in reducing code duplication and implementing wallet caching.