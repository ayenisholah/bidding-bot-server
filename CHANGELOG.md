# Changelog

All notable changes to the Bidding Bot Server will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **Duplicate Token Bids Race Condition**: Fixed race condition causing duplicate bids on the same token from scheduled bids and Token-vs-Collection defense
  - **Problem**: Meta Legend #4260 received TWO identical 0.0177 WETH bids placed 2 seconds apart from the same wallet
  - **Root Cause**: The scheduled token bid processor (`processOpenseaTokenBid`, `processMagicedenTokenBid`) and the Token-vs-Collection counterbid defense both queue bids for the same token simultaneously without coordination
  - **Timeline from Debug Logs**:
    - `23:10:46.994Z` - Scheduled Token Bid ENTRY for #4260
    - `23:10:49.241Z` - Token-vs-Collection: QUEUED at 0.0177 ETH (SECOND BID)
    - `23:10:49.570Z` - First bid confirmed
    - `23:10:50.745Z` - Second bid confirmed (DUPLICATE!)
  - **Solution**: Coordinate both bid paths through the `pendingCounterbids` cache:
    1. Scheduled bid processors now CHECK if a counterbid is already pending for the token
    2. Scheduled bid processors now MARK the token as pending before placing a bid
    3. Existing Token-vs-Collection defense already checks `markCounterbidPending()` before queuing
  - **Functions Updated**:
    - `processOpenseaTokenBid`: Added pending check at start, mark pending before bid, clear in finally block
    - `processMagicedenTokenBid`: Added pending check at start, mark pending before bid, clear in finally block
  - Files modified: `src/index.ts`
  - Resolves: Duplicate token bids within seconds, wasted gas fees from redundant bids
  - Impact: Only ONE bid source can win the "race" for each token, preventing duplicate bids

### Added
- **Comprehensive Max Bid Check Debug Logging**: Added debug logs showing max bid price check decisions for all counterbid handlers
  - **Purpose**: Provide complete visibility into whether counterbid prices are within max bid bounds
  - **Log Format**: `[Counterbid] [OS/ME] slug [identifier] | Max Check: counter=X max=Y | OK - PROCEED` or `EXCEEDS - SKIP`
  - **Locations Updated**:
    - OpenSea token counterbid: Shows calculated counter price vs max before queueing
    - OpenSea trait counterbid: Shows calculated counter price vs max before queueing
    - OpenSea collection offer counterbid: Shows base offer price vs max for collection offers
    - MagicEden token counterbid: Shows outbid calculation + max check
    - MagicEden trait counterbid: Shows outbid calculation + max check
  - **Additional Logging**: Added outbid calculation logs for MagicEden handlers showing incoming price, margin, and counter price
  - Files modified: `src/index.ts`
  - Resolves: Missing visibility into max bid price enforcement decisions
  - Impact: Complete debug logging trail for counterbid max bid checks

### Changed
- **Standardized Max Bid Check Operators**: Changed OpenSea token counterbid from `>=` to `>` for consistent max bid enforcement
  - **Previous**: `if (offerPriceEth >= maxBidPriceEth)` - Would skip bid AT exactly max price
  - **Now**: `if (offerPriceEth > maxBidPriceEth)` - Allows bid AT exactly max price (consistent with other handlers)
  - Files modified: `src/index.ts` (line ~12700)
  - Resolves: Inconsistent max bid enforcement across handlers
  - Impact: Bids can now be placed at exactly the max bid price value

### Fixed
- **Two-Tier Top Offer Caching (Phase 3)**: Implemented complete two-tier approach to ensure bids are placed ABOVE marketplace's actual top offer
  - **Problem Persisted**: Despite Phase 2 fix, cache was still empty when first token bids were placed because it was only populated from WebSocket incoming bids, not from API responses
  - **Evidence**: CLOAKS #12920 had TOP OFFER 0.0171 WETH but bot bid 0.0143 WETH. Cache was always MISS on first bid because scheduled processors weren't caching API results.
  - **Solution**: Two-tier approach as specified by user:
    1. **Scheduled Bid Processors**: ALWAYS fetch from API and cache the result after each highest offer check
    2. **Counterbid Handlers**: Check cache FIRST, if cache MISS → fetch from API as fallback
  - **Scheduled Bid Processors Updated (8 functions)** - Now cache API results after `outbid=true` fetch:
    - `processOpenseaTokenBid`: Caches `fetchItemOffers()` result after isOwnBid calculation
    - `processOpenseaTraitBid`: Caches `fetchOpenseaOffers('TRAIT')` result after isOwnBid calculation
    - `processOpenseaCollectionBid`: Caches `fetchOpenseaOffers('COLLECTION')` result after isOwnBid calculation
    - `processMagicedenTokenBid`: Caches `fetchMagicEdenOffer('TOKEN')` result after isOwnBid calculation
    - `processMagicedenTraitBid`: Caches `fetchMagicEdenOffer('TRAIT')` result after isOwnBid calculation
    - `processMagicedenCollectionBid`: Caches `fetchMagicEdenOffer('COLLECTION')` result after isOwnBid calculation
    - `processBlurCollectionBid`: Caches `fetchBlurBid('COLLECTION')` result after isTopBid calculation
    - `processBlurTraitBid`: Caches `fetchBlurBid('TRAIT')` result after isTopBid calculation
  - **Counterbid Handlers Updated** - Now fetch from API on cache miss:
    - `handleOpenseaCounterbid` Token-vs-Collection: Added API fallback with `fetchItemOffers()` when cache miss
    - `handleOpenseaCounterbid` Trait-vs-Collection: Added API fallback with `fetchOpenseaOffers('TRAIT')` when cache miss
    - `handleMagicEdenCounterbid` Trait/Token-vs-Collection: Added API fallback with `fetchMagicEdenOffer()` when cache miss
  - **Debug Logging Added**:
    - `[BidFlow][XX-Type] slug [identifier] | API→Cache: topOffer=X ETH | isOurs=Y` - When scheduled processor caches API result
    - `[Counterbid][XX] slug [identifier] | Token-vs-Collection: Cache MISS - fetching from API` - When counterbid handler falls back to API
    - `[Counterbid][XX] slug [identifier] | Token-vs-Collection: API→Cache: topOffer=X ETH | isOurs=Y` - When counterbid handler caches API result
    - `[Counterbid][XX] slug [identifier] | Token-vs-Collection: Using top offer X ETH (> collection offer Y ETH)` - When using cached/fetched offer
  - Files modified: `src/index.ts`
  - Resolves: First token bids not becoming top offers due to empty cache
  - Impact: Cache is now always warm from scheduled bids, and counterbid handlers get accurate data even on cache miss via API fallback

- **Cross-Type Counterbid Bid Price Calculation Fix (Phase 2)**: Fixed critical bug where cross-type counterbids (Token-vs-Collection, Token-vs-Trait, Trait-vs-Collection) calculated bid prices based on incoming offer instead of actual marketplace top offer
  - **Problem Persisted**: Despite implementing `marketplaceTopOfferCache`, bids were still NOT becoming top offers because the cache was checked AFTER the bid price was already calculated
  - **Evidence**: CLOAKS #17855 had TOP OFFER 0.0178 WETH (845ed3), but bot bid 0.0171 WETH (4th place). Logs showed: `Cache Check: SKIP | reason=competitor_still_top | cached=0.017` - cache correctly detected issue but bid was already queued with wrong price
  - **Root Cause**: Cross-type counterbid logic calculated `tokenOfferPrice = roundedEth + margin` where `roundedEth` was the incoming lower-tier offer (collection/trait), NOT the actual top token offer on the marketplace
  - **Solution**: Check `marketplaceTopOfferCache` BEFORE calculating bid price, use `max(incomingOffer, cachedTopOffer) + margin` for bid calculation
  - **Locations Fixed (6 total)**:
    1. **Token-vs-Collection** (OpenSea, line ~13289): Now uses `max(collectionOffer, cachedTopTokenOffer) + margin`
    2. **Token-vs-Trait** (OpenSea, line ~12969): Now uses `max(traitOffer, cachedTopTokenOffer) + margin`
    3. **Existing Token-vs-Trait** (OpenSea, line ~12928): Now uses `max(traitOffer, cachedTopTokenOffer) + margin`
    4. **Trait-vs-Collection** (OpenSea, line ~13192): Now uses `max(collectionOffer, cachedTopTraitOffer) + margin`
    5. **Trait/Token-vs-Collection** (MagicEden, line ~12194): Now uses `max(collectionOffer, cachedTopOffer) + margin`
    6. **Token-vs-Trait** (MagicEden, line ~11996): Now uses `max(traitOffer, cachedTopTokenOffer) + margin`
  - **Expected Behavior**: When collection offer arrives at 0.0142 WETH but existing competitor token offer is at 0.017 WETH, bot now bids `0.017 + margin` instead of `0.0142 + margin`
  - Files modified: `src/index.ts`
  - Resolves: Cross-type counterbids placing lower bids than marketplace top offer, wasted bids that can never win
  - Impact: All cross-type counterbids now correctly beat existing marketplace top offers

### Added
- **Marketplace Top Offer Cache (Token-vs-Collection Fix)**: Implemented cache-first approach to track marketplace's actual top offer, fixing bug where first token bids were NOT becoming top offers
  - **Problem Solved**: When a collection offer came in via WebSocket, the bot's "Token-vs-Collection" defense placed token bids based on the collection offer price (e.g., 0.0142 WETH), ignoring existing higher competitor token offers (e.g., 0.017+ WETH). Result: Bot's bids were NOT the top offer.
  - **Root Cause**: `handleOpenseaCounterbid` Token-vs-Collection logic at line ~12984 used `roundedEth` (the collection offer amount) instead of checking the existing top TOKEN offer on that specific token.
  - **Solution**: Implemented `marketplaceTopOfferCache` - a separate cache from `orderCache` that tracks WHO is winning each bid slot (ours vs competitor), not just our orders.
  - **Cache Architecture**:
    - `orderCache` (existing): Tracks OUR placed orders with offer prices
    - `marketplaceTopOfferCache` (new): Tracks the marketplace's actual top offer (including competitor bids)
    - Key format: `{taskId}:{marketplace}:{identifier}` (e.g., "abc123:opensea:17855")
  - **New Infrastructure** (`src/index.ts`):
    - `MarketplaceTopOffer` interface with: topOfferAmount, topOfferWei, topBidderAddress, isOurBid, timestamp
    - `marketplaceTopOfferCache` BoundedCache with 10k capacity and 5-minute TTL
    - `topOfferTaskIndex` Map for efficient task-level cleanup
    - Helper functions: `getTopOfferCacheKey()`, `getMarketplaceTopOffer()`, `setMarketplaceTopOffer()`, `updateTopOfferAfterOurBid()`, `updateTopOfferFromIncomingBid()`, `clearTopOffersByTask()`, `checkShouldCounterbid()`
  - **Cache-First Counterbid Logic**:
    - Cache HIT: Compare incoming bid vs cached top offer to decide if counterbid needed
    - Cache MISS: Counterbid proceeds (conservative approach), cache updated with incoming bid
    - Own bid detection: Skip counterbid if incoming bidder is our wallet
    - Already top bidder: Skip counterbid if we're already winning with higher price
  - **Scheduled Bid Processors Updated** (8 functions):
    - After successful bid placement, call `updateTopOfferAfterOurBid()` to mark us as top bidder
    - OpenSea: `processOpenseaTokenBid`, `processOpenseaTraitBid`, `processOpenseaCollectionBid`
    - MagicEden: `processMagicedenTokenBid`, `processMagicedenTraitBid`, `processMagicedenCollectionBid`
    - Blur: `processBlurCollectionBid`, `processBlurTraitBid`
  - **Counterbid Handlers Updated** (3 handlers):
    - Replaced `orderCache.getOfferPrice()` checks with `checkShouldCounterbid()` cache-first approach
    - `handleOpenseaCounterbid`: Token, trait, and collection counterbid sections
    - `handleMagicEdenCounterbid`: Token, trait, and collection counterbid sections
    - `handleBlurCounterbid`: Trait and collection counterbid sections
  - **Cache Cleanup**: `clearTopOffersByTask()` called in `stopTask()` to clean up when tasks stop
  - Files modified: `src/index.ts`
  - Resolves: First token bids not becoming top offers, wasted bids that can't win
  - Impact: Counterbids now correctly consider existing competitor offers, not just collection offer prices

### Fixed
- **Floor Price API Retry with Exponential Backoff**: Fixed critical bug where 0 floor prices were cached, causing percentage-based bids to calculate as 0
  - **Problem Solved**: When OpenSea/Blur/MagicEden API returned empty listings (no floor price), the code returned 0 and cached it for 60 seconds. During this time, all percentage-based bid calculations resulted in 0 prices (e.g., `floor=0 | min=0 | max=0 | offer=0`).
  - **Root Cause**: Debug logs showed `[PriceCalc] nakamigos-cloaks | floor=0 | min=0 | max=0 | offer=0 | WETH` - floor price was 0 which caused all percentage-based calculations to fail.
  - **Solution**: Added retry with exponential backoff for all floor price API functions:
    - `getOpenseaFloor()`: 3 retries with 500ms/1s/2s backoff for API errors and 0 floor prices
    - `getBlurFloor()`: 3 retries with 500ms/1s/2s backoff for API errors and 0 floor prices
    - `getMagicEdenFloor()`: 3 retries with 500ms/1s/2s backoff for API errors and 0 floor prices
  - **Key Changes**:
    - Retries when floor price is 0 (likely temporary API issue)
    - Retries on API errors (except 404)
    - Only caches POSITIVE floor prices - 0 values are NOT cached
    - 404 errors still cached for 5 minutes (collection doesn't exist)
  - Files modified: `src/functions/collection.ts`
  - Resolves: Scheduled bids placing lower bids than competitors due to 0 floor price calculations
  - Impact: Floor price fetching now resilient to temporary API failures, prevents incorrect bid pricing

### Added
- **WebSocket Connection Reliability Improvements**: Comprehensive improvements to prevent silent WebSocket connection death and missed counterbids
  - **Problem Solved**: WebSocket connection died silently at 20:10:19 UTC without triggering close event, causing all competitor bids during ~1 hour window to be missed. No reconnection attempt was made because the ping/pong mechanism didn't detect the half-open connection.
  - **Application-Level Watchdog** (`websocket-manager.ts`):
    - New `startBidActivityWatchdog()` monitors actual bid events, not just ping/pong
    - If active bidding subscriptions exist but no bid events received for 10+ minutes, forces reconnection
    - Tracks `lastBidEventReceived` timestamp, updated via `markBidEventReceived()` method
    - Runs every 5 minutes independently of ping/pong mechanism
  - **Enhanced Disconnection Logging** (`websocket-manager.ts`):
    - `handleClose()` now logs comprehensive disconnect info using `errorLog()` (ALWAYS logged, not just DEBUG)
    - Includes: code, reason, connectionDuration, lastMessageAge, lastPongAge, lastBidEventAge, lastServerStatusAge, missedPongs, activeSubscriptions, persistentListings, messagesReceived
    - Enables post-mortem analysis when connections die
  - **Server Status Health Check** (`websocket-manager.ts`):
    - Tracks `lastServerStatusReceived` timestamp when server_status messages arrive
    - `checkServerStatusHealth()` verifies server sends status every 30s (allows 90s grace)
    - If no server status received in 90+ seconds, sends ping to verify connection
    - Called every minute in health monitoring loop
  - **Health Logging to Debug File** (`websocket-manager.ts`):
    - Health status logged every minute to `websocket-YYYY-MM-DD.log` when DEBUG=true
    - Includes: uptime, lastMessageAge, lastPongAge, lastBidEventAge, lastServerStatusAge, missedPongs, activeSubscriptions, messageRate
  - Files modified: `src/utils/websocket-manager.ts`
  - Resolves: Silent WebSocket connection death causing missed counterbids for hours
  - Impact: Bot will detect dead connections within 10 minutes and automatically reconnect

- **Smart Subscription Cleanup on Task Stop**: WebSocket unsubscription now uses reference counting to prevent premature unsubscription when multiple tasks share a collection
  - **Problem Solved**: When `stopTask()` was called, it unconditionally unsubscribed from the collection, even if other tasks were still bidding on that same collection
  - **Solution**: Reference counting tracks how many active tasks are subscribed to each collection
  - **Implementation** (`src/index.ts`):
    - Added `collectionSubscriptionRefCount: Map<string, Set<string>>` (slug -> Set<taskId>)
    - `addTaskToCollectionSubscription(slug, taskId)`: Adds task to ref count, returns true if first subscriber
    - `removeTaskFromCollectionSubscription(slug, taskId)`: Removes task from ref count, returns true if should unsubscribe
    - `getCollectionSubscriptionCount(slug)`: Returns number of tasks subscribed to collection
  - **`subscribeToCollections()` updated**:
    - Only sends `join_the_party` WebSocket message if this is the first task for the collection
    - Additional tasks for same collection just increment ref count without sending message
  - **`unsubscribeFromCollection()` updated**:
    - Checks ref count before unsubscribing
    - Only sends `leave_the_party` WebSocket message when last task for collection is stopped
    - Returns `{ success: true, unsubscribed: false }` when other tasks still need the subscription
  - Files modified: `src/index.ts`
  - Resolves: Premature unsubscription causing other tasks to miss bid events
  - Impact: Multiple tasks can safely share collection subscriptions; stopping one task doesn't affect others

### Fixed
- **Multiple Bids on Same Token - Previous Bid Not Cancelled**: Fixed race condition causing duplicate bids on the same token
  - **Symptoms**: 3+ "Item Offer" bids visible on same token at different prices (e.g., CLOAKS #9353 showing 0.0173, 0.0173, 0.0171 WETH from same wallet)
  - **Root Cause 1**: `orderCache.get()` returned expired orders without filtering, causing code to think an old bid still existed
  - **Root Cause 2**: Scheduled bid processors used fire-and-forget `queue.addBulk()` or `queue.add()` for cancellation, placing new bids before old ones were cancelled
  - **Root Cause 3**: Counterbid processor `magicedenCollectionCounterBid` also used fire-and-forget `queue.addBulk()` for cancellation
  - **Fix 1**: Added expiry check to `orderCache.get()` - now returns `undefined` for expired orders and automatically cleans them from cache
  - **Fix 2**: Changed scheduled bid processors to use `await cancelOpenseaOrdersDirectly()` and `await cancelMagicEdenOrdersDirectly()` instead of fire-and-forget queue jobs
  - **Fix 3**: Changed `magicedenCollectionCounterBid` to use `await cancelMagicEdenOrdersDirectly()` instead of fire-and-forget `queue.addBulk()`
  - **Functions Fixed (Scheduled Bid Processors)**:
    - `processOpenseaTokenBid` - now awaits cancellation before placing new bid
    - `processMagicedenTokenBid` - now awaits cancellation before placing new bid
    - `processMagicedenTraitBid` - now awaits cancellation before placing new bid
    - `processMagicedenCollectionBid` - now awaits cancellation before placing new bid
  - **Functions Fixed (Counterbid Job Processors)**:
    - `magicedenCollectionCounterBid` - now awaits cancellation before placing new counterbid
  - **Counterbid Processors Audited (Already Correct)**:
    - `openseaCollectionCounterBid` - uses `await cancelOpenseaOrdersDirectly()` ✓
    - `openseaTraitCounterBid` - uses `await cancelOpenseaOrdersDirectly()` ✓
    - `openseaTokenCounterBid` - uses `await cancelOpenseaOrdersDirectly()` ✓
    - `magicedenTraitCounterBid` - uses `await cancelMagicEdenOrdersDirectly()` ✓
    - `magicedenTokenCounterBid` - uses `await cancelMagicEdenOrdersDirectly()` ✓
    - `blurCollectionCounterBid` - uses `await cancelBlurOrdersDirectly()` ✓
    - `blurTraitCounterBid` - uses `await cancelBlurOrdersDirectly()` ✓
  - Files modified: `src/utils/order-cache.ts`, `src/index.ts`
  - Resolves: Multiple active bids on same token, "Order already exists" errors, wasted gas fees
  - Impact: Each token now has at most 1 active bid, proper cancellation before new bid placement

### Added
- **Token Counterbid for Collection/Trait Offers (OpenSea)**: Token bid tasks now defend against incoming collection and trait offers
  - **Problem Solved**: When a user had a token offer (bidType = "token") on a specific NFT, and a competitor placed a collection or trait offer that covered that token, the system would NOT counterbid, causing the user to lose the token to a lower-tier offer
  - **Solution**: Added logic to `handleOpenseaCounterbid` to detect and counterbid against collection/trait offers for token-type tasks
  - **Trait Offer Defense**:
    - For tasks with `bidType === "token"` or `bidType === "rarity"`, when a trait offer comes in, the system now checks if any tokens in `task.tokenIds` have that trait using `tokenHasTrait()`
    - If a matching token is found and the incoming trait offer beats the current token offer, a token counterbid is queued
    - Uses `crossTypeMarginMultiplier` for price calculation
  - **Collection Offer Defense**:
    - For tasks with `bidType === "token"` or `bidType === "rarity"`, when a collection offer comes in, the system now proactively checks ALL tokens in `task.tokenIds`
    - If the incoming collection offer beats any token's current offer, a token counterbid is queued for that token
    - Skips tokens that already have orders in cache (already processed by existing logic)
  - **Safeguards**:
    - Uses `pendingCounterbids` cache to prevent duplicate counterbids on the same token
    - Respects `maxBidPrice` constraint - skips counterbid if calculated price exceeds max
    - Comprehensive debug logging with `Token-vs-Trait` and `Token-vs-Collection` prefixes
  - **Files Modified**:
    - `src/functions/index.ts` - Added export for `tokenMetadata` module
    - `src/index.ts` - Added import for `tokenHasTrait`, modified `handleOpenseaCounterbid` trait_offer and collection_offer handlers
  - Resolves: Token offers losing to lower-tier collection/trait offers on OpenSea
  - Impact: Better token position defense - token bids now automatically counterbid against any offer type that threatens their position

### Fixed
- **Missing Error Details in Catch Block Logging**: Fixed catch blocks that captured errors but didn't include them in log output
  - **Root Cause**: `warnLog()` was being used in catch blocks but doesn't accept an `error` parameter like `errorLog()` does
  - **Solution**: Changed `warnLog()` to `errorLog()` in catch blocks and added the captured `error` variable as the third parameter
  - **Files Fixed**:
    - `src/utils/balance.ts` - 8 catch blocks now include error details (cache read/write errors, Redis errors, balance fetch errors)
    - `src/functions/priceMonitor.ts` - 1 catch block fixed (was using commented console.log with no actual logging)
    - `src/marketplace/magiceden/bid.ts` - 9 catch blocks now include error details (trait floor cache, stats cache, tokens cache)
    - `src/marketplace/opensea/list.ts` - 1 catch block fixed (collection details fetch error)
    - `src/utils/lock.ts` - 1 catch block fixed (lock release error)
    - `src/marketplace/blur/bid.ts` - 1 catch block fixed (abort request controller error)
  - **Total**: 21 catch blocks fixed across 6 files
  - **Before**: `errorLog('Balance', 'Failed to invalidate cache...')` - error variable ignored
  - **After**: `errorLog('Balance', 'Failed to invalidate cache...', error)` - full stack trace captured
  - Resolves: Error details (message + stack trace) being lost in catch block logging
  - Impact: All catch block errors now include full error information for debugging

### Added
- **Log Filtering and Export System**: Comprehensive log filtering and export system for debug logs with metadata support
  - **New Log Metadata Support**:
    - Added `LogMetadata` interface with optional fields: `taskId`, `marketplace`, `slug`, `tokenId`, `traitKey`, `jobId`, `orderHash`, `wallet`, `identifier`
    - All logging functions (`debugLog`, `errorLog`, `warnLog`, `infoLog`) now accept optional metadata parameter
    - Metadata is JSON-serialized and appended to log lines for machine-parseable filtering
    - Backward-compatible: existing log calls without metadata continue to work
  - **Log Parsing Infrastructure**:
    - Added `parseLogLine()` function to extract timestamp, level, prefix, category, message, and metadata from log lines
    - Added `ParsedLogLine` interface for structured log data
    - Added `getMarketplaceCategories()` to map marketplace names to their log categories (e.g., 'blur' → ['blur', 'counterbid', 'bidflow'])
    - Added `getAllCategories()` helper function for retrieving all 22 log categories
  - **New Export Handler** (`src/utils/log-export-handler.ts`):
    - `exportLogs()` function with streaming file parser using `AsyncGenerator`
    - Supports filters: `taskId`, `marketplace`, `category`, `level`, `timeRange`, `startTime`, `endTime`, `limit`, `offset`
    - Time range presets: 5m, 10m, 1h, 24h, custom
    - Marketplace filtering searches across related categories (e.g., 'opensea' searches opensea, counterbid, bidflow logs)
    - `logsToCSV()` and `logsToJSON()` export formatters
    - Efficient streaming: handles large log files without loading entire file into memory
  - **New API Endpoint** (`GET /api/debug-logs/export`):
    - Query parameters: `taskId`, `marketplace`, `category`, `level`, `timeRange`, `startTime`, `endTime`, `format`, `limit`, `offset`
    - Marketplace options: 'blur', 'opensea', 'magiceden', 'system', 'health', 'all'
    - Format options: 'json' (default), 'csv'
    - Default limit: 10,000 logs, max: 50,000
    - Legacy `/api/logs/export` returns 301 redirect message pointing to new endpoint
  - Files created: `src/utils/log-export-handler.ts`
  - Files modified: `src/utils/debug-file-logger.ts`, `src/index.ts`
  - Resolves: Need to filter and export logs by marketplace, system, health, and taskId
  - Impact: Enables frontend log download with advanced filtering capabilities

### Changed
- **Console Logging Migration**: Migrated remaining console.log calls to centralized debug-file-logger
  - **Files Updated**:
    - `src/utils/balance.ts` - 10 console.log calls migrated to debugLog('Balance', ...)
    - `src/utils/order-cache.ts` - 8 console.log calls migrated to debugLog('Cache', ...) with metadata
    - `src/utils/task-state-manager.ts` - 1 console.log call migrated to debugLog('TaskState', ...)
  - All DEBUG-gated logs now use consistent prefix and metadata format
  - Enables filtering by marketplace, taskId, and other metadata fields
  - Files modified: `src/utils/balance.ts`, `src/utils/order-cache.ts`, `src/utils/task-state-manager.ts`
  - Resolves: Inconsistent debug logging format and missing metadata
  - Impact: All balance, order-cache, and task-state logs now visible in exported log files with filtering support

### Removed
- **Bid Trace Logger System**: Completely removed the `bidTraceLogger` system in favor of existing `debugLog` logging
  - **Files Deleted**:
    - `src/utils/bid-trace-logger.ts` - Bid trace logger class (~400 lines)
    - `src/types/bid-trace.types.ts` - Type definitions for bid tracing
  - **~226 bidTraceLogger calls removed** throughout job processors and counterbid handlers
  - **API Endpoints Removed**:
    - `GET /api/bid-traces` - Query bid traces (now returns HTTP 501)
    - `GET /api/bid-traces/export` - Export traces to JSON/CSV (now returns HTTP 501)
    - `GET /api/bid-traces/duplicates` - Find duplicate bids (now returns HTTP 501)
  - **Rationale**: The existing `debugLog()` system already provides comprehensive logging to category-specific files (`bidflow-YYYY-MM-DD.log`, `counterbid-YYYY-MM-DD.log`). The bidTraceLogger added redundant complexity with its own file format and in-memory trace contexts.
  - Files modified: `src/index.ts` (removed import, trace variable declarations, all logger calls)
  - Resolves: Redundant logging systems, unnecessary complexity
  - Impact: All debug logging now consolidated through `debugLog()` function

### Fixed
- **Counterbid Floating Point Precision**: Fixed counterbid price calculations showing ugly floating point values like `0.017499999999999998` instead of clean values like `0.0175`
  - **Root Cause**: `roundToValidPrecision()` was only applied to the incoming bid amount, not to the final counter bid price after margin addition
  - **Solution**: Applied `roundToValidPrecision()` to the final `offerPriceEth` in all 4 counterbid handlers
  - **Locations Fixed**:
    - OpenSea token counterbid (line ~12523)
    - OpenSea trait counterbid (line ~12748)
    - MagicEden token counterbid (line ~11699)
    - MagicEden trait counterbid (line ~11854)
  - Files modified: `src/index.ts`
  - Resolves: Floating point noise in counterbid logs and API calls
  - Impact: Clean, human-readable counterbid prices matching OpenSea precision requirements

- **Blur WebSocket Counterbid Log Shows slug=unknown**: Fixed debug log showing `slug=unknown` for Blur counterbid events
  - **Root Cause**: The debug log at line 11292 tried to extract slug from message structure before `getMarketplaceDetails()` was called. Blur messages don't include slug directly - it needs to be looked up from `contractToTasksMap`.
  - **Solution**: Moved debug log after `getMarketplaceDetails()` call so slug lookup is already complete
  - Files modified: `src/index.ts:11287-11295`
  - Resolves: `[WebSocket] [CounterBid] Received bid event | marketplace=BLUR | slug=unknown` messages
  - Impact: Blur counterbid logs now show proper collection slugs for easier debugging

### Changed
- **Lock Acquisition Error Downgraded to Warning**: Changed lock contention messages from ERROR to WARN level
  - **Analysis**: Lock acquisition failures when task is already being processed are expected behavior, not errors
  - **Previous**: `errorLog('BulkJobs', 'Failed to acquire scheduled bid lock', lockError)`
  - **Now**: `warnLog('BulkJobs', 'Lock contention | Task ... is already being processed - will retry on next loop')`
  - Files modified: `src/index.ts:6710-6711`
  - Resolves: False positive ERROR logs flooding the queue log file
  - Impact: Cleaner error logs, lock contention shown as expected WARN instead of ERROR

- **Schedule Jobs Configured for Infinite Retry**: Updated all *_SCHEDULE jobs (OPENSEA_SCHEDULE, BLUR_SCHEDULE, MAGICEDEN_SCHEDULE) to retry infinitely
  - **Previous**: `attempts: 100`, `removeOnFail: true`, `delay: 2000`
  - **Now**: `attempts: Number.MAX_SAFE_INTEGER`, `removeOnFail: false`, `delay: 1000`
  - **Changes**:
    - Infinite retries: Schedule jobs now never give up, will retry until successful
    - Failed jobs kept: `removeOnFail: false` keeps failed jobs for debugging
    - Faster retry: Backoff delay reduced from 2s to 1s for quicker recovery
  - Files modified: `src/index.ts:7850-7898`
  - Resolves: Schedule jobs giving up after 100 failures
  - Impact: More resilient scheduled bid processing that recovers automatically from transient failures

- **Invalid Task Status Transitions in markJobAsProcessed**: Fixed race condition causing hundreds of `[ERROR][TaskStatusTransition] Invalid state transition` errors
  - **Root Cause**: `markJobAsProcessed()` unconditionally attempted to set status to RUNNING, even when the marketplace was already in COMPLETED or SCHEDULED state (late jobs from previous loops)
  - **Solution**: Added state validation before RUNNING transition - only transitions from STARTED, QUEUED, or QUEUEING states
  - **Race Timeline Fixed**:
    1. All jobs complete → status becomes COMPLETED
    2. 500ms delay → status becomes SCHEDULED
    3. Late job from previous loop completes
    4. Now correctly skips RUNNING transition instead of causing error
  - Files modified: `src/index.ts:4061-4069`
  - Resolves: SCHEDULED→RUNNING and COMPLETED→RUNNING invalid state transition errors
  - Impact: Eliminates hundreds of error log entries per task

- **Parent Job Disappearance During Lock Acquisition**: Fixed error "Parent job disappeared during lock acquisition! Task {taskId}"
  - **Root Cause**: Scheduled job IDs (e.g., `task-opensea-schedule`) don't contain loop numbers, so the regex extraction at line 4023 failed. When `getParentJobId()` was called, it used the current loop counter which may have already been incremented, causing a mismatch.
  - **Solution**: Added fallback logic to search for parent jobs within the last 2 loops when the job ID doesn't contain a loop number
  - **Search Algorithm**:
    1. Try current loop (loopOffset = 0)
    2. Try previous loop (loopOffset = 1)
    3. Try loop before that (loopOffset = 2)
    4. Use first matching parent job found
  - Files modified: `src/index.ts:4038-4057`
  - Resolves: Parent job lookup failures for scheduled bid jobs
  - Impact: Eliminates "Parent job disappeared" errors for scheduled jobs

- **Blur Counterbid Slug Always Showing 'unknown'**: Fixed Blur counterbid logs displaying `slug=unknown` instead of the collection name
  - **Root Cause**: `handleBlurMessages()` only extracted `contractAddress` and returned it directly, while OpenSea/MagicEden handlers returned `{ contractAddress, slug }`
  - **Solution**: Updated `handleBlurMessages()` to look up the slug from `contractToTasksMap` using the contract address, since Blur WebSocket messages don't include the slug directly
  - **Implementation**:
    - Changed return type to `{ contractAddress?: string, slug?: string }`
    - Look up slug from first matching task in `contractToTasksMap`
    - Updated `getMarketplaceDetails()` to use new return format
  - Files modified: `src/index.ts:13544-13563, 11319-11321`
  - Resolves: Blur counterbid logs now show proper collection slugs
  - Impact: Improved debugging visibility for Blur counterbid operations

- **False Positive Redis Connection Leak Warnings**: Fixed spurious "Possible Redis connection leaks" warnings when only 6 connections exist
  - **Root Cause**: Warning triggered when any connections were older than 2 hours, but BullMQ legitimately maintains ~6-8 long-lived connections for queues, workers, scheduler, and events
  - **Solution**: Only warn when old connection count exceeds 8 (the expected number for BullMQ)
  - **Expected Connections**:
    - 2 for main queue (queue + worker)
    - 2 for blur queue (queue + worker)
    - 1 for scheduler
    - 1 for events
    - 2 buffer for Redis client and other operations
  - Files modified: `src/index.ts:2001-2014`
  - Resolves: False positive connection leak warnings during normal operation
  - Impact: Cleaner logs, warnings only appear for actual connection leaks

- **OrderCache Desync During Bulk Cancel Operations**: Fixed 7 critical bugs where MongoDB Order operations didn't update the in-memory OrderCache
  - **Root Cause**: Several code paths deleted orders from MongoDB but didn't update the in-memory cache, causing stale data issues in counterbid logic
  - **Bugs Fixed**:
    1. **Expired orders cleanup**: Added `orderCache.cleanupExpired()` after DB cleanup in `validateBidCount()`
    2. **Bulk cancel OpenSea (wallet not found)**: Added `orderCache.deleteById()` before DB deletion
    3. **Bulk cancel OpenSea (after API)**: Added `orderCache.deleteById()` before DB deletion
    4. **Bulk cancel MagicEden (wallet not found)**: Added cache deletion for all orderKeys
    5. **Bulk cancel MagicEden (after API)**: Added cache deletion for all orderKeys
    6. **Bulk cancel Blur (after API)**: Added `orderCache.deleteById()` before DB deletion
    7. **MagicEden batch cleanup**: Added cache sync after `Order.insertMany()` in unflushed queue cleanup
  - Files modified: `src/index.ts`, `src/marketplace/magiceden/bid.ts`
  - Resolves: Stale cache data causing incorrect counterbid decisions, duplicate bids, or missed counterbid opportunities
  - Impact: OrderCache now stays in sync with MongoDB for all Order operations

- **Counterbid Handler Bug Fixes and Improvements (16 Issues)**: Comprehensive audit and fixes for `handleMagicEdenCounterbid`, `handleOpenseaCounterbid`, and `handleBlurCounterbid`
  - **Critical Fixes**:
    1. **OrderCache Expiry Filtering**: Added `expiresAt > Date.now()` check after all `orderCache.get()` calls to prevent using expired cached orders (6 locations across all 3 handlers)
    2. **Blur Silent Error Catch**: Fixed catch block at lines 13501-13506 that extracted error details but never logged them - now uses `errorLog()`
    3. **OpenSea Unreachable Null Check**: Fixed `if (!orders)` check that could never execute (arrays are always truthy) - now checks `if (!cachedOrder)` before creating array
    4. **MagicEden pendingCounterbids Leak**: Added cleanup in outer catch block to clear pending marks on exception, preventing 60-second token blocking
    5. **TypeScript Error**: Added null guard for `contractAddress` possibly undefined at line 13555
  - **High Priority Fixes**:
    6. **Missing await on Cancellation Jobs**: Added `await` to 14 `queue.add()` and `queue.addBulk()` cancellation calls to ensure job completion before proceeding
    7. **Blur JSON.parse Safety**: Wrapped 5 `JSON.parse(order.orderHash)` calls in try-catch with `errorLog()` to prevent crashes on corrupted data
  - **Medium Priority Fixes**:
    8. **Below-Min-Price Logging**: Added `debugLog()` for 4 silent early returns when incoming bid is below minimum price (MagicEden trait/collection, OpenSea trait/collection)
    9. **Empty If-Block Fixes**: Added meaningful debug logs to 4 empty if-blocks that previously did nothing (MagicEden token counterbids, OpenSea normalized price, OpenSea token/collection counterbids)
    10. **Redundant Cache Lookup**: Replaced `orderCache.getOfferPrice()` call inside loop with `Number(order.offerPrice)` since order is already available
    11. **Typo Fix**: Renamed `colletionOffer` to `collectionOffer` (37 occurrences)
  - **Code Quality Fixes**:
    12. **Variable Collision**: Fixed duplicate `collectionOffer` declaration by renaming OpenSea handler variable to `collectionOfferEth`
  - Files modified: `src/index.ts` (handleMagicEdenCounterbid ~11431-12180, handleOpenseaCounterbid ~12182-13065, handleBlurCounterbid ~13068-13507)
  - Resolves: Expired order usage, silent errors, unreachable code, pending bid leaks, potential crashes, missing visibility into counterbid decisions
  - Impact: More reliable counterbid handling with better error visibility and proper resource cleanup

### Changed
- **Consolidated ourTopBids into OrderCache**: Eliminated redundant in-memory bid tracking store by using existing `orderCache` for counterbid price comparison
  - **Problem Solved**: Two parallel caching systems (`ourTopBids` and `orderCache`) stored the same order data redundantly, risking desync and adding maintenance complexity
  - **Solution**: Consolidated all bid price lookups into `orderCache.getOfferPrice()` method
  - **Implementation**:
    - Added `getOfferPrice(taskId, marketplace, identifier)` method to OrderCache class
    - Replaced 7 `getOurTopBid()` calls with `orderCache.getOfferPrice()` in counterbid handlers
    - Removed 14 redundant `updateOurTopBid()` calls (orderCache.set() already called in bid.ts files)
    - Removed lifecycle calls (`initializeTopBidsFromDatabase`, `clearTaskTopBids`)
    - Deleted entire `ourTopBids` code block (~150 lines)
  - **Benefits**:
    - Single source of truth for order data
    - Better expiration handling (orderCache checks `isExpired()`, ourTopBids didn't)
    - Reduced memory footprint
    - Simpler codebase (~140 lines removed)
  - **Counterbid Handlers Updated**:
    - `handleMagicEdenCounterbid`: token, trait, bulk handlers
    - `handleOpenseaCounterbid`: trait, bulk handlers
    - `handleBlurCounterbid`: trait, bulk handlers (with wei→ETH conversion for Blur)
  - Files modified: `src/index.ts`, `src/utils/order-cache.ts`
  - Resolves: Potential desync between two caching systems storing same data
  - Impact: Cleaner architecture with single source of truth for order lookups

### Added
- **Floor Price Pre-Fetching Optimization**: Added automatic floor price cache warming when tasks start
  - **Problem Solved**: Counterbid handlers were blocking on floor price API calls when cache was cold, adding latency to time-critical counterbid decisions
  - **Solution**: Pre-fetch floor prices for all selected marketplaces during task initialization
  - **Implementation**:
    - Fires parallel floor price fetches for OpenSea, MagicEden, and Blur based on selected marketplaces
    - Uses fire-and-forget pattern (non-blocking) - task start doesn't wait for fetches to complete
    - Populates `collectionCache` L1/L2 cache so counterbid handlers get instant cache hits
    - Logs pre-fetch results with `[TaskStart]` prefix to `task-YYYY-MM-DD.log`
  - **Cache Keys**:
    - OpenSea: `task.contract.slug`
    - MagicEden: `task.contract.contractAddress`
    - Blur: `task.contract.slug`
  - Files modified: `src/index.ts` (startTaskInternal function ~line 7612-7646)
  - Resolves: Cold cache latency during first counterbid after task start
  - Impact: Counterbid handlers immediately get cache hits instead of blocking API calls

### Changed
- **Counterbid Handler Logging Standardization**: Converted all ~45 `console.log` statements in counterbid handlers to use centralized `debugLog()` function
  - **Handlers Updated**:
    - `handleMagicEdenCounterbid`: ~20 console.log calls converted
    - `handleOpenseaCounterbid`: ~20 console.log calls converted
    - `handleBlurCounterbid`: ~5 console.log calls converted
  - **Log Format**: `[Counterbid] [ME|OS|Blur] {slug} [{identifier}] | Action: details`
  - **Categories Logged**:
    - WS Event receipt with bid type, price, maker
    - Deduplication checks (orderHash, pending counterbids)
    - Own bid detection
    - Counterbid disabled checks
    - Price validation (min/max bounds)
    - Job queue confirmations
  - **Routing**: All logs with `Counterbid` prefix route to `counterbid-YYYY-MM-DD.log`
  - Files modified: `src/index.ts` (handleMagicEdenCounterbid ~line 11540-12300, handleOpenseaCounterbid ~line 12302-13170, handleBlurCounterbid ~line 13171-13622)
  - Resolves: Inconsistent logging format, missing file persistence for counterbid debug logs
  - Impact: Complete counterbid flow visibility in dedicated log file

- **Debug File Logger Prefix Updates**: Added new prefix mapping for task initialization logs
  - Added `TaskStart` prefix → `task` category (routes to `task-YYYY-MM-DD.log`)
  - Updated Task category count from 6 to 7 prefixes
  - Files modified: `src/utils/debug-file-logger.ts`

### Fixed
- **Task Loop Stuck at 100% Due to BullMQ "Missing key for job" Error**: Fixed critical bug where scheduled bid loops would get stuck indefinitely when BullMQ's internal Redis key was corrupted or deleted
  - **Root Cause**: When `job.updateProgress()` fails with "Missing key for job" error, the job fails but the task completion handler never runs, preventing `scheduleNextLoop()` from being called. The task gets stuck at 100% progress with no timer registered for the next loop.
  - **Solution**: Extended the existing timeout recovery logic to also handle "Missing key for job" errors in `queueEvents.on('failed')` handler
  - **Recovery Behavior**: When infrastructure error is detected on a `-schedule` job:
    1. Acquires distributed lock to prevent multiple recoveries
    2. Clears stuck parent jobs for all marketplaces
    3. Clears preprocessing data
    4. Releases any scheduled bid locks
    5. Resets task status to SCHEDULED
    6. Restarts the task
    7. Logs recovery to `logs/task-restarts.log` and debug log
  - **Errors Now Handled**:
    - `timed out after` - Job exceeded 15-minute timeout (existing)
    - `Missing key for job` - BullMQ Redis key corrupted/deleted during processing (NEW)
  - Files modified: `src/index.ts` (queueEvents.on('failed') handler ~line 4722)
  - Resolves: Tasks stuck at RUNNING 100% for hours after BullMQ infrastructure errors
  - Impact: Tasks now automatically recover and continue bidding after BullMQ errors

### Changed
- **Granular Log File Separation (22 Files)**: Refactored `debug-file-logger.ts` to automatically route logs to separate files based on prefix categories
  - **Zero Code Changes Required**: All existing `debugLog()`, `errorLog()`, `warnLog()` calls work unchanged
  - **New `infoLog()` Function**: Added info-level logging that always logs (like errorLog/warnLog) for important status messages
  - **WebSocket Logs Fixed**: Converted 6 raw `console.log('[WebSocket]...')` calls in index.ts to use `debugLog()`/`infoLog()` - now properly route to `websocket-YYYY-MM-DD.log`
  - **22 Category-Specific Log Files**:
    - `startup-YYYY-MM-DD.log` - Server initialization (STARTUP, ServerStartup, ServerRestart, BullMQInit)
    - `task-YYYY-MM-DD.log` - Task lifecycle (Task, TaskStatus, TaskState, TaskRecovery)
    - `scheduler-YYYY-MM-DD.log` - Scheduling operations (TaskScheduler, Scheduler, TimedLockManager)
    - `bidflow-YYYY-MM-DD.log` - Bid processing (BidFlow, ScheduledBid, Preprocessing, PriceCalc, StopOption)
    - `counterbid-YYYY-MM-DD.log` - Counterbid handling (Counterbid)
    - `queue-YYYY-MM-DD.log` - Job queue operations (Queue, BlurQueue, QueueEvent, JobProcessing, BulkJobs)
    - `worker-YYYY-MM-DD.log` - Worker health (Worker, BlurWorker, WorkerHealth, WorkerFailure)
    - `database-YYYY-MM-DD.log` - MongoDB operations (Database, MongoDBConnection, ConnectionPool, DB Retry)
    - `redis-YYYY-MM-DD.log` - Redis operations (Redis, RedisOperation, RedisRecovery)
    - `lock-YYYY-MM-DD.log` - Distributed locks (Lock, LockRelease)
    - `blur-YYYY-MM-DD.log` - Blur marketplace (Blur)
    - `opensea-YYYY-MM-DD.log` - OpenSea marketplace (OpenSea)
    - `magiceden-YYYY-MM-DD.log` - MagicEden marketplace (MagicEden)
    - `api-YYYY-MM-DD.log` - External API calls (API, Blockchain)
    - `websocket-YYYY-MM-DD.log` - WebSocket events (WebSocket)
    - `listing-YYYY-MM-DD.log` - Listing operations (Listing, ListingScheduler, ListingPriceCache)
    - `collection-YYYY-MM-DD.log` - Collection data (Collection, TRAIT FLOOR, CollectionStats)
    - `balance-YYYY-MM-DD.log` - Balance operations (Balance)
    - `cancellation-YYYY-MM-DD.log` - Order cancellation (Cancel, CancellationTracking, OrderCleanup, TopBids)
    - `health-YYYY-MM-DD.log` - Health monitoring (Health, HealthCheck, ConnectivityMonitor)
    - `metadata-YYYY-MM-DD.log` - Token/rarity metadata (Metadata, Rarity)
    - `system-YYYY-MM-DD.log` - General/fallback (General, Security, Crypto, Cache, + unknown prefixes)
  - **Prefix-to-Category Mapping**: Central configuration routes each log prefix to appropriate file
  - **Unknown Prefix Handling**: Any unmapped prefix automatically routes to `system.log`
  - **Daily Rotation**: Each category file rotates by date (YYYY-MM-DD suffix)
  - Files modified: `src/utils/debug-file-logger.ts`
  - Resolves: 100MB+ monolithic log files difficult to analyze
  - Impact: Smaller ~5MB files per category, easier debugging by domain

- **Complete Console Logging Migration**: Replaced ALL remaining 130 `console.error` and `console.warn` calls with centralized `errorLog()`/`warnLog()` from `debug-file-logger.ts`
  - **Files Updated** (20 total):
    - `src/index.ts` - 69 console.error/warn replaced (Task, Queue, Cancellation, WebSocket, Counterbid, Marketplace categories)
    - `src/utils/websocket-manager.ts` - 27 silent catch blocks fixed with warnLog (all had commented-out `// console.log`)
    - `src/utils/task-state-manager.ts` - 11 console.error replaced with errorLog('TaskState', ...)
    - `src/utils/redis.ts` - 7 console.error/warn replaced with errorLog/warnLog('Redis', ...)
    - `src/marketplace/magiceden/list.ts` - 6 console.error consolidated into errorLog (listing errors with context)
    - `src/marketplace/opensea/list.ts` - 6 console.error consolidated into errorLog (listing errors with context)
    - `src/functions/listingScheduler.ts` - 6 console.error replaced with errorLog('ListingScheduler', ...)
    - `src/functions/tokenMetadata.ts` - 4 console.error replaced, RED/RESET colors removed
    - `src/functions/approve.ts` - 3 console.warn replaced with warnLog('Cache', ...)
    - `src/utils/bid-trace-logger.ts` - 3 console.error replaced with warnLog (to avoid recursive logging)
    - `src/marketplace/blur/bid.ts` - 2 console.error replaced with errorLog('Blur', ...)
    - `src/functions/rarity.ts` - 2 console.error replaced with errorLog('Rarity', ...)
    - `src/utils/db-retry.ts` - 2 console calls replaced with errorLog/warnLog('DB Retry', ...)
    - `src/utils/cancellation-debug-logger.ts` - 1 console.error replaced with warnLog
    - `src/functions/relistExpired.ts` - 1 console.error replaced with errorLog
    - `src/functions/cleanupExpiredPrices.ts` - 1 console.error replaced with errorLog
    - `src/marketplace/opensea/bid.ts` - 1 console.error replaced with errorLog
    - `src/utils/opensea-trait-floor.ts` - 1 duplicate console.error removed
    - `src/utils/listing-price-cache.ts` - 1 console.warn replaced with warnLog
  - **Silent Catch Blocks Fixed**: websocket-manager.ts had 27 catch blocks with `// console.log(error)` - all now log properly
  - **Color Constants Removed**: Removed RED, GOLD, RESET, YELLOW, BLUE, MAGENTA color codes from error messages
  - **Result**: 130 → 3 console.error/warn calls (2 in debug-file-logger.ts itself, 1 in test script)
  - **Impact**: 100% of production error logging now centralized to `debug-logs/server-debug-YYYY-MM-DD.log`
  - Files modified: 20 files, ~130 console calls replaced

### Removed
- **Removed Better Stack Logger (logger.ts)**: Completely removed the Better Stack logging integration
  - **Files Deleted**:
    - `src/utils/logger.ts` - Better Stack HTTP logging integration (~400 lines)
    - `src/utils/marketplace-file-logger.ts` - Redundant marketplace-specific file logger (~466 lines)
    - `src/types/marketplace-logs.types.ts` - Associated type definitions
  - **~200+ logger calls replaced** across the entire codebase with `errorLog()`, `warnLog()`, `debugLog()` from `debug-file-logger.ts`
  - **Files Updated**:
    - `src/index.ts` - 169 logger calls replaced
    - `src/marketplace/opensea/bid.ts` - 8 logger calls replaced
    - `src/marketplace/magiceden/bid.ts` - 25+ logger calls replaced
    - `src/marketplace/blur/bid.ts` - 14 logger calls replaced
    - `src/marketplace/opensea/list.ts` - 4 logger calls replaced
    - `src/marketplace/magiceden/list.ts` - 3 logger calls replaced
    - `src/utils/redis.ts` - 5 logger calls replaced
    - `src/utils/balance.ts` - 2 logger calls replaced
    - `src/utils/lock.ts` - 3 logger calls replaced
    - `src/utils/nft.ts` - 5 logger calls replaced
    - `src/utils/task-circuit-breaker.ts`, `src/utils/token-trait-cache.ts`, `src/utils/listing-price-cache.ts`, `src/utils/opensea-cache.ts`, `src/utils/opensea-trait-floor.ts`, `src/utils/redis-job-fallback.ts`, `src/utils/bullmq-error-formatter.ts`, `src/utils/collection-cache.ts`, `src/utils/wallet-cache.ts`
    - `src/functions/collection.ts`, `src/functions/listingScheduler.ts`, `src/functions/relistExpired.ts`, `src/functions/tokenMetadata.ts`, `src/functions/approve.ts`, `src/functions/cleanupExpiredPrices.ts`, `src/functions/list.ts`, `src/functions/priceMonitor.ts`
  - **Deprecated Endpoints**: `/api/logs/export` and `/api/logs` now return HTTP 501 "Endpoint no longer available"
  - **Log Output**: All logs now write to `debug-logs/server-debug-YYYY-MM-DD.log` instead of Better Stack
  - Impact: Simplified logging, removed external dependency, faster error logging, all logs in one place

### Changed
- **Migrated Function Files to debug-file-logger**: Replaced all logger utility calls in function files with errorLog() and warnLog() from debug-file-logger
  - **Files Migrated**:
    - `src/functions/collection.ts` - Removed 18 logger calls, now uses errorLog/warnLog for cache errors, API errors, collection errors
    - `src/functions/listingScheduler.ts` - Removed 15 logger calls, simplified with errorLog for queue/WebSocket errors
    - `src/functions/relistExpired.ts` - Removed 3 logger calls, now uses errorLog for listing/marketplace errors
    - `src/functions/tokenMetadata.ts` - Removed 5 logger calls, now uses errorLog for API/metadata errors
    - `src/functions/approve.ts` - Removed 1 logger call, now uses errorLog for blockchain errors
    - `src/functions/cleanupExpiredPrices.ts` - Removed 1 logger call, now uses errorLog
    - `src/functions/list.ts` - Removed 12 logger calls, now uses errorLog/warnLog for listing/database/API errors
    - `src/functions/priceMonitor.ts` - Removed 18 logger calls, now uses errorLog/warnLog for WebSocket/queue/listing errors
  - **Log Prefixes Used**: Collection, Listing, API, Database, Metadata, Blockchain, WebSocket, Queue, Cache
  - **Removed Dependencies**: No longer imports logger utility or ErrorCategory enums in these files
  - **Consistent Format**: All errors now follow `errorLog('Category', '[file.ts] Message with context | key=value', error)` pattern
  - Impact: Cleaner function files with simpler error handling, reduced MongoDB logger writes for non-critical errors

### Added
- **Full Logging Coverage - errorLog() and warnLog()**: Extended debug-file-logger with error and warning log levels
  - **New Functions**:
    - `errorLog(prefix, message, error?)` - Logs errors with full stack traces (ALWAYS logged, regardless of DEBUG flag)
    - `warnLog(prefix, message)` - Logs warnings (ALWAYS logged, regardless of DEBUG flag)
  - **Log Format**:
    - Errors: `[timestamp] [ERROR][prefix] message | error details`
    - Warnings: `[timestamp] [WARN][prefix] message`
  - **Files Updated with Error Logging**:
    - `src/index.ts` - 15+ silent catch blocks now log errors (queue rate limiting, counterbid handlers, health checks)
    - `src/marketplace/opensea/bid.ts` - Cancel order errors
    - `src/marketplace/blur/bid.ts` - Auth failures, cache errors, cancel errors, trait floor fetch errors
    - `src/marketplace/magiceden/bid.ts` - Cache read/write errors, trait floor fetch errors
    - `src/utils/order-cache.ts` - Database initialization errors
    - `src/utils/balance.ts` - Balance fetch errors, cache errors, GraphQL errors
    - `src/utils/redis.ts` - Redis connection errors, BullMQ connection errors
    - `src/functions/collection.ts` - Floor price fetch errors, cache errors
  - **Log Prefix Standards**:
    - `[ERROR][API]` - API call failures (OpenSea, Blur, MagicEden)
    - `[ERROR][Cache]` - Cache read/write failures
    - `[ERROR][Database]` - MongoDB/Redis connection errors
    - `[ERROR][Balance]` - WETH balance fetch failures
    - `[WARN][Queue]` - Job rescheduling failures (non-critical)
    - `[WARN][Health]` - Health check timeouts (non-critical)
    - `[WARN][Cancel]` - Cancellation minor issues
  - **Silent Catch Blocks Fixed**: ~50+ previously silent catch blocks now log useful diagnostics
  - Files modified: `src/utils/debug-file-logger.ts`, `src/index.ts`, `src/marketplace/*/bid.ts`, `src/utils/*.ts`, `src/functions/collection.ts`
  - Resolves: Silent error swallowing making debugging difficult
  - Impact: All errors and warnings now visible in debug log files for post-mortem analysis

- **Unified Debug File Logger**: Created centralized file-based debug logging system that writes to both console AND file
  - Enable via `DEBUG=true` environment variable
  - **Log File Location**: `nfttools/debug-logs/server-debug-YYYY-MM-DD.log` (daily rotation, project root)
  - **Features**:
    - Dual output: Console (for real-time monitoring) + File (for persistent history)
    - ISO timestamps in file logs for precise timing analysis
    - Daily log file rotation (new file each day)
    - Silent file write failures (won't crash app)
    - Same `DEBUG=true` flag controls all debug output
  - **Log Prefix Categories**:
    - `[TaskScheduler]` - Lock acquisition/release, timer registration, timer expiration, task completion flow
    - `[STARTUP]` - Server initialization sequence (HTTP server, workers, rate limiter, wallet cache)
    - `[ScheduledBid]` - Scheduled bid parent function entry (OS/Blur/ME)
    - `[BidFlow]` - Individual bid processor entry (all marketplaces and bid types)
    - `[Preprocessing]` - Marketplace preprocessing decisions
    - `[PriceCalc]` - Bid price calculation results
    - `[StopOption]` - Stop option trigger evaluation
  - **Example Output**:
    ```
    [2025-11-30T10:15:30.123Z] [TaskScheduler] Timer EXPIRED | timerKey=abc123-opensea | taskId=abc123 | marketplace=opensea
    [2025-11-30T10:15:30.125Z] [TaskScheduler] Lock ACQUIRED | lockKey=abc123-opensea
    [2025-11-30T10:15:30.130Z] [TaskScheduler] Calling startTask | taskId=abc123 | marketplace=opensea
    ```
  - **Implementation**:
    - Replaced 47 `if (DEBUG) console.log(...)` statements with `debugLog(prefix, message)` calls
    - All scheduler-related logging now uses file logger for complete scheduling flow visibility
  - Files created: `src/utils/debug-file-logger.ts`
  - Files modified: `src/index.ts` (replaced all debug logging with `debugLog()` function)
  - Resolves: Debug logs lost after terminal closed, difficulty analyzing scheduling issues
  - Impact: Complete persistent debug log history for diagnosing task scheduling and bid flow issues

- **Cancellation Debug Log File**: Added comprehensive file-based debug logging for task cancellation flow
  - Enable via `DEBUG=true` environment variable
  - **Log File Location**: `nfttools/cancellation-debug-logs/cancellation-debug-{taskId}.log` (project root, overwrites on each run)
  - **Phases Logged**:
    - `ENTRY`: stopTask called with task info, marketplace, stoppedByWarning flag
    - `WALLET`: Decryption success/failure, wallet address (truncated)
    - `BLACKLIST`: Task blacklisted to prevent new jobs
    - `JOB_CANCEL`: Jobs removed from queue (count)
    - `ORDERS_SOURCE`: Per-marketplace order source (cache, mongodb, or empty) with counts
    - `ORDERS_FETCH`: Summary of orders retrieved with sources
    - `STATE`: CANCELLING vs STOPPED decision with order counts
    - `CANCEL_QUEUE`: Orders queued per marketplace with sample orderHashes
    - `CANCEL_JOB`: Individual cancellation job results (SUCCESS/ERROR/SKIPPED with reason)
    - `CLEANUP`: Resources cleared
    - `COMPLETE`: Final status
    - `ERROR`: Error details if stopTask fails
  - **Output Format**: Human-readable with elapsed timestamps (MM:SS.mmm), no absolute timestamps
  - **File Structure**:
    ```
    ================================================================================
    CANCELLATION DEBUG LOG
    Task: {taskId} | Slug: {slug}
    ================================================================================
    [00:00.000] [ENTRY] stopTask called
                marketplace: all | stoppedByWarning: false
    [00:00.100] [ORDERS_SOURCE] opensea: Found 906 orders in MONGODB
    [00:00.200] [CANCEL_JOB] SUCCESS | orderId: 0xabc123def456...
    [00:00.300] [CANCEL_JOB] ERROR | orderId: 0x789xyz... | reason: API timeout
    ...
    ================================================================================
    SUMMARY
    Duration: 2.5s | Status: SUCCESS
    orderSources: {"opensea":"mongodb","magiceden":"empty","blur":"empty"}
    cancellationResults: {"success":850,"error":10,"skipped":3}
    ================================================================================
    ```
  - Files created: `src/utils/cancellation-debug-logger.ts`
  - Files modified: `src/index.ts` (stopTask function, bulkCancelOpenseaBid function)
  - Resolves: Difficulty debugging cancellation flow during task stop operations
  - Impact: Complete visibility into cancellation lifecycle including individual job results with DEBUG=true

### Fixed
- **Orders Not Cancelled When Cache Empty**: Fixed bug where `stopTask` would skip order cancellation if OrderCache was empty
  - **Root Cause**: `fetchOrdersForCancellation()` only queried in-memory cache, missing orders that existed in MongoDB but not in cache (e.g., task started before server restart, cache not populated)
  - **Solution**: Added MongoDB fallback - if cache returns 0 orders, query MongoDB `Order` collection
  - **Flow**: Cache first (fast path) → MongoDB fallback if cache empty (ensures no orders missed)
  - **Debug Log**: Added console log when fallback is used: `[fetchOrdersForCancellation] Cache miss for {marketplace}, found {count} orders in MongoDB`
  - Files modified: `src/index.ts` (fetchOrdersForCancellation function)
  - Resolves: Orders not being cancelled when stopping tasks that started before server restart
  - Impact: All orders are now properly cancelled regardless of cache state

### Changed
- **Order Lookups Migrated to In-Memory Cache**: Replaced ~35 MongoDB `Order.find()` calls with `OrderCache` in-memory lookups for faster bid processing
  - **Performance Impact**: O(1) cache lookups vs O(log n) database queries, 20-30% reduction in MongoDB queries during peak bidding
  - **OrderCache Methods Used**:
    - `orderCache.get(taskId, marketplace, identifier)` - Single order lookup
    - `orderCache.getByTaskAndMarketplace(taskId, marketplace)` - All orders for task/marketplace
    - `orderCache.getByTaskAndIdentifiers(taskId, identifiers, marketplace)` - Batch identifier lookup
  - **Functions Updated (Phase 1 - Counterbid Handlers & Scheduled Processors)**:
    - `handleMagicEdenCounterbid`: Single order lookup for token/trait/collection
    - `handleOpenseaCounterbid`: Single order lookup with expiry filter
    - `processOpenseaCollectionBid`, `processOpenseaTraitBid`, `processOpenseaTokenBid`
    - `processMagicedenCollectionBid`, `processMagicedenTokenBid`, `processMagicedenTraitBid`
    - Counterbid job processors: `openseaCollectionCounterBid`, `magicedenCollectionCounterBid`, `magicedenTraitCounterBid`, `blurCollectionCounterBid`, `blurTraitCounterBid`, `openseaTokenCounterBid`, `openseaTraitCounterBid`, `magicedenTokenCounterBid`
  - **Functions Updated (Phase 2 - Bulk Operations)**:
    - `handleMagicEdenCounterbid`, `handleOpenseaCounterbid`, `handleBlurCounterbid`: Batch cancellation lookups
    - `cancelRemovedTokenBids`: Identifier batch lookup with `getByTaskAndIdentifiers()`
    - `stopTask`: Task marketplace order cleanup
    - `attemptRemainingCancellations`: Active order retrieval
  - **Queries Kept as MongoDB** (require features not in cache):
    - Cache initialization query in `initializeTopBidsFromDatabase`
    - Pagination queries with `.skip().limit()` in `bulkCancelOpenseaBid`
    - Cross-task aggregation in `validateBidCount`
    - API endpoint `/task/:slug/orders` (fresh data + pagination)
  - **Type Compatibility**: Added type guards for `CachedOrder` interface which stores `expiresAt`/`createdAt` as timestamps (number) vs Date objects
  - Files modified: `src/index.ts`
  - Resolves: Database latency during time-critical counterbid operations
  - Impact: Faster counterbid response, reduced MongoDB load, more consistent latency

### Fixed
- **Missing taskId Filter in MagicEden Counterbid Handler**: Fixed bug where `allTaskOrders` query was missing `taskId` filter
  - **Root Cause**: Query used `slug` and `marketplace` but omitted `taskId`, potentially returning orders from other tasks with same collection
  - **Original Code**: `Order.find({ slug: currentTask.contract.slug, marketplace: MAGICEDEN.toLowerCase() })`
  - **Fixed Code**: `orderCache.getByTaskAndMarketplace(task._id, MAGICEDEN.toLowerCase())`
  - Files modified: `src/index.ts` (handleMagicEdenCounterbid ~line 12567)
  - Resolves: Incorrect order cancellation affecting wrong tasks
  - Impact: Orders are now correctly scoped to the specific task

### Fixed
- **Spread Comparison Floating-Point Precision**: Fixed false positives in `spread > outbidMargin` comparisons due to floating-point precision errors
  - **Root Cause**: JavaScript floating-point division produced values like `0.00010000000000000002` instead of clean `0.0001`, causing incorrect "spread greater than margin" evaluations when values should be equal
  - **Example**: `spread = 0.00010000000000000002` vs `outbidMargin = 0.0001` → comparison returns TRUE (should be FALSE)
  - **Solution**: Applied `roundToValidPrecision()` to spread before comparison, matching OpenSea's tiered precision rules:
    - 0.0001-0.0999 ETH: 4 decimals
    - 0.100-0.999 ETH: 3 decimals
    - 1.00+ ETH: 2 decimals
  - **Locations Fixed (8 total)**:
    - `processOpenseaCollectionBid`: spread calculation
    - `processOpenseaTraitBid`: spread calculation
    - `processOpenseaTokenBid`: spread calculation
    - `processMagicedenCollectionBid`: spread calculation
    - `processMagicedenTokenBid` (isOwnBid): spread calculation
    - `processMagicedenTokenBid` (isTopBid): spread calculation
    - `processMagicedenTraitBid` (isOwnBid): spread calculation
    - `processMagicedenTraitBid` (isTopBid): spread calculation
  - **Note**: Blur already uses `+spread.toFixed(2)` so no changes needed
  - Files modified: `src/index.ts`
  - Resolves: Unnecessary bid adjustments when spread equals margin, floating-point noise triggering incorrect bid optimization
  - Impact: Spread comparisons now use clean rounded values matching marketplace precision requirements

- **Spread Logic Bugs in Scheduled Bid Functions**: Fixed 7 bugs in spread-based bid optimization logic across MagicEden and Blur
  - **Bug 1 - MagicEden Empty else-if Fall-through (5 locations)**: When `spread > margin` but `potentialNewBid < minBid`, empty else-if blocks caused unintended fall-through to TTL check, potentially placing unwanted bids
    - `processMagicedenCollectionBid`: Now sets `collectionOffer = highestBidAmount` to maintain position
    - `processMagicedenTokenBid` (isOwnBid): Now sets `tokenOffer = highestBidAmount`
    - `processMagicedenTokenBid` (isTopBid): Now sets `tokenOffer = +order.offerPrice`
    - `processMagicedenTraitBid` (isOwnBid): Now sets `traitOffer = highestBidAmount`
    - `processMagicedenTraitBid` (isTopBid): Now sets `traitOffer = +order.offerPrice`
  - **Bug 2 - Blur Missing minBidPriceEth Check (2 locations)**: Blur could place bids below task's minimum bid price when lowering
    - `processBlurCollectionBid`: Added `&& bidPrice >= minBidPriceEth` to spread condition
    - `processBlurTraitBid`: Added `&& bidPrice >= minBidPriceEth` to spread condition
  - **Root Cause**: Spread logic is used when you're already the top bidder to optimize bid price (lower your bid to save money while staying ahead). The bugs caused: (1) MagicEden to place redundant bids when it couldn't lower, (2) Blur to place bids below minimum
  - Files modified: `src/index.ts`
  - Resolves: Unwanted bid placements, bids below minimum price
  - Impact: All marketplaces now correctly handle the "can't lower below min" case by maintaining current bid position

### Added
- **Comprehensive Debug Logging Audit**: Added ~25 new debug log statements across the entire bidding system from server startup to bid placement
  - Enable via `DEBUG=true` in environment
  - **Log Prefix Conventions**:
    - `[STARTUP]` - Server initialization sequence
    - `[ScheduledBid][XX-Sched]` - Scheduled bid parent function entry (OS/Blur/ME)
    - `[BidFlow][XX-Type]` - Individual bid processor entry (OS/Blur/ME + Token/Trait/Coll)
    - `[Preprocessing]` - Marketplace preprocessing decisions
    - `[PriceCalc]` - Bid price calculation results
    - `[StopOption]` - Stop option trigger evaluation
  - **Phase 1 - Server Startup Logs (10 locations)**:
    - Environment configuration loaded
    - HTTP server created
    - BullMQ queues initialized
    - Workers initialized (with count and concurrency)
    - Rate limiter initialized (with RATE_LIMIT value)
    - Wallet cache pre-loaded
    - Internet connectivity monitoring started
    - Task restart count (or "No previously running tasks")
    - Orphaned scheduled tasks recovered
    - Final "SERVER FULLY INITIALIZED" signal
  - **Phase 2 - Scheduled Bid Parent Functions (3 locations)**:
    - `processOpenseaScheduledBid`: ENTRY with slug, bidType, loop count
    - `processBlurScheduledBid`: ENTRY with slug, bidType, loop count
    - `processMagicedenScheduledBid`: ENTRY with slug, bidType, loop count
  - **Phase 3 - Individual Bid Processors (8 locations)**:
    - `processOpenseaTokenBid`: ENTRY with slug and tokenId
    - `processOpenseaTraitBid`: ENTRY with slug and traitKey
    - `processOpenseaCollectionBid`: ENTRY with slug
    - `processBlurCollectionBid`: ENTRY with slug
    - `processBlurTraitBid`: ENTRY with slug and traitKey
    - `processMagicedenCollectionBid`: ENTRY with taskId
    - `processMagicedenTokenBid`: ENTRY with slug and tokenId
    - `processMagicedenTraitBid`: ENTRY with slug and traitKey
  - **Phase 4 - Helper Functions (6 locations)**:
    - `performMarketplacePreprocessing`: ENTRY and EXIT with marketplace, bidType, token count, trait count, enabled status, job count, skip reason
    - `calculateBidPrice`: Result with floor, min, max, offer, and currency
    - `stopOption`: Multiple exit points for different trigger conditions (not enabled, trait floor triggered, floor triggered, max purchase triggered)
  - Files modified: `src/index.ts`
  - **Expected Output** (with DEBUG=true):
    ```
    [STARTUP] Environment configuration loaded
    [STARTUP] BullMQ queues initialized
    [STARTUP] ✅ SERVER FULLY INITIALIZED
    [ScheduledBid][OS-Sched] my-collection | ENTRY | bidType=token | loop=1
    [Preprocessing] my-collection | ENTRY | marketplace=opensea | bidType=token | tokens=5 | traits=0
    [Preprocessing] my-collection | EXIT | marketplace=opensea | enabled=true | jobCount=5
    [BidFlow][OS-Token] my-collection [1234] | ENTRY
    [PriceCalc] my-collection | floor=0.5 | min=0.1 | max=0.4 | offer=0.35 | ETH
    ```
  - Resolves: Missing visibility into server startup sequence, bid processing flow, preprocessing decisions, and price calculations
  - Impact: Complete end-to-end visibility into the bidding system with a single `DEBUG=true` flag

- **In-Memory Order Cache (OrderCache)**: Implemented high-performance in-memory cache for order lookups to replace MongoDB queries during bid processing
  - **Purpose**: Eliminate database round-trips during time-critical bid decisions (cancel/outbid logic)
  - **Cache Key Format**: `{taskId}:{marketplace}:{identifier}` for O(1) lookups
  - **CachedOrder Interface**: `_id`, `orderHash`, `expiresAt`, `offerPrice`, `createdAt`, `identifier`, `marketplace`, `slug`, `taskId`
  - **Indexes**:
    - `taskIndex` (Map<taskId, Set<cacheKey>>) for efficient task-level operations
    - `idIndex` (Map<orderId, cacheKey>) for deletion by order ID
    - `initializedTasks` (Set<taskId>) for lazy initialization tracking
  - **Key Methods**:
    - `set()`: Add/update order in cache with automatic index maintenance
    - `get()`: O(1) lookup by taskId/marketplace/identifier
    - `delete()`, `deleteById()`, `deleteByTask()`, `deleteByTaskAndMarketplace()`: Various deletion patterns
    - `getByTask()`, `getActiveByTask()`, `getByTaskAndMarketplace()`, `getByTaskAndIdentifiers()`: Query methods
    - `countGroupedByMarketplace()`: Get counts per marketplace (for bidStats)
    - `cleanupExpired()`: Remove expired orders from cache
    - `initializeFromDatabase()`: Lazy load orders for a task on first start
  - **Integration Points**:
    - **Bid Creation**: Cache updated immediately after `Order.create()` / `Order.insertMany()` in OpenSea, MagicEden, and Blur bid functions
    - **Bid Cancellation**: Cache updated in `cancelOpenseaOrdersDirectly()`, `cancelMagicEdenOrdersDirectly()`, `cancelBlurOrdersDirectly()`, and cancellation job handlers
    - **Task Start**: `orderCache.initializeFromDatabase(task._id)` called in `startTaskInternal()` after top bids initialization
    - **Task Stop**: `orderCache.deleteByTask(taskId)` called in `stopTask()` during cleanup
    - **bidStats**: `validateBidCountInternal()` now uses `orderCache.countGroupedByMarketplace()` instead of MongoDB aggregate
    - **Task Start bidStats**: Uses cache counts instead of `Order.aggregate()` query
  - **Debug Logging**: Enable via `DEBUG=true` environment variable
    - Logs SET, GET, DELETE operations with key and metadata
    - Logs CLEANUP operations with count of removed expired orders
    - Logs INIT operations with task ID and order count loaded
  - Files created: `src/utils/order-cache.ts`
  - Files modified: `src/marketplace/opensea/bid.ts`, `src/marketplace/magiceden/bid.ts`, `src/marketplace/blur/bid.ts`, `src/index.ts`
  - **Write-Through Pattern**: MongoDB Order collection still updated for client compatibility; cache updated after successful DB write
  - Resolves: Slow bid decisions due to database latency, especially during high-frequency bidding
  - Impact: O(1) order lookups during bid processing, reduced MongoDB load, faster bid response times

- **WebSocket Pipeline Debug Logging**: Added comprehensive debug logging throughout the entire WebSocket message pipeline to diagnose counterbid connectivity issues
  - Enable via `DEBUG=true` in environment (connection log is ALWAYS-ON)
  - **Problem Diagnosed**: Log analysis showed 0 counterbid activity despite 1891 scheduled bid logs working - WebSocket pipeline completely silent
  - **Log Prefixes Added**:
    - `[WebSocket]` - Connection status (ALWAYS-ON, not behind DEBUG flag)
    - `[WebSocket][Message]` - Raw message reception with eventType and size
    - `[WebSocket][Route]` - Message routing decision in handleMarketplaceMessage()
    - `[WebSocket][CounterBid]` - Counterbid handler entry with marketplace and slug
    - `[WebSocket][Subscribe]` - Subscription attempts and skip reasons
  - **Locations Instrumented**:
    - `wsManager.on('connected')`: ALWAYS logs connection with URL and connectionId
    - `wsManager.on('message')`: DEBUG logs eventType and message size at very start
    - `handleMarketplaceMessage()`: DEBUG logs routing decision (BID_CREATED, LISTING_CREATED, ITEM_SOLD)
    - `handleCounterBid()`: DEBUG logs marketplace and slug when bid event received
    - `subscribeToCollections()`: DEBUG logs collection count and slugs, also logs if WebSocket not connected
  - Files modified: `src/index.ts`
  - **Expected Output** (with DEBUG=true):
    ```
    [WebSocket] CONNECTED to wss://nfttools-ws-proxy.nfttools.io | connectionId=abc123
    [WebSocket][Subscribe] Subscribing to 7 collections: crypts-and-caverns, lil-mutants, ...
    [WebSocket][Message] Received: eventType=BID_CREATED | size=1234 bytes
    [WebSocket][Route] Processing: eventType=BID_CREATED | marketplace=opensea
    [WebSocket][CounterBid] Received bid event | marketplace=opensea | slug=crypts-and-caverns
    ```
  - **Diagnosis Guide**:
    - No `[WebSocket] CONNECTED` → WebSocket connection failing
    - Connected but no `[WebSocket][Subscribe]` → subscribeToCollections not called
    - Subscribe but no `[WebSocket][Message]` → Proxy not sending messages or subscription failed
    - Message but no `[WebSocket][Route]` → pQueue not processing messages
    - Route but no `[WebSocket][CounterBid]` → eventType not matching BID_CREATED
  - Resolves: Missing visibility into WebSocket pipeline failures
  - Impact: Complete diagnostic capability for counterbid connectivity issues

- **Counterbid Handler Debug Logging**: Added comprehensive debug logging to all 3 counterbid WebSocket handlers for improved visibility into counterbid decision-making
  - Enable via `DEBUG=true` in environment
  - **Log Prefix Pattern**: `[Counterbid][{ME|OS|Blur}] {slug} | MESSAGE`
  - **Handlers Updated**:
    - `handleMagicEdenCounterbid`: WS event receipt, dedup checks, own bid detection, counterbid disabled, pending checks, price validation, job queue
    - `handleOpenseaCounterbid`: WS event receipt, dedup checks, own bid detection, counterbid disabled, pending checks, price validation, job queue
    - `handleBlurCounterbid`: WS event receipt, counterbid disabled, trait matching, price validation (min/max), job queue for trait and collection bids
  - **Log Points per Handler**:
    - WS Event: `[Counterbid][XX] {slug} | WS Event: {type} | Type: {bidType} | Price: {price} | Maker: {maker}`
    - Dedup: `[Counterbid][XX] {slug} | Dedup Check: orderHash={hash} | Result: {PASS|BLOCKED}`
    - Own Bid: `[Counterbid][XX] {slug} | SKIP: Own bid detected`
    - Disabled: `[Counterbid][XX] {slug} | SKIP: Counterbid disabled for task`
    - Pending: `[Counterbid][XX] {slug} | Pending Check: identifier={id} | Result: {PASS|BLOCKED}`
    - Price: `[Counterbid][XX] {slug} [{identifier}] | Price Check: incoming={x} min={y} max={z} | Result: {VALID|BELOW_MIN|ABOVE_MAX}`
    - Queue: `[Counterbid][XX] {slug} [{identifier}] | QUEUE: {Token|Trait|Collection} counterbid at {price}`
  - Files modified: `src/index.ts`
  - Documentation: Added "Counterbid Handler Debug Logging" and "Counterbid Job Processor Debug Logging" sections to `BIDDING-FUNCTIONS.md` with flow diagrams
  - **Note**: Job processor debug logs (`[BidFlow][XX-XX-CB]`) were already implemented - this adds handler-level logs (`[Counterbid][XX]`) and comprehensive documentation for both
  - Resolves: Missing visibility into counterbid handler decision points
  - Impact: Complete visibility into why counterbids are queued, skipped, or blocked at each decision point (handlers) and why they succeed or fail during execution (job processors)

### Fixed
- **Bid Price Changes Not Taking Effect While Task Running**: Fixed bug where editing `bidPrice`, `openseaBidPrice`, `blurBidPrice`, or `magicEdenBidPrice` while a task is running did not take effect until task restart
  - **Root Cause**: `updateTaskData()` stored `dbTask.toObject()` which has `_id` as ObjectId, but `fetchCurrentTasks()` converts `_id` to string. This type inconsistency caused tasks fetched from `activeTasks` to have ObjectId `_id` which broke subsequent lookups when comparing against string keys.
  - **Solution**: Convert `_id` and `user` to strings in `updateTaskData()` after calling `toObject()`, matching the pattern established in `fetchCurrentTasks()`
  - Files modified: `src/index.ts` (updateTaskData function ~line 6362)
  - Resolves: Bid price changes (min, max, hardCap) not applying until task restart
  - Impact: All task field changes now take effect immediately on the next bid loop without requiring task restart

- **Always Cancel Previous Orders Before Bidding**: Fixed active bid count exceeding total bid count due to orphaned orders not being cancelled
  - **Root Cause**: When bids were skipped (max exceeded, own bid still valid), existing orders were not cancelled. The `cancelMaxWhenOutbid` flag controlled cancellation, but when false, orders accumulated.
  - **Solution**: Orders are now ALWAYS cancelled before any bid processing decision, regardless of `cancelMaxWhenOutbid` flag
  - **Pattern Applied**: All 8 scheduled bid functions now cancel orders unconditionally at the start of bid processing
  - **Functions Fixed**:
    - `processOpenseaTokenBid`: Now always cancels orders when max exceeded
    - `processOpenseaTraitBid`: Removed redundant `cancelMaxWhenOutbid` check (orders already cancelled earlier)
    - `processOpenseaCollectionBid`: Removed redundant `cancelMaxWhenOutbid` block
    - `processMagicedenTokenBid`: Now always cancels orders when max exceeded
    - `processMagicedenTraitBid`: Removed redundant `cancelMaxWhenOutbid` block
    - `processMagicedenCollectionBid`: Removed redundant `cancelMaxWhenOutbid` block
    - `processBlurCollectionBid`: Removed redundant `cancelMaxWhenOutbid` block
    - `processBlurTraitBid`: Removed redundant `cancelMaxWhenOutbid` block
  - Files modified: `src/index.ts`
  - Resolves: Active bid count showing higher than total bid count, orphaned orders accumulating when bot can't compete
  - Impact: Clean order state maintained regardless of bid outcome, consistent behavior across all marketplaces

### Changed
- **Unified Debug Logging System**: Consolidated 6 separate debug environment variables into a single `DEBUG=true` variable
  - **Previous State**: 6 different debug variables across 5 files:
    - `DEBUG_SCHEDULED_BID` (27 uses in index.ts)
    - `DEBUG_TOP_BIDS` (11 uses in index.ts)
    - `DEBUG_JOB_KEYS` (5 uses in index.ts)
    - `DEBUG_WETH_BALANCE` (10 uses in balance.ts)
    - `DEBUG_AXIOS_RETRY` (1 use in init.ts)
    - `DEBUG` (9 uses in logger files)
  - **Solution**: All replaced with single `const DEBUG = process.env.DEBUG === 'true'` at top of each file
  - **Log Prefixes**: Consistent prefixes to distinguish log sources when DEBUG=true:
    - `[ScheduledBid][OS-Token]` / `[ScheduledBid][OS-Trait]` - Scheduled bid decisions
    - `[TopBids]` - Top bid store operations
    - `[WETH Balance]` - Balance fetching
    - `[JobKeys]` - Job deduplication
    - `[BidFlow][OS-Token]` / `[BidFlow][OS-Trait]` - New comprehensive bid flow logs
  - Files modified: `src/index.ts`, `src/utils/balance.ts`, `src/init.ts`, `.env`
  - Resolves: Confusing multiple debug variables, inconsistent debug behavior
  - Impact: Single `DEBUG=true` in .env now enables all debug logging across the codebase

### Added
- **Comprehensive BidFlow Debug Logging**: Added `[BidFlow]` prefixed logs to ALL 19 bidding job processors for complete bid execution lifecycle visibility
  - Enable via `DEBUG=true` in environment
  - **Log Pattern**: `[BidFlow][PREFIX] {slug} [{identifier}] | MESSAGE`
  - **Prefixes by Function**:
    - Scheduled Bids: `[OS-Sched]`, `[OS-Coll]`, `[OS-Token]`, `[OS-Trait]`, `[Blur-Sched]`, `[Blur-Coll]`, `[Blur-Trait]`, `[ME-Sched]`, `[ME-Coll]`, `[ME-Token]`, `[ME-Trait]`
    - Counterbids: `[OS-Coll-CB]`, `[OS-Token-CB]`, `[OS-Trait-CB]`, `[Blur-Coll-CB]`, `[Blur-Trait-CB]`, `[ME-Coll-CB]`, `[ME-Token-CB]`, `[ME-Trait-CB]`
  - **Log Points per Processor**:
    - Early exits: `SKIP: {reason}` (marketplace stopped, task blacklisted, not enabled, floor outside range, insufficient balance, bid exceeds max)
    - Price decisions: `Floor: X ETH`, `PriceCalc: offer=X min=Y max=Z CURRENCY`
    - Cancellation: `Cancelling N existing orders before bid/counterbid`
    - Bid placement: `PLACING BID: X CURRENCY` or `PLACING COUNTERBID: X CURRENCY`
  - **Functions Updated** (19 total):
    - OpenSea: `processOpenseaScheduledBid`, `processOpenseaCollectionBid`, `processOpenseaTokenBid`, `processOpenseaTraitBid`, `openseaCollectionCounterBid`, `openseaTokenCounterBid`, `openseaTraitCounterBid`
    - Blur: `processBlurScheduledBid`, `processBlurCollectionBid`, `processBlurTraitBid`, `blurCollectionCounterBid`, `blurTraitCounterBid`
    - MagicEden: `processMagicedenScheduledBid`, `processMagicedenCollectionBid`, `processMagicedenTokenBid`, `processMagicedenTraitBid`, `magicedenCollectionCounterBid`, `magicedenTokenCounterBid`, `magicedenTraitCounterBid`
  - Files modified: `src/index.ts`
  - Resolves: Missing visibility into early exit reasons and bid flow decisions across all marketplaces
  - Impact: Complete end-to-end visibility into why bids are skipped, stopped, or placed for all marketplaces

### Fixed
- **Active Bid Count Shows 0 After Server Restart**: Fixed "Active: 0 / 1" display bug where UI showed 0 active bids despite orders existing in database
  - **Root Cause**: `bidStats` object was empty after server restart because `validateBidCount()` only runs every 60 seconds via setInterval. Clients connecting before the first interval tick received empty counts.
  - **Solution**: Two-part fix ensures bidStats is always populated:
    1. **Server Startup**: Call `validateBidCount()` immediately after `fetchCurrentTasks()` to populate bidStats before clients connect
    2. **Task Start**: Initialize bidStats for each task when it starts by querying active orders from database
  - **Changes**:
    - Added `await validateBidCount()` call after `fetchCurrentTasks()` in server initialization (line ~3068)
    - Added Order.aggregate query in `startTaskInternal()` to populate bidStats when task starts (lines ~7706-7726)
    - Added fallback to empty counts with error logging if query fails
  - Files modified: `src/index.ts`
  - Resolves: Active count showing 0 even when bids were placed successfully
  - Impact: UI immediately shows correct active bid counts after server restart and when tasks start

### Added
- **WETH Balance Debug Logging**: Added comprehensive debug logging for WETH balance fetch operations
  - Enable via `DEBUG_WETH_BALANCE=true` environment variable
  - Logs cache hits (fresh and stale), cache misses, and fresh fetch results
  - Shows wallet address (truncated for privacy), balance in wei and ETH, and elapsed time
  - Log format: `[WETH Balance] {action}: wallet={truncated}, balance={wei} ({eth} ETH), elapsed={ms}ms`
  - Helps diagnose balance fetch issues and cache behavior
  - Files modified: `src/utils/balance.ts`

### Fixed
- **Balance Log Display Format**: Fixed balance logs showing incorrect ETH values
  - **Root Cause**: OpenSea GraphQL API returns balance in ETH format, but logs were applying `formatWeiToEth()` which divided by 1e18 again
  - **Symptom**: Logs showed `balance=0.05 (0.000000 ETH)` instead of `balance=0.05 ETH`
  - **Solution**: Removed redundant `formatWeiToEth()` calls from all 8 balance log statements
  - Files modified: `src/utils/balance.ts` (lines 85, 125, 152, 177, 210, 239, 282, 316)
  - Resolves: Confusing balance display in debug logs
  - Impact: Balance logs now correctly show ETH values

- **Redundant Redis Configuration Logging**: Fixed Redis config being logged 6 times on startup
  - **Root Cause**: `getRedisConfig()` logs configuration but is called once per Redis client creation (6 clients)
  - **Solution**: Added static flag `redisConfigLogged` to ensure config is logged only once
  - Files modified: `src/utils/redis.ts`
  - Resolves: Log noise during server startup
  - Impact: Redis configuration logged once instead of 6 times


- **Floating Point Precision in Debug Log Display**: Fixed debug log output showing ugly floating point values like `0.017499999999999998` instead of clean values like `0.0175`
  - **Root Cause**: JavaScript floating point arithmetic produced values with excessive decimal places in debug log console.log statements
  - **Solution**: Applied `roundToValidPrecision()` to all bid price values displayed in debug logs
  - **Fixes Applied** (6 locations across 3 functions):
    - `processOpenseaCollectionBid`: Lowering bid log (line 15310), Outbidding log (line 15340)
    - `processOpenseaTraitBid`: Lowering bid log (line 16563), Outbidding log (line 16588)
    - `processOpenseaTokenBid`: Lowering bid log (line 16943), Outbidding log (line 16972)
  - Files modified: `src/index.ts`
  - Resolves: Unreadable debug log output with 17+ decimal places
  - Impact: Debug logs now display clean, human-readable bid prices

- **OpenSea Offer Precision Compliance for Trait and Collection Bids**: Applied `roundToValidPrecision()` to all bid calculations in `processOpenseaTraitBid` and `processOpenseaCollectionBid`
  - **Root Cause**: While `processOpenseaTokenBid` was already fixed, trait and collection bid functions were missing precision rounding, violating OpenSea's requirements (January 28, 2025)
  - **Precision Rules**:
    - 0.0001-0.0999 ETH: 4 decimals allowed
    - 0.100-0.999 ETH: 3 decimals allowed
    - 1.00+ ETH: 2 decimals allowed
  - **Bugs Fixed in `processOpenseaTraitBid`** (4 locations):
    - Line 16560: Lowering bid - now uses `roundToValidPrecision(potentialNewBidEth)`
    - Line 16577: Refresh same price - now uses `roundToValidPrecision(highestBidAmount / Math.pow(10, decimal))`
    - Line 16584-16585: Outbid competitor - now calculates `newBidEth` and applies `roundToValidPrecision()`
    - Line 16617: Min bid fallback - now uses `roundToValidPrecision(minBidPriceEth)`
  - **Bugs Fixed in `processOpenseaCollectionBid`** (3 locations):
    - Line 15308: Lowering bid - now uses `roundToValidPrecision(potentialNewBidEth)`
    - Line 15330: Refresh same price - now uses `roundToValidPrecision(highestBidAmount / Math.pow(10, decimal))`
    - Line 15337-15338: Outbid competitor - now calculates `newBidEth` and applies `roundToValidPrecision()`
  - Files modified: `src/index.ts`
  - Resolves: "Invalid bid precision" errors for trait and collection bids
  - Impact: All OpenSea bid types (token, trait, collection) now comply with precision requirements

- **OpenSea Offer Precision Compliance (January 28, 2025)**: Applied `roundToValidPrecision()` to all token bid price calculations in `processOpenseaTokenBid`
  - **Root Cause**: OpenSea is enforcing new price-based decimal place limits for all offers starting January 28, 2025
  - **Precision Rules**:
    - 0.0001-0.0999 ETH: 4 decimals allowed (e.g., 0.0542 ETH valid, 0.00992 invalid)
    - 0.100-0.999 ETH: 3 decimals allowed (e.g., 0.235 ETH valid, 0.1001 invalid)
    - 1.00+ ETH: 2 decimals allowed (e.g., 1.05 ETH valid, 1.003 invalid)
  - **Changes Applied**:
    - Initial offer calculation now rounds via `roundToValidPrecision(offerPriceEth)`
    - Lowering bid (spread > margin) now rounds `potentialNewBidEth` before conversion
    - Renewing bid at same price now rounds `highestBidAmount` before conversion
    - Outbidding competitor now rounds `newBidEth` before conversion
    - Min bid price fallback now rounds `minBidPriceEth` before conversion
  - Files modified: `src/index.ts:16874,16939,16959,16967-16968,16998`
  - Resolves: Potential "invalid bid precision" errors from OpenSea API after January 28, 2025
  - Impact: All OpenSea token bids will comply with new precision requirements

### Changed
- **Balance API Requests Get Highest Priority**: Balance fetch requests now use priority 0 (highest) in Bottleneck limiter
  - Balance checks jump to the front of the queue even when bid requests are backed up
  - Uses `limiter.schedule({ priority: 0 }, ...)` instead of default priority 5
  - Files modified: `src/utils/balance.ts:47`
  - Impact: Faster balance detection, prevents delayed insufficient balance errors when queue is busy

- **WETH Balance Fetching via OpenSea GraphQL**: Replaced slow Alchemy RPC balance fetching with fast OpenSea GraphQL API
  - **Root Cause**: Alchemy RPC calls for WETH balance were timing out massively (10+ seconds), causing cascading job failures and log flooding
  - **Solution**: Use OpenSea's `addressBalances` GraphQL query which is faster and more reliable (already used for bids)
  - **Lean GraphQL Query**: Minimal query fetching only `contractAddress`, `chain.identifier`, and `quantity` fields
  - **Changes**:
    - Created `fetchWethBalanceGraphQL()` function using OpenSea GraphQL API
    - Updated `fetchERC20Balance()` to use GraphQL instead of Alchemy RPC
    - Updated `fetchERC20BalanceFresh()` to use GraphQL instead of Alchemy RPC
    - Integrated with existing Bottleneck rate limiter via `limiter.schedule()`
    - Maintained existing Redis caching (60s fresh, 1hr stale fallback)
    - Removed ethers `Contract` and `formatEther` imports (no longer needed)
  - Files modified: `src/utils/balance.ts`
  - Resolves: Balance fetch timeouts, log flooding, cascading job failures
  - Impact: Balance fetching now uses OpenSea API (same as bids), eliminating Alchemy RPC timeout issues

### Added
- **Comprehensive Bid Trace Logging System**: Created production-ready bid trace logging for diagnosing duplicate bid issues (17-20+ bids on same token within seconds)
  - **Purpose**: Track every bid attempt through its complete lifecycle with correlation IDs, enabling root cause analysis of duplicate bid issues
  - **Type Definitions** (`src/types/bid-trace.types.ts`):
    - `BidTraceEntry`: Complete bid event record with traceId, correlationId, stage, marketplace, taskId, tokenId, etc.
    - `BidTraceStage`: Comprehensive stages - ws_received, dedup_orderhash_check/passed/blocked, dedup_pending_check/passed/blocked, top_bid_check/skip, job_queued/started/completed/failed, bid_api_start/success/error, cancel_start/complete, early_return
    - `BidTraceFilter`, `BidTraceSummary`, `BidTraceContext` for querying and correlation
  - **Logger Class** (`src/utils/bid-trace-logger.ts`):
    - File-based JSON logging to `logs/bid-traces/bid-traces-{YYYY-MM-DD}.log`
    - 1-day retention with automatic cleanup of old log files
    - In-memory trace contexts with 5-minute TTL for elapsed time calculation
    - Helper methods: `createTrace()`, `log()`, `logDedupCheck()`, `logJobQueued()`, `logBidResult()`, `logEarlyReturn()`, `logTopBidCheck()`
    - Query methods: `getTraces()`, `findDuplicates()`, `exportTraces()` (JSON/CSV)
    - Environment config: `BID_TRACE_ENABLED` (default true), `BID_TRACE_LEVEL` (default info)
  - **WebSocket Handler Instrumentation**:
    - `handleCounterBidForTask`: Creates trace with correlationId based on orderHash
    - `handleOpenseaCounterbid`: Logs seenOrderHashes dedup, pendingCounterbids dedup, ourTopBids check, job queuing
    - `handleMagicEdenCounterbid`: Same trace logging pattern
    - `handleBlurCounterbid`: Key traces for dedup and job queuing
  - **Job Processor Instrumentation** (all counterbid processors):
    - `openseaTokenCounterBid`: Full lifecycle logging (job_started, early_return, cancel_start/complete, bid_api_start/success/error, job_completed/failed)
    - `magicedenTokenCounterBid`: Same pattern
    - `magicedenTraitCounterBid`: Same pattern
    - `blurCollectionCounterBid`: Same pattern
    - `blurTraitCounterBid`: Same pattern
  - **Scheduled Bid Processor Instrumentation** (all 9 scheduled bid functions):
    - `processOpenseaTokenBid`: Full lifecycle logging (job_started, all early_return reasons, bid_api_start/success/error, job_completed/failed)
    - `processOpenseaTraitBid`: Same pattern with trait-specific traitKey tracking
    - `processOpenseaCollectionBid`: Same pattern for collection-level bids
    - `processMagicedenTokenBid`: Same pattern with tokenId tracking
    - `processMagicedenTraitBid`: Same pattern with traitKey tracking
    - `processMagicedenCollectionBid`: Same pattern for collection-level bids
    - `processBlurCollectionBid`: Same pattern for collection-level bids
    - `processBlurTraitBid`: Same pattern with traitKey tracking
    - Early return reasons tracked: marketplace_stopped_insufficient_balance, no_enabled_marketplaces, task_not_active_or_marketplace_disabled, task_blacklisted, marketplace_not_selected, marketplace_disabled, stop_option_triggered, insufficient_balance, bid_still_valid, own_bid_still_valid_ttl, max_bid_price_exceeded, offer_exceeds_floor, below_minimum_offer
  - **API Endpoints**:
    - `GET /api/bid-traces?taskId=&tokenId=&marketplace=&stage=&startTime=&endTime=&limit=&offset=`: Query traces with filtering and pagination
    - `GET /api/bid-traces/export?taskId=&format=json|csv`: Export traces for download
    - `GET /api/bid-traces/duplicates?taskId=&tokenId=&timeWindowMs=`: Find duplicate bid groups for debugging
  - **Interface Updates**: Added `traceContext?: BidTraceContext` to `IProcessOpenseaTokenBidData`, `IMagicedenTokenBidData`, `IMagicedenTraitBidData`, `IBlurBidData`
  - Files created: `src/types/bid-trace.types.ts`, `src/utils/bid-trace-logger.ts`
  - Files modified: `src/index.ts` (counterbid handlers, job processors, API endpoints)
  - Resolves: Diagnosis of 17-20+ duplicate bids on same token, visibility into deduplication failures and race conditions
  - Impact: Complete visibility into bid lifecycle from WebSocket receipt through API completion

### Fixed
- **Truncated Trace IDs in Console Output**: Fixed trace IDs being truncated to first 12 characters in console logs
  - Root cause: `bid-trace-logger.ts` line 208 used `traceId.substring(0, 12)...` for console output
  - Now displays full trace ID for easier correlation and debugging
  - Files modified: `src/utils/bid-trace-logger.ts:208`
  - Resolves: Difficulty correlating log entries when trace IDs appear truncated

- **Balance Fetch Timeout Too Aggressive**: Increased balance fetch timeout from 5s to 10s to reduce timeout errors
  - Root cause: 5-second timeout was too short for some Alchemy RPC responses under load
  - Logs showed ~50 `[fetchERC20BalanceFresh] Failed` timeout errors in production
  - Increased timeout to 10 seconds to account for network latency
  - Improved error message to include actual error reason (not just elapsed time)
  - Files modified: `src/utils/balance.ts:191-192,278-279`
  - Resolves: Excessive balance fetch timeouts causing unnecessary stale cache fallbacks

- **processOpenseaTokenBid Reliability Improvements**: Fixed multiple bugs and added comprehensive debug logging
  - **Bug Fixes**:
    - Order query now filters by `expiresAt: { $gt: new Date() }` to exclude expired orders (line 16729)
    - Fixed duplicate wallet fetch - now validates wallet early and reuses for privateKey decryption
    - Added proper null guards for TTL calculations when no active orders exist (prevents NaN/Invalid Date)
    - Removed duplicate `errorStats` initialization (dead code)
  - **Debug Logging**: Added `DEBUG_SCHEDULED_BID` environment variable for development debugging
    - Logs order state: active orders count, TTL, current price, new offer
    - Logs TTL skip decisions with reason
    - Logs outbid logic decisions: isOwnBid, topBid, secondBid, margin
    - Logs each outbid branch: lowering bid, cannot lower, skip, renew, outbid competitor
  - **Trace Logging**: Added bid trace logging for:
    - `wallet_not_found` early return
    - `bid_still_valid` TTL skip with metadata (ttl, currentPrice, tokenOffer, expiry)
  - Files modified: `src/index.ts`
  - Enable debug: `DEBUG_SCHEDULED_BID=true`

- **Reliable Top Bid Comparison for OpenSea Token Counterbids**: Fixed unreliable bid comparison that could skip valid counterbid opportunities
  - **Root Cause**: The `handleOpenseaCounterbid` function used an in-memory `ourTopBids` store to compare incoming bids against our current bids. This store could become stale because:
    1. Orders expire naturally but the store was never cleared on expiration
    2. Store was not synchronized with the database
    3. Could result in skipping valid counterbid opportunities when we thought we had a top bid that had actually expired
  - **Solution**: Replace in-memory store lookup with Order model queries (source of truth)
  - **Changes**:
    - Handler Order query (line 12851): Added `expiresAt: { $gt: new Date() }` filter to only consider active orders
    - Top bid check (line 12945): Replaced `getOurTopBid()` in-memory lookup with derivation from the already-queried `orders` array
    - Job processor Order query (line 17731): Added `expiresAt: { $gt: new Date() }` filter for consistency
    - Reduced `seenOrderHashes` cache TTL from 30 minutes to 5 minutes (line 381)
  - Files modified: `src/index.ts`
  - Resolves: Missed counterbid opportunities due to stale in-memory bid data
  - Impact: Counterbid decisions now use reliable database queries filtered by order expiration

- **processOpenseaTraitBid Reliability Improvements**: Fixed multiple bugs and added comprehensive debug logging
  - **Bug Fixes**:
    - Order query now filters by `expiresAt: { $gt: new Date() }` to exclude expired orders (line 16406)
    - Added early wallet validation with `wallet_not_found` trace logging (prevents late cryptic errors)
    - Added proper null guards for TTL calculations using `hasActiveOrder` check (prevents NaN/Invalid Date)
  - **Debug Logging**: Uses same `DEBUG_SCHEDULED_BID` environment variable
    - Logs order state: active orders count, TTL, current price, new offer
    - Logs TTL skip decisions with reason
    - Logs outbid logic decisions: isOwnBid, topBid, secondBid, margin
    - Logs each outbid branch: lowering bid, cannot lower (below min), TTL skip, renew, outbid competitor
  - **Trace Logging**: Added bid trace logging for:
    - `wallet_not_found` early return with wallet metadata
    - `bid_still_valid` TTL skip with metadata (ttl, currentPrice, traitOffer, expiry)
  - Files modified: `src/index.ts` (processOpenseaTraitBid function lines 16269-16616)

- **processOpenseaCollectionBid Reliability Improvements**: Fixed multiple bugs and added comprehensive debug logging
  - **Bug Fixes**:
    - Order query now filters by `expiresAt: { $gt: new Date() }` to exclude expired orders (line 15177)
    - Added early wallet validation with `wallet_not_found` trace logging after task validation
    - Removed duplicate wallet fetch - now validates early and reuses for privateKey decryption
    - Added proper null guards for TTL calculations using `hasActiveOrder` check (prevents NaN/Invalid Date)
  - **Debug Logging**: Uses same `DEBUG_SCHEDULED_BID` environment variable
    - Logs order state: active orders count, TTL, current price, new offer
    - Logs TTL skip decisions with reason
    - Logs outbid logic decisions: isOwnBid, topBid, secondBid, margin
    - Logs each outbid branch: lowering bid, cannot lower (below min), TTL skip, renew, outbid competitor
  - **Trace Logging**: Added bid trace logging for `bid_still_valid` TTL skip with metadata
  - Files modified: `src/index.ts` (processOpenseaCollectionBid function lines 15109-15453)

- **Wallet Not Found Error Cascade Prevention**: Fixed cascade of 800+ errors triggered by deleted/missing wallets during bulk cancel operations
  - **Root Cause**: When a wallet was deleted from MongoDB but tasks still referenced it, `bulkCancelOpenseaBid` and `bulkCancelMagicedenBid` threw errors that cascaded into multiple downstream failures
  - **Error Cascade Pattern**: `Wallet not found` (199 errors) → `Failed to cancel OpenSea order` (204 errors) → `Failed to cancel order` (213 errors) → `Worker Job failed` (213 errors)
  - **Solution**: Multi-layer fix to prevent wallet-related cascades:
  - **Fix 1 - Graceful Handling in bulkCancelOpenseaBid**: Changed from throwing error to graceful handling
    - Log warning instead of error when wallet not found
    - Clean up order from database anyway (prevents orphaned orders)
    - Update cancellation tracker and bid counter
    - Return early without throwing (prevents cascade)
  - **Fix 2 - Graceful Handling in bulkCancelMagicedenBid**: Applied same pattern for MagicEden
  - **Fix 3 - Task Start Wallet Validation**: Added pre-flight wallet check in `startTaskInternal`
    - Validates wallet exists before starting any task
    - Validates wallet has `encryptedPrivateKey`
    - Returns `{ success: false, reason: 'wallet_not_found' }` with structured error logging
    - Prevents tasks from starting if wallet is missing
  - **Fix 4 - Database Retry Logic**: Added retry mechanism to `validateBidCount`
    - 3 retries with exponential backoff (1s, 2s, 4s delays)
    - Extracted logic to `validateBidCountInternal()` for clean retry wrapper
    - Falls back to zero counts on persistent failure (prevents crash)
    - Reduces `titan_SERVER_DATABASE_FAILED` errors from transient DB issues
  - Files modified: `src/index.ts` (bulkCancelOpenseaBid, bulkCancelMagicedenBid, startTaskInternal, validateBidCount)
  - Files created: `docs/ERROR_TRACKING.md` (error catalog and troubleshooting guide)
  - Resolves: 829 cascading errors from single root cause (wallet not found)
  - Impact: Wallet lookup failures now handled gracefully without cascade, tasks cannot start with missing wallets

- **Duplicate Token Counterbid Spamming (20+ bids on same token)**: Fixed critical race condition where multiple competitors bidding on the same token caused 20+ duplicate counterbids
  - **Root Cause**: Existing deduplication used `orderHash` (unique per competitor bid), not `tokenId`. When 20 competitors bid on token #6375, each had a unique orderHash → all 20 passed deduplication → 20 duplicate counterbids placed
  - **Solution**: Added token-level deduplication using `pendingCounterbids` BoundedCache to ensure only ONE counterbid job per token at a time
  - **Implementation Details**:
    - Added `pendingCounterbids` cache with 60-second TTL and 10k item capacity
    - Added `markCounterbidPending()` and `clearCounterbidPending()` helper functions
    - OpenSea token counterbid: Added deduplication check before job queue, try/catch around queue.add, finally block in job handler
    - MagicEden token counterbid: Added deduplication check before job queue, try/catch around queue.add, clearCounterbidPending on all early returns, finally block in job handler
  - Files modified: `src/index.ts:386-405` (cache), `src/index.ts:12808-12842` (OS handler), `src/index.ts:11947-12071` (ME handler), `src/index.ts:17001-17010` (OS job), `src/index.ts:17174-17177` (ME job)
  - Resolves: 20+ duplicate bids on same token within seconds, wasted gas fees
  - Impact: Maximum 1 counterbid per token per ~60 seconds instead of unlimited duplicates

- **MongoDB Transaction Error ("Transaction numbers are only allowed on a replica set member")**: Fixed error when updating tasks via WebSocket
  - **Root Cause**: `updateTaskData()` function used MongoDB transactions (`startSession`/`startTransaction`) which require replica set configuration
  - **Symptom**: Error `MongoServerError: Transaction numbers are only allowed on a replica set member or mongos` when client updates task settings
  - **Solution**: Removed unnecessary transaction code - single-document updates are atomic by MongoDB design, making transactions redundant
  - **Changes**:
    - Removed `mongoose.startSession()` and `session.startTransaction()` calls
    - Removed `session` parameter from `Task.findOneAndUpdate()` options
    - Removed `session.commitTransaction()`, `session.abortTransaction()`, and `session.endSession()` calls
    - Removed transaction-related error handling and rollback logic
    - Simplified debug logging (removed ~30 lines of transaction boilerplate)
  - Files modified: `src/index.ts:6253-6722`
  - Resolves: Task update failures when MongoDB is running as standalone instance
  - Impact: Task updates now work with both standalone MongoDB and replica sets without requiring manual `rs.initiate()` command

- **Excessive InsufficientBalanceError Logging (Docker Log Flooding)**: Fixed log flooding when wallet has insufficient balance for bulk token bids
  - **Root Cause**: When a task started with insufficient balance, thousands of bid jobs were already queued. Each job failed with `InsufficientBalanceError` and logged separately, generating 6000+ log entries in seconds.
  - **Symptom**: Docker logs filling with identical balance error messages, potentially exhausting disk space and crashing the bot
  - **Solution**: Two-part fix to prevent log flooding and skip unnecessary job processing:
  - **Fix 1 - Early Exit on Balance Stop**: Added `isMarketplaceStoppedDueToBalance()` check at the start of all bid handlers
    - Jobs exit immediately if marketplace already marked with `insufficientBalance: true`
    - Prevents thousands of redundant balance checks and error logs
  - **Fix 2 - Log Throttling**: Added `loggedBalanceErrors` BoundedCache to track logged balance errors
    - `shouldLogBalanceError()` returns true only for FIRST error per task/marketplace
    - Subsequent balance errors for same task/marketplace are suppressed from console
  - **Updated Handlers** (8 total):
    - `processOpenseaTokenBid`, `processOpenseaTraitBid` - early exit + log throttling
    - `processMagicedenTokenBid`, `processMagicedenTraitBid` - early exit only (uses `logBidError` which dedupes in DB)
    - `processBlurCollectionBid`, `processBlurTraitBid` - early exit only
  - Files modified: `src/index.ts`
  - Resolves: Docker log flooding, disk space exhaustion, wasted CPU cycles on doomed jobs
  - Impact: Maximum ~1-3 balance error logs per task/marketplace instead of thousands

- **Duplicate Bid Spamming (17+ bids on same token)**: Fixed critical race condition causing duplicate counterbids
  - **Root Cause 1 - Fire-and-Forget Task Processing**: `relevantTasks.map()` in `handleCounterBid()` created promises without awaiting them, allowing multiple WebSocket messages to race through deduplication
  - **Root Cause 2 - Non-Atomic Deduplication**: The `hasSeenItem()` + `addSeenItem()` pattern had a race window between check and add
  - **Root Cause 3 - Missing await on processBulkJobs**: Blur trait batch processing didn't await job queueing (line 15441)
  - **Fix 1**: Wrapped `relevantTasks.map()` with `await Promise.all()` to ensure sequential message processing (line 11777)
  - **Fix 2**: Added atomic `addIfNotExists()` method to `BoundedCache` for race-free deduplication
  - **Fix 3**: Added `markSeenIfNew()` helper that atomically checks and adds in one operation
  - **Fix 4**: Updated MagicEden and OpenSea counterbid handlers to use atomic deduplication
  - **Fix 5**: Added `await` to `processBulkJobs()` call in Blur trait batch processing
  - Files modified: `src/index.ts`, `src/utils/bounded-cache.ts`
  - Resolves: 17+ duplicate bids on same token within seconds, wasted gas fees, unexpected bid acceptances
  - Impact: Each incoming competitor bid now results in exactly ONE counterbid, not 17+

- **Fast Balance Drop Detection**: Fixed slow detection (up to 30 minutes) when wallet balance drops suddenly
  - **Root Cause**: Bot only stopped marketplace when its own `InsufficientBalanceError` was thrown from pre-bid balance checks, not when OpenSea API returned balance errors
  - **Symptom**: Hundreds of failed bids with OpenSea error "does not have enough balance of payment asset... found: X, require: Y" before bot detected the issue
  - **Solution**: Parse OpenSea's API balance error message in job failure handler, invalidate cache, and stop affected marketplaces immediately
  - **Implementation Details**:
    - Added `invalidateBalanceCache()` function to `src/utils/balance.ts` for immediate cache clearing
    - Added `parseOpenseaBalanceError()` to extract found/required amounts and detect currency from error message
    - Added `getMarketplacesForCurrency()` mapping: WETH → [opensea, magiceden], BETH → [blur], USDC → [magiceden]
    - Added `getTokenAddressForCurrency()` for cache invalidation with correct token address
    - Updated job failure handler (lines 4931-4980) to detect balance errors and stop all affected marketplaces
    - Currency detection from error message: checks for BETH/USDC token addresses, defaults to WETH
  - **Currency-Based Marketplace Stopping**:
    - WETH balance error → stops both OpenSea AND MagicEden (both use WETH)
    - BETH balance error → stops only Blur
    - USDC balance error → stops only MagicEden
  - Files modified: `src/utils/balance.ts`, `src/index.ts`
  - Resolves: Slow balance drop detection, hundreds of wasted failed bid attempts
  - Impact: Balance drops detected in < 1 second (vs previous 30+ minutes), immediate marketplace stopping

- **Counterbid Wasted Gas on Non-Winning Bids**: Fixed critical inefficiency where counterbidding would place bids even when not becoming the top bidder
  - **Root Cause**: Counterbid handlers only compared incoming bids against our OWN bids in the database, not against the marketplace's actual top offer
  - **Scenario**: If marketplace top offer is 1.0 ETH and we bid 0.8 ETH, any incoming bid (e.g., 0.85 ETH) would trigger a counterbid even though we'd still be outbid
  - **Solution**: Implemented local `ourTopBids` store that tracks our confirmed top bids per task/marketplace/identifier
  - **Implementation Details**:
    - Added `TopBidStore` interface and `ourTopBids` Map to track bid state in memory
    - Helper functions: `getOurTopBid()`, `updateOurTopBid()`, `clearOurTopBid()`, `clearTaskTopBids()`
    - `initializeTopBidsFromDatabase()` loads active orders on task start
    - Updated all 7 counterbid handlers to check store before proceeding:
      - OpenSea: token, trait, collection handlers
      - MagicEden: token, trait, collection handlers
      - Blur: trait, collection handlers
    - Updated all 8 scheduled bid jobs to update store after successful bids:
      - `processOpenseaTokenBid`, `processOpenseaTraitBid`, `processOpenseaCollectionBid`
      - `processMagicedenTokenBid`, `processMagicedenTraitBid`, `processMagicedenCollectionBid`
      - `processBlurCollectionBid`, `processBlurTraitBid`
    - Updated all 7 counterbid job processors to update store after successful counterbids:
      - `openseaTokenCounterBid`, `openseaTraitCounterBid`
      - `magicedenTokenCounterBid`, `magicedenTraitCounterBid`
      - `blurCollectionCounterBid`, `blurTraitCounterBid`
    - Store cleanup on task stop via `clearTaskTopBids()` in `stopTask()`
    - Blur price handling: Store uses wei, Blur uses ETH - conversion applied in handlers
  - Debug logging available with `DEBUG_TOP_BIDS=true` environment variable
  - Files modified: `src/index.ts` (lines 223-372 for store, multiple counterbid handlers)
  - Resolves: Wasted gas fees on bids that won't win, unnecessary API calls, inefficient counterbid behavior
  - Impact: Counterbids only placed when we'll actually become the new top bidder, significantly reducing wasted gas

- **Duplicate Bids / Bid Spamming Issue**: Fixed critical bug causing multiple bids on the same NFT tokens within seconds
  - **Root Cause 1 - Premature Cache Deletion**: `seenOrderHashes.delete()` was called immediately after queuing counterbid jobs, allowing duplicate WebSocket messages to bypass deduplication before the job completed
  - **Root Cause 2 - Missing `await` on Counterbid Handlers**: `handleOpenseaCounterbid()` and `handleMagicEdenCounterbid()` were called without `await`, creating race conditions where multiple concurrent messages could race through deduplication
  - **Root Cause 3 - Rate Limiter at 2x Configured Limit**: Internal rate limiter was set to `RATE_LIMIT * 2`, effectively doubling the actual API request rate
  - **Root Cause 4 - Fire-and-Forget processBulkJobs**: Initial token/trait bid jobs were queued without `await`, flooding the queue with hundreds of jobs simultaneously
  - **Fix 1**: Removed all 7 `seenOrderHashes.delete()` calls across MagicEden and OpenSea counterbid handlers - now relies on 30-minute TTL for cache expiration
    - Lines removed: 11791 (ME token), 11983 (ME trait), 12201 (ME collection), 12550 (OS token), 12794 (OS trait), 12984/12986 (OS collection - had duplicate delete calls)
  - **Fix 2**: Added `await` to `handleOpenseaCounterbid()` and `handleMagicEdenCounterbid()` calls to ensure sequential processing per task
  - **Fix 3**: Changed rate limiter from `RATE_LIMIT * 2` to `RATE_LIMIT` at line 6590
  - **Fix 4**: Added `await` to all 4 `processBulkJobs()` calls (OpenSea token/trait at lines 14219/14273, MagicEden token/trait at lines 17245/17317)
  - Files modified: `src/index.ts`
  - Resolves: Duplicate bids on same tokens, HTTP 429 rate limit errors, bid spamming during counterbid scenarios
  - Impact: Each incoming competitor bid now results in exactly one counterbid, not 4-5 duplicates

### Added
- **Development-Only Rate Limit Debug Logging**: Added comprehensive diagnostics for tracking actual request rates
  - Logs actual RPS vs configured RATE_LIMIT every 5 seconds in development
  - Shows queue depth, concurrent executions, and completion rates
  - Warns when actual RPS exceeds RATE_LIMIT or when queue depth grows excessively
  - Added Bottleneck event listeners to track dropped jobs and rate limit errors (429/430)
  - Only enabled when `DEBUG_RATE_LIMIT=true` in .env or `NODE_ENV=development`
  - Output format includes:
    - Configured RATE_LIMIT
    - Actual RPS (last 1 second)
    - Average RPS (10 second window)
    - Queue depth (jobs waiting)
    - Concurrent executions (jobs running)
    - Total completed jobs
    - Visual warnings when limits are exceeded
  - Files modified: `src/init.ts:90-113`, `src/init.ts:135-194`
  - Helps diagnose: Rate limiting effectiveness, burst traffic patterns, queue buildup
  - Impact: Developers can see exactly how many requests/second are being made and identify rate limit violations

- **WebSocket Debug Logging**: Added comprehensive debug logging throughout the entire WebSocket message pipeline
  - Logs connection establishment to MARKETPLACE_WS_URL with connection metadata (URL, clientId, connectionId)
  - Tracks subscription operations when tasks start (bidding tasks and listing collections)
  - Logs message reception with eventType, message size in bytes, and timestamp
  - Shows pQueue state when messages are added (pending count, queue size)
  - Logs message processing start with eventType and timestamp
  - Tracks routing decisions for each eventType (BID_CREATED, LISTING_CREATED, ITEM_SOLD)
  - Adds entry logs to marketplace-specific handlers (handleBlurMessages, handleOpenSeaMessages, handleMagicEdenMessages)
  - Shows extracted data from each marketplace (collection slug, contract address)
  - Logs message completion with total processing time in milliseconds
  - Enhanced error logging throughout the pipeline with context and timing
  - Only enabled when `DEBUG_WEBSOCKET=true` in .env or `NODE_ENV=development`
  - Log format: `[WebSocket][Phase] Details` for easy filtering and grep
  - Files modified: `src/index.ts:10994-11157` (connection phase), `src/index.ts:11099-11134` (reception), `src/index.ts:11181-11236` (processing), `src/index.ts:13293-13372` (marketplace handlers)
  - Helps diagnose: WebSocket connectivity issues, message processing delays, marketplace-specific parsing failures, queue buildup
  - Impact: Complete visibility into WebSocket pipeline from connection through message completion

- **Auto-Stop Running Tasks/Listings on Server Restart**: Ensures clean state on every server restart
  - All tasks with `running: true` automatically set to `running: false` on server startup
  - All listings with `running: true` automatically set to `running: false` on server startup
  - Executes unconditionally immediately after MongoDB connection is established
  - Prevents orphaned running tasks/listings from previous server sessions
  - Users must manually restart tasks/listings via dashboard after server restart
  - Removed redundant password-gated stop logic (now runs unconditionally regardless of password)
  - Files modified: `src/index.ts`
  - Improves: Server state consistency, prevents race conditions, clearer restart behavior
  - Impact: Clean slate on every restart - no tasks/listings run until user explicitly restarts them

- **Rarity Bid Type Support**: Server now recognizes and processes rarity bid types
  - Added "rarity" to bidType enum in task model (collection | token | rarity)
  - Added rarityCategory field to task schema (LEGENDARY | EPIC | RARE)
  - Server treats rarity bids identically to token bids for processing
  - Updated 9 bid type checks in preprocessing and job creation logic
  - Rarity tokens fetched by client are processed like regular token bids
  - OpenSea and MagicEden support rarity bids; Blur correctly skips them
  - Files modified: `src/models/task.model.ts`, `src/index.ts`
  - Enables: Users can create bids targeting specific rarity tiers (LEGENDARY/EPIC/RARE)

- **Password-Gated Startup System**: Server now waits gracefully for password before starting operations
  - Server automatically stops all running tasks and listings if no password is available on startup
  - Auto-stops prevent tasks/listings from attempting decryption operations without password
  - Listing scheduler initialization skipped when no password available
  - Task starting operations blocked until password is provided
  - Password checks added to `initializeListingScheduler()`, `processListingJob()`, and `startTask()`
  - Rate-limited error logging prevents console spam (max 1 log per minute)
  - Clear startup messages indicate server is in "paused" state awaiting password
  - Operations automatically resume when password is set via API
  - Files modified: `src/index.ts`, `src/functions/listingScheduler.ts`, `src/utils/crypto.ts`
  - Resolves: Console log spam with "No password available" errors, unauthorized decryption attempts

- **Bulletproof Task Update System**: Implemented comprehensive reliability improvements for bid price updates
  - Added MongoDB transaction support in `updateTaskData()` for atomic database-memory synchronization
  - Database updates now persist BEFORE in-memory state changes, ensuring data consistency
  - Implemented rollback mechanism if in-memory updates fail after database commit
  - Added WebSocket message idempotency tracking with 5-minute TTL to prevent duplicate processing
  - Implemented acknowledgment protocol for all WebSocket messages with success/error/duplicate status
  - Added message validation to ensure required fields are present before processing
  - Server now sends acknowledgments for every processed message including duplicates
  - Idempotency cache automatically cleans up entries older than 5 minutes
  - Files modified: `src/index.ts`
  - Resolves: Message loss on WebSocket disconnect, database-memory state divergence, duplicate message processing

- **Centralized Rarity Token Fetching API**: Migrated rarity token fetching from client to server with rate limiting
  - Created server-side `/api/rarity` Express endpoint to fetch rarity tokens from OpenSea GraphQL API
  - Integrated Bottleneck rate limiter to prevent OpenSea HTTP 429 rate limit errors
  - Implemented two-tier caching: Redis (1-hour TTL) + MongoDB (persistent) for rarity token results
  - Added distributed locking with `lockManager.withLock()` to prevent duplicate expensive API calls
  - Preserved early stopping optimization: stops fetching when lower rarity tiers appear (LEGENDARY → EPIC → RARE)
  - Migrated `RarityCache` MongoDB model from client to server for persistent caching
  - Recursive pagination with cursor-based fetching to retrieve all tokens for a rarity category
  - Files created: `src/models/rarityCache.model.ts`, `src/functions/rarity.ts`
  - Files modified: `src/functions/index.ts`, `src/index.ts`
  - Resolves: Client-side API calls bypassing rate limiter, potential HTTP 429 errors during rarity bidding setup
  - Impact: All OpenSea API calls now centralized through server rate limiter for consistent rate limiting

### Fixed
- **MongoDB Transaction Error for Task Updates**: Fixed "Transaction numbers are only allowed on a replica set member" error
  - Root cause: MongoDB was configured as standalone instance, but transactions require replica set configuration
  - Error occurred when WebSocket received bidPrice.max updates and called `updateTaskData()` with session parameter
  - Added `--replSet rs0` flag to MongoDB command in `compose.optimized.yaml`
  - Updated `.env.example` to include `?replicaSet=rs0` parameter in MONGODB_URI connection string
  - Added comprehensive debug logging throughout `updateTaskData()` transaction flow (12 checkpoints)
  - Debug logs track session creation, transaction lifecycle, database updates, and in-memory synchronization
  - Created `REPLICA_SET_INIT.md` guide with one-time initialization command for Docker deployments
  - Files modified: `compose.optimized.yaml:109-116`, `.env.example:1-5`, `src/index.ts:5952-6467`
  - Files created: `REPLICA_SET_INIT.md`
  - Resolves: Transaction failures when updating task bid prices via WebSocket
  - Impact: Bulletproof Task Update System now works correctly with atomic database-memory synchronization
  - Required action: Run one-time replica set initialization after first Docker container start (see REPLICA_SET_INIT.md)

- **Blur WebSocket Message Validation**: Fixed 38+ "BLUR_INCOMPLETE" warnings from malformed WebSocket messages
  - Added validation before accessing nested `message['1']` property in Blur counterbid handler
  - Prevents null reference errors and skips processing of invalid Blur messages
  - Now logs detailed warning when Blur sends incomplete/malformed messages
  - Files modified: `src/index.ts:11348-11362`
  - Resolves: Recurring WebSocket warnings and potential crashes from null references
  - Impact: Eliminates console spam and improves Blur marketplace reliability

- **Trading Disabled Cache TTL**: Extended trading disabled cache from 1 hour to 24 hours
  - Reduced wasted API calls on tokens with "Trading is not enabled" errors
  - Trading restrictions rarely change quickly, so longer cache is more efficient
  - Cache still allows retry after 24 hours in case restrictions are lifted
  - Files modified: `src/utils/trading-disabled-cache.ts:18-22`
  - Resolves: Repeated 400 errors on same disabled tokens
  - Impact: Reduces API waste and improves overall success rate

- **API Key Expiration Detection**: Added critical alerting for OpenSea API key expiration (HTTP 401)
  - Detects "API key has expired" errors and logs critical alerts
  - Provides clear console output indicating manual renewal is required
  - Added detailed error context including task ID, token ID, and collection
  - Prevents silent failures by clearly identifying authentication issues
  - Files modified: `src/index.ts:15765-15788`
  - Resolves: Silent API key expiration failures with unclear error messages
  - Impact: Immediate visibility when API keys need renewal, preventing prolonged outages

- **OpenSea Listing Fee Validation Error**: Fixed OpenSea marketplace listing failures caused by incorrect fee configuration
  - Updated hardcoded OpenSea fee from 50 basis points (0.5%) to 100 basis points (1%)
  - Integrated marketplace fee cache system (`getOpenseaMarketplaceFee()`) for dynamic fee management
  - Fixed hardcoded `BigInt(50)` values in `startAmount` and `endAmount` calculations
  - Files modified: `src/marketplace/opensea/list.ts`
  - Resolves error: "Validation error: An incorrect OpenSea fee has been provided. Expecting 100 basis points, but received 50."

- **Counterbid Race Condition**: Fixed critical race condition where new bids were placed before old bids were cancelled
  - Root cause: 6 of 7 counterbid functions used asynchronous cancellation (queued jobs), allowing new bids to be placed before old bids finished cancelling
  - Result: Multiple active bids could exist simultaneously on the same item/trait/collection, wasting gas fees and risking unexpected bid acceptances
  - Created `cancelMagicEdenOrdersDirectly()` and `cancelBlurOrdersDirectly()` helper functions for synchronous order cancellation
  - Standardized all counterbid functions to use synchronous cancellation (matching `openseaTraitCounterBid` pattern)
  - Updated 6 counterbid functions to cancel orders synchronously BEFORE placing new bids:
    - `openseaTokenCounterBid` - now uses `cancelOpenseaOrdersDirectly()`
    - `openseaCollectionCounterBid` - now uses `cancelOpenseaOrdersDirectly()`
    - `magicedenTokenCounterBid` - now uses `cancelMagicEdenOrdersDirectly()`
    - `magicedenTraitCounterBid` - now uses `cancelMagicEdenOrdersDirectly()`
    - `blurTraitCounterBid` - now uses `cancelBlurOrdersDirectly()`
    - `blurCollectionCounterBid` - now uses `cancelBlurOrdersDirectly()`
  - `openseaTraitCounterBid` already used synchronous cancellation (was the correct pattern)
  - Files modified: `src/index.ts:10662-10749` (new helper functions), `src/index.ts:16079-16173,16252-16301,16308-16407,16410-16505,16510-16616,16725-16778` (counterbid functions)
  - Resolves: Race conditions causing double-bidding, wasted gas fees, unpredictable bid state
  - Impact: All marketplaces now guarantee clean state (old bid cancelled) before placing new counterbids

- **Task Update Index Mismatch**: Fixed contradictory error in `updateTaskData()` causing task update failures
  - Root cause: Function used index from `currentTasks` array to lookup task in `activeTasks` Map
  - The index position in the array doesn't correspond to the Map's entry order, causing lookup failures
  - Error manifested as: "Found existing task at index 19" followed immediately by "ERROR: Task not found in memory at index 19"
  - Changed from index-based lookup `getActiveTaskWithIdByIndex(existingTaskIndex)?.task` to direct Map lookup `activeTasks.get(task._id)`
  - Enhanced debug logging to show both array index and Map existence status for better diagnostics
  - Files modified: `src/index.ts:5980-6000`
  - Resolves: Task update failures when `currentTasks` and `activeTasks` become desynchronized
  - Impact: Task updates now reliably find tasks using consistent ID-based lookups instead of unreliable index-based lookups

- **API Key Validation and Debugging**: Enhanced API key configuration validation and debugging capabilities
  - Added startup validation to ensure `API_KEY` environment variable is properly loaded
  - Added startup logging showing first 8 characters of loaded API key for verification
  - Added debug logging before OpenSea API calls to track API key presence and request details
  - Removed redundant `dotenv.config()` calls from `collection.ts` to prevent configuration conflicts
  - Files modified: `src/index.ts`, `src/functions/collection.ts`
  - Helps diagnose HTTP 401 Unauthorized errors related to missing or invalid API keys

### Fixed
- **HTTP 429 Rate Limit Errors from Retry Storms**: Stopped retrying 429/430 rate limit errors
  - Root cause: axios-retry was configured to retry 429/430 errors, compounding rate limit violations
  - Each failed request was retried 3 times, turning 1 failed request into 4 total API calls
  - Removed `status === 429 || status === 430` from axios-retry `retryCondition` function
  - Removed retry delay logic for 429/430 errors (now dead code)
  - Files modified: `src/init.ts:71-82`
  - Resolves: Retry storms that overwhelm OpenSea API rate limits
  - Impact: Rate limit errors now fail fast instead of compounding the problem

### Changed
- **Reverted Bottleneck Rate Limiter Configuration**: Removed maxConcurrent and reservoir token bucket settings
  - Removed `maxConcurrent: RATE_LIMIT` from Bottleneck configuration in `src/init.ts`
  - Removed reservoir token bucket configuration (reservoir, reservoirRefreshAmount, reservoirRefreshInterval)
  - Bottleneck now configured with only `minTime` and `trackDoneStatus` settings
  - Files modified: `src/init.ts:93-96`
  - Reason: maxConcurrent and reservoir settings were making HTTP 429/430 errors worse
  - Impact: Simpler rate limiting strategy using only time-based throttling
- **Debug Log Cleanup**: Removed redundant debug logging from collection functions
  - Removed all 20 `console.log` and `console.error` statements from collection.ts
  - Cleaned up 14 active debug logs and 6 commented-out debug logs
  - All error tracking now uses the centralized logger utility for consistency
  - Cache errors now fail silently with inline comments instead of console spam
  - Files modified: `src/functions/collection.ts`
  - Improves: Log cleanliness, reduces console noise, maintains consistent error tracking

- **Enhanced Cache Error Logging**: Added detailed error logging for cache operations
  - Added console.error logging for 7 cache-related catch blocks in collection.ts
  - Each error log includes source file identifier, cache key, collection slug, and detailed error information
  - Logs include error message and stack trace where appropriate
  - Format: `[collection.ts] Error description: { contextual details }`
  - Fixed ESLint @typescript-eslint/no-unused-vars warnings for unused error variables
  - Files modified: `src/functions/collection.ts`
  - Affected functions: Redis cache write, Blur floor cache/fetch, Magic Eden floor cache/fetch, collection items cache, trait floors cache
  - Improves: Error visibility for cache operations, debugging capabilities

## [2025-11-20]

### Fixed
- **TypeScript Type Errors**: Fixed type mismatches after MagicEden API return type changes
  - Updated `collection.ts` to parse string stats directly as number
  - Updated `index.ts` to convert `magicEdenFloorPrice` string to number
  - Commit: `c92a573` (2025-11-20)

- **MagicEden Collection Stats API Errors**: Migrated to v4 API endpoint to resolve HTTP 400 errors
  - Updated `fetchMagicEdenCollectionStats` to use `/magiceden/v4/assets` instead of deprecated `/magiceden/v3/rtp/ethereum/tokens/v7`
  - Updated query parameters to match v4 API requirements (`includeCollectionStats`, `includeMarket`, `sortBy`, `sortDir`, `collectionId`)
  - Changed response parsing to use `priceV2.amount.native` instead of `price.amount.decimal`
  - Changed return type to `string` to preserve full precision without floating point loss
  - Files modified: `src/marketplace/magiceden/bid.ts`
  - Commit: `2ab1f80` (2025-11-20)

### Changed
- **Memory Optimization**: Reduced Node.js memory allocation for improved resource management
  - Decreased `max-old-space-size` from 4096MB to 2048MB in nodemon configuration
  - Updated package dependencies with peer dependency markers
  - Files modified: `nodemon.json`, `package-lock.json`
  - Commit: `338a580` (2025-11-20)
