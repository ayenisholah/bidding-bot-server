# Changelog

All notable changes to the Bidding Bot Server will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **tokenHasTrait Function Debug Logging (2025-12-04)**: Added comprehensive debug logs to `tokenHasTrait()` function to trace trait checking during Token-vs-Trait defense.
  - **Purpose**: Diagnose why Token-vs-Trait defense completes silently - now we can see exactly what `tokenHasTrait()` is doing
  - **Debug Logs Added** (`src/functions/tokenMetadata.ts`):
    - ENTRY: Logs function call with contract address (truncated), tokenId, and trait being checked
    - Cache HIT: Logs when trait is found in cache (fast path)
    - Cache MISS: Logs when cache doesn't have the trait and API fetch is needed
    - Metadata fetch FAILED: Logs when `fetchTokenMetadata()` returns null
    - Metadata fetched: Logs traits count and first 5 traits found on the token
    - EXIT: Logs final result (true/false) and what trait was being looked for
  - **Expected Log Output** (token with cache miss):
    ```
    [Metadata] tokenHasTrait ENTRY | contract=0x1234abcd... | tokenId=1001 | trait=HEADWEAR:HAT
    [Metadata] tokenHasTrait: Cache MISS | tokenId=1001 | fetching metadata...
    [Metadata] tokenHasTrait: Metadata fetched | tokenId=1001 | traitsCount=8 | traits=[BACKGROUND:Blue, EYES:Laser, HEADWEAR:Crown, MOUTH:Smile, BODY:Gold (+3 more)]
    [Metadata] tokenHasTrait EXIT | tokenId=1001 | looking for HEADWEAR:HAT | result=false
    ```
  - Files: `src/functions/tokenMetadata.ts` (lines 363-401)
  - Resolves: Missing visibility into why `tokenHasTrait()` returns true/false
  - Impact: Can now see cache hits/misses, actual traits on tokens, and why trait matching succeeds or fails

- **Token-vs-Trait Counterbid Debug Logging (2025-12-04)**: Added comprehensive debug logs to trace Token-vs-Trait defense execution for diagnosing silent completion issues.
  - **Problem Identified**: 26+ `Token-vs-Trait ENTRY` logs in production had ZERO follow-up output - defense was completing silently with no visibility into why no counterbids were queued
  - **OpenSea Debug Logs Added** (`handleOpenseaCounterbid` Token-vs-Trait block):
    - After `tokenHasTrait()` call: Logs `tokenHasTrait=${hasMatchingTrait}` for each token checked against the incoming trait
    - After loop completion: Logs `Loop COMPLETE | tokensChecked=X | counterbidsQueued=Y` to show summary even when no counterbids queued
  - **MagicEden Debug Log Added** (`handleMagicEdenCounterbid` Token-vs-Trait block):
    - When token has trait but no cached order: Logs `SKIP - no cached order for this token (token has trait but no active bid)`
  - **Expected Log Output** (OpenSea, token task with 5 tokens, none have trait):
    ```
    [Counterbid] [OS] my-collection | Token-vs-Trait ENTRY | tokenCount=5 | traitType=HEADWEAR | traitValue=HAT
    [Counterbid] [OS] my-collection [1001] | Token-vs-Trait: tokenHasTrait=false | checking trait=HEADWEAR:HAT
    [Counterbid] [OS] my-collection [1002] | Token-vs-Trait: tokenHasTrait=false | checking trait=HEADWEAR:HAT
    ...
    [Counterbid] [OS] my-collection | Token-vs-Trait: Loop COMPLETE | tokensChecked=5 | counterbidsQueued=0
    ```
  - Files: `src/index.ts` (lines ~13576-13650 for OpenSea, ~12442-12445 for MagicEden)
  - Resolves: Missing visibility into Token-vs-Trait defense execution and why counterbids are/aren't queued
  - Impact: Can now diagnose whether tokens have the incoming trait, and why the loop completes with zero counterbids

### Fixed
- **Insufficient Balance Incorrectly Stopping Tasks (2025-12-04)**: Fixed critical bug where tasks were incorrectly stopped when wallet balance was greater than task min bid price.
  - **Problem**: Task `692f4a90091617c457c5f569` (digital-slop) was stopped due to "insufficient balance" even though wallet balance (0.0129 WETH) was greater than task min bid (0.0061 WETH)
  - **Root Cause 1**: Regex mismatch - error message format is `available 0.0129...` (space) but regex looked for `available:\s*` (colon), causing parse failure
  - **Root Cause 2**: When regex failed to parse amounts, fallback passed `balance=0` to `stopMarketplaceDueToInsufficientBalance()`, which always triggers stop since `0 < minBid`
  - **Solution**:
    - Fixed regex patterns to match both formats: `available[:\s]\s*([\d.]+)` (colon OR space)
    - Changed fallback behavior: when amounts can't be parsed, set `warningBids[taskId][marketplace] = true` instead of stopping marketplace
    - Task only stops when we can CONFIRM balance < min bid price
  - **Expected Behavior Now**:
    - Balance parsed successfully AND balance < minBid → STOP marketplace (no bids possible)
    - Balance parsed successfully AND balance >= minBid → SKIP this bid, continue others
    - Balance parsing FAILED → Set warning flag, skip bid, continue others (don't stop)
  - Files: `src/index.ts` (handleInsufficientBalanceError function, lines 560-623)

- **Tasks Stuck at 100% Running (2025-12-04)**: Fixed critical bug where tasks became stuck at `running=true` with `runningPercentage=100` indefinitely.
  - **Problem**: Tasks `692f4700091617c457c5f52e` (chronoforge) and `692f4a90091617c457c5f569` (digital-slop) were stuck, with scheduler logging 782+ errors
  - **Root Cause**: `checkTaskCompletionAndScheduleNext(taskId)` was being called without a marketplace parameter from 3 recovery functions, causing immediate return with error: `ERROR: Called without marketplace parameter for task {taskId}`
  - **Solution**: Changed all 3 call sites to iterate through `task.selectedMarketplaces` and call with each marketplace:
    - Line 25378: `logJobTrackingStatus()` - stuck task recovery
    - Line 25688: `checkStuckTasks()` - 99% completion force
    - Line 25742: `checkStuckTasks()` - 30-second timeout force
  - Files: `src/index.ts`

- **TypeScript Compilation Errors (2025-12-03)**: Fixed 8 TypeScript errors in `src/index.ts` - TaskStatus initialization, LogMetadata marketplace type, MongoDB topology access.

- **Server Startup Race Condition (2025-12-03)**: Fixed "Client must be connected" errors by moving `setInterval()` calls inside `startServer()` after MongoDB connection.

- **Circuit Breaker Cascade (2025-12-03)**: Fixed 26-minute outage from overly aggressive circuit breaker settings; updated thresholds (failureThreshold: 5→10, resetTimeout: 60s→120s).
  - Files: `src/functions/collection.ts`

- **MongoDB Connection Pool Monitoring (2025-12-03)**: Added DEBUG-mode pool stats logging every minute for diagnosing connection issues.
  - Files: `src/index.ts`

- **Blur Floor Prices Returning 0 (2025-12-02)**: Fixed type mismatch where `fetchBlurCollectionStats()` return was accessed incorrectly; added circuit breaker for Blur API.
  - Files: `src/functions/collection.ts`

- **Insufficient Balance Log Flooding (2025-12-02)**: Added intelligent balance handling - stops marketplace when balance < min, skips individual bids otherwise.
  - Files: `src/index.ts`

- **MagicEden Timeout (2025-12-02)**: Increased timeout from 120s to 600s for large token count tasks.
  - Files: `src/index.ts`

- **Internet Connectivity False Positives (2025-12-02)**: Implemented multi-endpoint connectivity check with 3 retries across Google, Cloudflare, and Cloudflare DNS.
  - Files: `src/index.ts`

- **RPC Timeout for Approval Cache (2025-12-02)**: Added 30s timeout, 3 retries with exponential backoff, and PublicNode fallback provider.
  - Files: `src/utils/approval-cache.ts`

- **Token-vs-* Defense Running After Task Stop**: Added `currentTask.running` check at start of each defense block.
  - Files: `src/index.ts`

- **Token-vs-Trait Defense for Token/Rarity Tasks**: Fixed early return preventing token/rarity tasks from defending against trait offers.
  - Files: `src/index.ts`

- **MagicEden Token-vs-Collection Defense**: Added dedicated defense block to iterate through ALL `task.tokenIds` for token/rarity tasks.
  - Files: `src/index.ts`

- **Tasks Not Blacklisted on stopTask**: Fixed critical bug where `isTaskBlacklisted()` was called instead of `cancelTaskJobs()`.
  - Files: `src/index.ts`

- **Floor Price API Crash**: Fixed `Cannot set properties of undefined` by initializing `floorPrices[taskId]` before async fetches.
  - Files: `src/index.ts`

- **Bids Placed After stopTask**: Added blacklist checks at `processBulkJobs` entry, chunk loop, and all scheduled bid processors.
  - Files: `src/index.ts`

- **"Order already exists" Error**: Now syncs local state with OpenSea when duplicate offers detected instead of throwing.
  - Files: `src/marketplace/opensea/bid.ts`, `src/index.ts`

- **Bulk Cancel Passing Private Key Instead of WalletId**: Fixed incorrect parameter in `fetchAndCancelOpenseaBids()`.
  - Files: `src/index.ts`

- **Token Bids Not Cancelling on StopTask**: Added blacklist re-check inside `postItemOffer()` limiter callback.
  - Files: `src/marketplace/opensea/bid.ts`

- **Cancellation BidStats Zeroing Bug**: Removed premature `delete bidStats[taskId]` and added `decrementBidCount()` to bulk cancel functions.
  - Files: `src/index.ts`, `src/utils/order-cache.ts`

- **Duplicate Token Bids Race Condition**: Coordinated scheduled bids and Token-vs-Collection defense through `pendingCounterbids` cache.
  - Files: `src/index.ts`

- **Two-Tier Top Offer Caching**: Scheduled bid processors now cache API results; counterbid handlers fetch from API on cache miss.
  - Files: `src/index.ts`

- **Cross-Type Counterbid Price Calculation**: Now uses `max(incomingOffer, cachedTopOffer) + margin` for all cross-type counterbids.
  - Files: `src/index.ts`

- **Floor Price API Retry**: Added 3 retries with exponential backoff; 0 values no longer cached.
  - Files: `src/functions/collection.ts`

- **Multiple Bids on Same Token**: Added expiry check to `orderCache.get()`; scheduled bid processors now await cancellation before placing new bids.
  - Files: `src/utils/order-cache.ts`, `src/index.ts`

- **Counterbid Floating Point Precision**: Applied `roundToValidPrecision()` to final counterbid prices.
  - Files: `src/index.ts`

- **Spread Comparison Floating-Point Precision**: Applied `roundToValidPrecision()` to spread before comparison.
  - Files: `src/index.ts`

- **Spread Logic Bugs**: Fixed empty else-if fall-through in MagicEden and missing minBidPriceEth check in Blur.
  - Files: `src/index.ts`

- **OrderCache Desync During Bulk Cancel**: Added cache updates after all MongoDB Order deletions.
  - Files: `src/index.ts`, `src/marketplace/magiceden/bid.ts`

- **Counterbid Handler Bug Fixes (16 issues)**: Fixed OrderCache expiry filtering, silent error catch blocks, pending counterbids cleanup, missing awaits on cancellation jobs.
  - Files: `src/index.ts`

- **Task Loop Stuck at 100%**: Extended recovery logic to handle "Missing key for job" errors.
  - Files: `src/index.ts`

- **Orders Not Cancelled When Cache Empty**: Added MongoDB fallback when OrderCache returns 0 orders.
  - Files: `src/index.ts`

- **Bid Price Changes Not Taking Effect**: Fixed `_id` type inconsistency in `updateTaskData()`.
  - Files: `src/index.ts`

- **Active Bid Count Shows 0 After Restart**: Call `validateBidCount()` immediately after `fetchCurrentTasks()`.
  - Files: `src/index.ts`

- **OpenSea Offer Precision Compliance**: Applied `roundToValidPrecision()` to all bid calculations for OpenSea's January 28, 2025 requirements.
  - Files: `src/index.ts`

- **Duplicate Token Counterbid Spamming**: Added token-level deduplication using `pendingCounterbids` BoundedCache.
  - Files: `src/index.ts`

- **Duplicate Bid Spamming**: Wrapped `relevantTasks.map()` with `await Promise.all()`; added atomic `addIfNotExists()` deduplication.
  - Files: `src/index.ts`, `src/utils/bounded-cache.ts`

- **Fast Balance Drop Detection**: Parse OpenSea balance error messages and stop affected marketplaces immediately.
  - Files: `src/utils/balance.ts`, `src/index.ts`

- **Counterbid Wasted Gas**: Implemented `ourTopBids` store to only place counterbids when we'll become top bidder.
  - Files: `src/index.ts`

- **Counterbid Race Condition**: All counterbid functions now cancel orders synchronously before placing new bids.
  - Files: `src/index.ts`

- **Wallet Not Found Error Cascade**: Graceful handling in bulk cancel functions; pre-flight wallet validation in `startTaskInternal`.
  - Files: `src/index.ts`

- **MongoDB Transaction Error**: Removed unnecessary transaction code from `updateTaskData()`.
  - Files: `src/index.ts`

- **Docker Log Flooding**: Added early exit on balance stop and log throttling for balance errors.
  - Files: `src/index.ts`

- **HTTP 429 Retry Storms**: Stopped retrying rate limit errors in axios-retry.
  - Files: `src/init.ts`

### Added
- **Marketplace Top Offer Cache**: Implemented `marketplaceTopOfferCache` to track actual marketplace top offers.
  - Files: `src/index.ts`

- **Token Counterbid for Collection/Trait Offers**: Token bid tasks now defend against incoming collection and trait offers on OpenSea.
  - Files: `src/index.ts`

- **Log Filtering and Export System**: Added `/api/debug-logs/export` endpoint with metadata filtering support.
  - Files: `src/utils/log-export-handler.ts`, `src/utils/debug-file-logger.ts`, `src/index.ts`

- **In-Memory Order Cache**: Implemented high-performance OrderCache for O(1) order lookups.
  - Files: `src/utils/order-cache.ts`, `src/marketplace/*/bid.ts`, `src/index.ts`

- **WebSocket Connection Reliability**: Added bid activity watchdog, enhanced disconnection logging, server status health check.
  - Files: `src/utils/websocket-manager.ts`

- **Smart Subscription Cleanup**: WebSocket unsubscription uses reference counting for shared collections.
  - Files: `src/index.ts`

- **Unified Debug File Logger**: Centralized file-based logging with 22 category-specific log files.
  - Files: `src/utils/debug-file-logger.ts`

- **StopTask Debug Logging**: Added double blacklist check in limiter callbacks; comprehensive stopTask flow logging.
  - Files: `src/index.ts`, `src/marketplace/*/bid.ts`

- **Floor Price Pre-Fetching**: Automatic floor price cache warming when tasks start.
  - Files: `src/index.ts`

- **Comprehensive BidFlow Debug Logging**: Added `[BidFlow]` prefixed logs to all 19 bidding job processors.
  - Files: `src/index.ts`

- **Auto-Stop Tasks on Server Restart**: All running tasks/listings automatically stopped on startup.
  - Files: `src/index.ts`

- **Rarity Bid Type Support**: Added "rarity" to bidType enum with rarityCategory field.
  - Files: `src/models/task.model.ts`, `src/index.ts`

- **Centralized Rarity Token Fetching**: Server-side `/api/rarity` endpoint with rate limiting and two-tier caching.
  - Files: `src/models/rarityCache.model.ts`, `src/functions/rarity.ts`, `src/index.ts`

- **Bulletproof Task Update System**: WebSocket message idempotency tracking with acknowledgment protocol.
  - Files: `src/index.ts`

- **Password-Gated Startup System**: Server waits for password before starting operations.
  - Files: `src/index.ts`, `src/functions/listingScheduler.ts`, `src/utils/crypto.ts`

### Changed
- **Unified Debug Logging**: Consolidated 6 debug variables into single `DEBUG=true`.
  - Files: `src/index.ts`, `src/utils/balance.ts`, `src/init.ts`

- **Consolidated ourTopBids into OrderCache**: Eliminated redundant in-memory bid tracking.
  - Files: `src/index.ts`, `src/utils/order-cache.ts`

- **Order Lookups Migrated to Cache**: Replaced ~35 MongoDB queries with OrderCache lookups.
  - Files: `src/index.ts`

- **WETH Balance via OpenSea GraphQL**: Replaced slow Alchemy RPC with faster OpenSea GraphQL API.
  - Files: `src/utils/balance.ts`

- **Balance API Priority**: Balance fetch requests now use priority 0 (highest) in Bottleneck limiter.
  - Files: `src/utils/balance.ts`

- **Schedule Jobs Infinite Retry**: Updated all *_SCHEDULE jobs to retry infinitely with `removeOnFail: false`.
  - Files: `src/index.ts`

- **Granular Log File Separation**: Logs routed to 22 category-specific files based on prefix.
  - Files: `src/utils/debug-file-logger.ts`

### Removed
- **Better Stack Logger**: Removed `logger.ts`, `marketplace-file-logger.ts`, and associated types; replaced ~200+ calls with debug-file-logger.

- **Bid Trace Logger**: Removed `bid-trace-logger.ts` and `bid-trace.types.ts`; consolidated to `debugLog()`.

## [2025-11-20]

### Fixed
- **TypeScript Type Errors**: Fixed type mismatches after MagicEden API return type changes.
  - Files: `src/functions/collection.ts`, `src/index.ts`

- **MagicEden Collection Stats API**: Migrated to v4 API endpoint to resolve HTTP 400 errors.
  - Files: `src/marketplace/magiceden/bid.ts`

### Changed
- **Memory Optimization**: Reduced Node.js `max-old-space-size` from 4096MB to 2048MB.
  - Files: `nodemon.json`, `package-lock.json`
