# Changelog

All notable changes to the Bidding Bot Server will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
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
