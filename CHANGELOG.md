# Changelog

All notable changes to the Bidding Bot Server will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
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
