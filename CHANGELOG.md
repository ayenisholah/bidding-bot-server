# Changelog

All notable changes to the Bidding Bot Server will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
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

### Fixed
- **OpenSea Listing Fee Validation Error**: Fixed OpenSea marketplace listing failures caused by incorrect fee configuration
  - Updated hardcoded OpenSea fee from 50 basis points (0.5%) to 100 basis points (1%)
  - Integrated marketplace fee cache system (`getOpenseaMarketplaceFee()`) for dynamic fee management
  - Fixed hardcoded `BigInt(50)` values in `startAmount` and `endAmount` calculations
  - Files modified: `src/marketplace/opensea/list.ts`
  - Resolves error: "Validation error: An incorrect OpenSea fee has been provided. Expecting 100 basis points, but received 50."

- **API Key Validation and Debugging**: Enhanced API key configuration validation and debugging capabilities
  - Added startup validation to ensure `API_KEY` environment variable is properly loaded
  - Added startup logging showing first 8 characters of loaded API key for verification
  - Added debug logging before OpenSea API calls to track API key presence and request details
  - Removed redundant `dotenv.config()` calls from `collection.ts` to prevent configuration conflicts
  - Files modified: `src/index.ts`, `src/functions/collection.ts`
  - Helps diagnose HTTP 401 Unauthorized errors related to missing or invalid API keys

- **OpenSea Rate Limiting (HTTP 429) Errors**: Fixed excessive concurrent API calls causing rate limit violations
  - Root cause: Bottleneck had no maxConcurrent limit, allowing unlimited simultaneous API calls
  - Root cause: pQueue concurrency set to 200 (RATE_LIMIT * 2) created burst traffic
  - Added `maxConcurrent: RATE_LIMIT` (100) to Bottleneck configuration in `src/init.ts`
  - Added reservoir token bucket: 100 tokens refreshing every 1 second for burst protection
  - Reduced pQueue concurrency from 200 to 100 (RATE_LIMIT) in `src/index.ts`
  - Files modified: `src/init.ts:93-100`, `src/index.ts:13210-13212`
  - Resolves: "Rate limit exceeded" HTTP 429 errors from OpenSea API during bidding operations
  - Impact: Ensures all concurrent execution respects RATE_LIMIT environment variable

- **Cascading Concurrency Multiplication**: Fixed worker concurrency calculation causing 200-400+ concurrent API calls
  - Root cause: WORKER_COUNT hardcoded to 1 but 2 workers actually run (Main + Blur)
  - Root cause: Each worker had concurrency=100, totaling 200 concurrent jobs
  - Root cause: Each job spawns 2+ parallel API calls via pQueue.addAll(), multiplying actual API calls
  - Result: 2 workers × 100 concurrency × 2 parallel tasks = 400 concurrent API calls (4× RATE_LIMIT)
  - Updated WORKER_COUNT from 1 to 2 to accurately reflect Main Worker + Blur Worker
  - Added PARALLEL_TASKS_PER_JOB constant (2) to account for parallel API calls within each job
  - Updated concurrency formula: `RATE_LIMIT / WORKER_COUNT / PARALLEL_TASKS_PER_JOB = 100 / 2 / 2 = 25`
  - New calculation: 2 workers × 25 concurrency × 2 parallel tasks = 100 concurrent API calls ✓
  - Files modified: `src/index.ts:223`, `src/index.ts:2261-2265`
  - Resolves: Persistent HTTP 429 "Rate limit exceeded" errors despite individual component configuration
  - Impact: Total concurrent API calls now accurately match RATE_LIMIT environment variable

### Changed
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
