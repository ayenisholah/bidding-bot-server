# Changelog

All notable changes to the Bidding Bot Server will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [3.3.0] - 2025-12-07

### Added
- **Update Server with Logging and Safeguards**: Enhanced update server (port 9999) with comprehensive logging and debugging capabilities.
  - **Wrapper Script Approach**: Updates now execute via a self-contained bash wrapper script that survives server restarts
  - **Full Logging**: All update output logged to `/var/log/nfttools-update.log`
  - **Status Tracking**: Update status persisted to `/tmp/update-status.json`
  - **Debug Endpoints**:
    - `GET /logs` - Returns last 200 lines of update logs
    - `GET /status` - Returns last update result (success/failed/running)
    - `GET /health` - Health check with uptime
  - **Lock File Protection**: `/tmp/update-in-progress.lock` prevents concurrent updates (10-minute timeout)
  - **Installation Logging**: All install.sh output redirected to `/var/log/nfttools-install.log`
  - Files: `install.sh`

## [3.1.0] - 2025-12-07

### Changed
- **Removed File-Based Password Persistence (2025-12-07)**: Production now requires password entry on every server start.
  - **Previous Behavior**: Password was encrypted and stored in `/app/data/.encrypted-password` for auto-recovery after crashes
  - **New Behavior**:
    - **Production**: Password only stored in memory, no persistence. Requires re-entry on every restart/crash
    - **Development**: Password persists in Redis (7-day TTL) for convenience during development
  - **Removed**:
    - `savePasswordToFile()`, `loadPasswordFromFile()`, `deletePasswordFile()`, `recoverPasswordFromFile()`, `recoverPassword()` functions
    - `PASSWORD_FILE_PATH` constant
    - `ENCRYPTION_KEY` environment variable from compose files
    - `server_data` Docker volume (no longer needed)
  - **Kept for Development**:
    - `recoverPasswordFromRedis()` - recovers password from Redis in dev mode
    - `renewPasswordTTL()` - extends Redis password TTL
  - **Security Note**: `encryptPassword()`/`decryptPassword()` functions retained for client-server password transmission (AES-256-ECB)
  - Files: `src/utils/crypto.ts`, `src/index.ts`, `install.sh`, `compose.yaml`

### Added
- **Stuck Job Cleanup System (2025-12-07)**: Added automatic cleanup of jobs stuck in "delayed" or "waiting" BullMQ states.
  - **Problem**: Jobs could get stuck indefinitely in delayed/waiting states (observed 19+ delayed jobs) with no recovery mechanism
  - **Root Cause**: `job.moveToDelayed()` in Blur rate limiting could leave jobs stuck forever if rate limiter never cleared
  - **Solution**: New `cleanupStuckJobs()` function runs every 60 seconds and processes stuck jobs:
    - **SCHEDULE jobs** (`OPENSEA_SCHEDULE`, `MAGICEDEN_SCHEDULE`, `BLUR_SCHEDULE`): Retry infinitely with exponential backoff
    - **Other jobs**: Retry up to 3 times, then trigger full task restart
    - Jobs stuck for >60 seconds are considered stuck and processed
  - **Debug Logging**: Comprehensive logging under `StuckJobCleanup` prefix:
    - `SCAN`: Summary of delayed/waiting job counts per queue
    - `FOUND`: Details of each stuck job discovered
    - `PROCESSING`: Action taken on stuck jobs
    - `RETRY`: When job is moved to failed for retry
    - `TASK_RESTART`: When task restart is triggered after exhausted retries
    - `CLEANUP COMPLETE`: Summary of cleanup run
  - **Schedule Job Options Updated**: All schedule jobs now use `attempts: Number.MAX_SAFE_INTEGER` and `removeOnFail: false` for infinite retries
  - Files: `src/index.ts`, `src/utils/debug-file-logger.ts`

### Fixed
- **debug-container.sh Queue Names (2025-12-07)**: Fixed incorrect queue names in debug script.
  - **Problem**: Script checked for `main-queue`/`blur-queue` but actual queue names are `BIDDING_BOT`/`BLUR_BIDDING_QUEUE`
  - **Solution**: Updated all Redis key references to use correct queue names
  - **Affected Sections**: Queue sizes, stalled jobs, failed jobs, active jobs
  - Files: `debug-container.sh`

### Fixed
- **Tasks Stuck in SCHEDULED with waitingTime=0 (2025-12-06)**: Fixed critical bug where tasks would get stuck in SCHEDULED status with waitingTime=0 after server restart and never execute their next loop.
  - **Root Cause 1**: Orphaned task detection checked `taskTimeouts.has(taskId)` but timers are stored with keys like `${taskId}-${marketplace}`
  - **Root Cause 2**: `checkStuckTasks()` only checked for RUNNING/QUEUED/QUEUEING statuses, ignoring SCHEDULED tasks
  - **Root Cause 3**: No recovery mechanism when timer is lost (e.g., after server restart) and task stays in SCHEDULED with waitingTime=0
  - **Fixes Implemented**:
    - **Fix 1**: Updated orphaned task detection to check marketplace-specific timer keys
    - **Fix 2**: Added SCHEDULED status checking to `checkStuckTasks()` with 30-second recovery window
    - **Fix 3**: Added periodic timer health check function `verifyScheduledTaskTimers()` (runs every 60 seconds)
    - **Fix 4**: Added Redis persistence for task state (taskStatus, loopCounterStats, nextLoopTime)
    - **Fix 5**: Added state restoration on startup for running tasks from Redis
  - **New Redis Keys** (24h TTL):
    - `nft-bot:task-status:{taskId}` - Persisted task status
    - `nft-bot:loop-count:{taskId}` - Loop counter per marketplace
    - `nft-bot:next-loop:{taskId}:{marketplace}` - Next loop timestamp for timer recreation
    - `nft-bot:task-started:{taskId}` - Task start timestamp
    - `nft-bot:last-schedule:{taskId}` - Last schedule timestamp
  - Files: `src/index.ts`

### Added
- **StopOption Debug Logging (2025-12-06)**: Added comprehensive debug logs to trace stopOption evaluation flow for debugging price limit issues.
  - **Log Phases**:
    - `ENTRY`: All input parameters (task, marketplace, floor, traitFloor, trait)
    - `DISABLED`: When triggerStopOptions is false
    - `CONFIG`: Parsed configuration (minFloor, maxFloor, traitKey, hasMarketplaceKey, hasOldKey, traitPriceKeys)
    - `TRAIT_CHECK`: Trait floor evaluation with belowMin/aboveMax/outOfRange
    - `FLOOR_CHECK`: Collection floor evaluation with belowMin/aboveMax/outOfRange
    - `MAX_PURCHASE_CHECK`: Balance vs limit comparison
    - `TRAIT_TRIGGERED` / `FLOOR_TRIGGERED` / `MAX_PURCHASE_TRIGGERED`: When checks stop bidding
    - `EXIT`: Final result (all checks passed)
  - **Call Site Logging**:
    - `PRE_CALL`: Before each stopOption call with context (counterbid vs scheduled, collection vs trait vs token)
    - `TRAIT_FLOOR_DECISION`: Before trait floor fetch, showing key format being checked
  - **Metadata**: All logs include LogMetadata for filtering by taskId/marketplace/slug/traitKey
  - Files: `src/index.ts`

### Fixed
- **stopOptions.traitPrices Not Working (2025-12-06)**: Fixed critical bug where trait-specific price limits in stopOptions were completely ignored.
  - **Problem**: Client stored traitPrices with 3-part keys (`opensea:HEADWEAR:HAT`) but server checked for 2-part keys (`HEADWEAR:HAT`), causing the `in` check to always fail
  - **Impact**: Trait floor prices were never fetched, so trait-specific stop conditions were bypassed entirely - falling back to collection floor price checks only
  - **Solution**: Updated all 6 locations that check traitPrices to use the correct 3-part key format with marketplace prefix
  - **Affected Functions**:
    - `handleMagicEdenCounterbid` (line 12446)
    - `handleOpenseaCounterbid` (line 13499)
    - `handleBlurCounterbid` (line 14374)
    - `processOpenseaTraitBid` (line 17098)
    - `processBlurTraitBid` (line 18851)
    - `processMagicedenTraitBid` (line 20348)
  - Files: `src/index.ts`

### Added
- **Enhanced Export with HTTP Download (2025-12-05)**: The `export` command now includes crash analysis and optional HTTP serving.
  - Added `analyze_crash` to `show_all()` - exports now include human-readable exit code analysis
  - Added `serve` option to export command for HTTP download
  - **Usage**:
    ```bash
    # Export to file (now includes crash analysis)
    ./debug-container.sh export

    # Export and serve via HTTP for download
    ./debug-container.sh export serve

    # Export and serve on custom port
    ./debug-container.sh export serve 9000
    ```
  - Files: `debug-container.sh`

- **Snapshot Download via HTTP (2025-12-05)**: Added `download` command to easily download all snapshots from VPS.
  - Creates zip archive of all snapshots in `./debug-snapshots/`
  - Starts temporary HTTP server for easy browser/curl download
  - Auto-stops after timeout (default 5 minutes) and cleans up zip file
  - **Usage**:
    ```bash
    # On VPS:
    ./debug-container.sh download

    # On local machine:
    curl -O http://YOUR_VPS_IP:8888/snapshots_20251205_143022.zip
    ```
  - Files: `debug-container.sh`

- **Automated Crash Diagnostics Capture (2025-12-05)**: Debug script now automatically captures diagnostic snapshots at hourly intervals and on crash detection.
  - **Problem**: Manual execution of `./debug-container.sh all` was required after crashes, and volatile data (memory, queue state, connections) was lost after container restart
  - **Solution**:
    - Periodic snapshots: Takes full diagnostic snapshot every hour (configurable)
    - Crash detection: Watches Docker events and captures snapshot immediately on container die/OOM
    - Systemd services: Auto-starts on VPS boot, restarts on failure
  - **New Commands**:
    - `snapshot [trigger]` - Take immediate diagnostic snapshot
    - `monitor [interval]` - Start periodic snapshot daemon (default: 3600s = 1hr)
    - `crashes` - Watch for crashes and auto-capture snapshots
    - `snapshots` - List saved snapshots with monitor status
    - `stop-monitor` - Stop monitoring daemons
  - **Snapshot Files**:
    - Periodic: `./debug-snapshots/snapshot_YYYYMMDD_HHMMSS.txt`
    - Crash: `./debug-snapshots/crash_YYYYMMDD_HHMMSS_exitN.txt` (includes exit code)
  - **Auto-Start**: Systemd services enabled on installation:
    - `debug-monitor` - Hourly snapshots
    - `debug-crashwatch` - Crash detection and capture
  - **Retention**: Automatically keeps last 168 snapshots (~1 week at hourly intervals, ~15MB storage)
  - **Usage**:
    ```bash
    # Check service status
    sudo systemctl status debug-monitor
    sudo systemctl status debug-crashwatch

    # View saved snapshots
    ./debug-container.sh snapshots

    # View specific crash snapshot
    cat ./debug-snapshots/crash_20251205_143022_exit137.txt
    ```
  - Files: `debug-container.sh`, `install.sh`

### Changed
- **Secure Password Encryption with Unique Per-Installation Key (2025-12-05)**: Password file encryption now uses a unique `ENCRYPTION_KEY` instead of hardcoded `JWT_SECRET`.
  - **Problem**: Password file was encrypted with `JWT_SECRET` which is hardcoded in `compose.yaml`, making it trivially decryptable by anyone with VPS access
  - **Solution**:
    - `install.sh` now generates a unique 256-bit `ENCRYPTION_KEY` per installation stored in `.env` with 600 permissions
    - `crypto.ts` uses `ENCRYPTION_KEY` for password file encryption with additional key derivation
    - Falls back to `JWT_SECRET` for backwards compatibility during migration
  - **Security Improvement**: Password can no longer be decrypted via `docker inspect` or reading `compose.yaml`
  - **Migration Note**: Existing users will need to re-enter password once after update (old encrypted file uses different key)
  - Files: `install.sh`, `src/utils/crypto.ts`

### Fixed
- **Auto-Recovery Password Persistence Across Updates (2025-12-05)**: Fixed critical bug where encrypted password file was deleted on reinstall/update, breaking auto-recovery.
  - **Problem**: `install.sh` removed `server_data` volume on each update, deleting the encrypted password file at `/app/data/.encrypted-password`
  - **Solution**: Updated `install.sh` to preserve `server_data` volume alongside `mongodb_data`
  - **Impact**: Auto-recovery now works correctly across server updates/reinstalls - tasks will automatically resume after crashes even after running `install.sh`
  - Files: `install.sh`

### Added
- **Comprehensive Debug Container Script (2025-12-05)**: Enhanced `debug-container.sh` with 6 new diagnostic commands to fully diagnose all crash vectors.
  - **New Commands**:
    - `redis` - Redis connection health, memory stats, client connections, queue sizes, slowlog
    - `mongodb` - MongoDB connection pool stats, long-running operations, database size
    - `health` - Application health endpoints (/health, /api/health/redis, /metrics)
    - `memory` - Node.js heap usage, container memory limits, memory trends from logs
    - `network` - DNS resolution tests, external API connectivity, WebSocket status, network errors
    - `queue` - BullMQ queue counts, stalled jobs detection, active job ages (stuck job detection), worker errors
  - **Diagnostic Coverage**:
    - Redis down/connection exhaustion
    - MongoDB pool exhaustion/slow queries
    - Memory leaks/OOM kills
    - Health check timeouts
    - DNS failures
    - WebSocket disconnects
    - Queue backlog/stalled workers
  - **Usage**: `./debug-container.sh all` runs all diagnostics, or use individual commands
  - Files: `debug-container.sh`

- **Debug Script with Auto-Export (2025-12-05)**: The `debug-container.sh` script is now automatically downloaded and executed during installation.
  - Script available at `./debug-container.sh` in the VPS project directory
  - Auto-generates `debug-output.txt` with full container diagnostics after installation
  - Manual run: `./debug-container.sh [command]` from VPS project root
  - Re-export anytime: `./debug-container.sh all > debug-output.txt`
  - Commands: status, logs, events, inspect, resources, lifecycle, crash, all, watch, export
  - Files: `install.sh`

- **Crash Detection and Automatic Task Recovery (2025-12-04)**: Server now detects crashes and automatically resumes previously running tasks after restart.
  - **Problem**: When server crashed/restarted unexpectedly, all tasks were stopped and required manual restart via dashboard
  - **Solution**:
    - Added crash detection using Redis flags to distinguish graceful shutdown vs crash
    - On graceful shutdown (SIGINT/SIGTERM), saves list of running tasks/listings to Redis
    - On startup, detects if previous shutdown was a crash and auto-resumes previously running tasks
    - Password is now persisted to encrypted file on Docker volume (production) for automatic recovery
  - **Password Persistence**:
    - Production: Encrypted password stored in `/app/data/.encrypted-password` (AES-256-ECB with JWT_SECRET)
    - Development: Continues using Redis (7-day TTL)
  - **Auto-Resume Flow**:
    1. Server starts → detects previous shutdown type (graceful/crash/first-start)
    2. Attempts to recover password from encrypted file (production) or Redis (development)
    3. If crash detected AND password recovered → auto-resumes all previously running tasks
    4. If graceful shutdown → normal startup, no auto-resume
  - Files: `src/index.ts`, `src/utils/crypto.ts`

- **Process Lifecycle Logger (2025-12-04)**: Added dedicated logging for process lifecycle events to diagnose unexpected restarts.
  - **Purpose**: Track process start/stop/crash events that occur outside normal application logging
  - **Features**:
    - Logs process start with PID, Node version, memory info
    - Logs all signals received (SIGINT, SIGTERM, SIGHUP, SIGUSR1/2)
    - Logs uncaught exceptions and unhandled rejections with full stack traces
    - Periodic memory usage logging (every 5 minutes)
    - Logs process exit with exit code and reason
  - **Log Location**: `/app/debug-logs/process-lifecycle-YYYY-MM-DD.log`
  - **Docker Volume**: Logs now persisted via `server_logs:/app/debug-logs` volume mount
  - Files: `src/utils/process-lifecycle-logger.ts`

### Fixed
- **Insufficient Balance Error Log Flooding (2025-12-04)**: Fixed critical issue where insufficient balance errors were flooding logs with 37,000+ identical errors.
  - **Problem**: When multiple parallel bid jobs hit the same balance check, each one logged an error, causing massive log flooding (20+ errors within milliseconds for the same wallet/collection)
  - **Solution**: Added `BoundedCache`-based deduplication to only log insufficient balance errors once per wallet/collection combo within a 5-second window
  - **Impact**: Reduces API log volume by ~99% during insufficient balance conditions
  - Files: `src/marketplace/opensea/bid.ts`, `src/marketplace/magiceden/bid.ts`

- **StopOption Log Verbosity (2025-12-04)**: Removed excessive "stopOptions not enabled" debug logs that were flooding logs (54+ identical entries per loop).
  - **Problem**: Every token check logged `[StopOption] {collection} | stopOptions not enabled | RESULT=false` even when the feature was disabled
  - **Solution**: Removed the log statement for disabled stopOptions - only log when stopOptions is enabled or triggers
  - **Impact**: Reduces bidflow log volume significantly for tasks without stopOptions enabled
  - Files: `src/index.ts`

- **Network Error Status Parsing (2025-12-04)**: Improved error logging for floor price fetch failures to show actual error codes.
  - **Problem**: Network errors (DNS failures, timeouts) showed `status=undefined` which was unhelpful for debugging
  - **Solution**: Parse `error.code` (e.g., `EAI_AGAIN`, `ENOTFOUND`, `ETIMEDOUT`) when `error.response.status` is unavailable
  - **Expected Log Output**: Now shows `error=EAI_AGAIN` instead of `status=undefined` for DNS failures
  - Files: `src/functions/collection.ts`

### Added
- **fetchTokenMetadata Debug Logging (2025-12-04)**: Added comprehensive debug logs to `fetchTokenMetadata()` to investigate 495+ metadata fetch failures.
  - **Purpose**: Diagnose WHY metadata fetches fail - previously failures were silent with no visibility into root cause
  - **Debug Logs Added** (`src/functions/tokenMetadata.ts`):
    - ENTRY: Logs when cache miss occurs and API fetch begins
    - NULL NFT: Logs when API returns success but no NFT data (with response structure keys)
    - HTTP 400: Logs 400 errors with error message from API (previously completely silent)
    - HTTP errors: Logs other HTTP status codes with error message
    - Network errors: Logs network/timeout errors with error code
  - **Expected Log Output** (HTTP 400):
    ```
    [Metadata] fetchTokenMetadata ENTRY | contract=0x1234abcd... | tokenId=3105
    [Metadata] fetchTokenMetadata: HTTP 400 | contract=0x1234abcd... | tokenId=3105 | errorMsg="Token not found"
    [Metadata] tokenHasTrait: Metadata fetch FAILED | tokenId=3105 | returning false
    ```
  - **Expected Log Output** (NULL response):
    ```
    [Metadata] fetchTokenMetadata ENTRY | contract=0x1234abcd... | tokenId=3105
    [Metadata] fetchTokenMetadata: API returned NULL NFT | contract=0x1234abcd... | tokenId=3105 | responseKeys=[data] | dataKeys=[contract] | contractKeys=[address,name]
    ```
  - Files: `src/functions/tokenMetadata.ts`
  - Resolves: Missing visibility into why `fetchTokenMetadata()` returns null (was causing 495 silent failures)

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
