# Changelog

All notable changes to this project will be documented in this file.

## [2025-12-13]

### Fixed

- **tokenHasTrait Broken GraphQL Query**: Fixed critical bug where `fetchTokenMetadata()` used a non-existent GraphQL field (`contract(address:...)`) causing all token metadata fetches to fail.
  - **Problem**: The OpenSea GraphQL API doesn't have a `contract` field in the Query type, causing 100% failure rate for trait lookups
  - **Solution**: Updated to use `itemByIdentifier` query with proper `ItemIdentifierInput` variables
  - **Impact**: Token-vs-Trait defense and trait-based bidding now work correctly

### Added

- **MongoDB Bounded Cache for Token Traits**: Replaced Redis-based token trait caching with MongoDB for better persistence and reliability.
  - **New Model**: `src/models/tokenTraitCache.model.ts` with compound unique index on `contractAddress + tokenId`
  - **Bounded Cache**: Maximum 100,000 documents with LRU eviction (deletes oldest 10% when full)
  - **TTL Index**: Auto-expires documents after 7 days of no access via `accessedAt` field
  - **Two-Tier Caching**: In-memory LRU cache (1000 items, 1 hour TTL) + MongoDB persistent storage

- **Comprehensive tokenHasTrait Debug Logging**: Added detailed timing and flow logs to trace trait checking.
  - Logs cache hit/miss with timing (`cacheMs`)
  - Logs API fetch timing (`fetchMs`)
  - Logs trait match results with actual vs expected values
  - Logs all available trait types on mismatch for debugging

- **Cross-Type Counterbid Detection Logs**: Added debug logs to identify and trace cross-type counterbidding scenarios.
  - **TOKEN-vs-TRAIT**: When a token/rarity task receives a trait-level offer
  - **TOKEN-vs-COLLECTION**: When a token/rarity task receives a collection-level offer
  - **TRAIT-vs-COLLECTION**: When a trait task receives a collection-level offer

- **tokenHasTrait Test Script**: Added standalone test script `src/scripts/test-token-has-trait.ts` for validating trait checking.

- **New Log Prefix Mappings**: Added `StuckTaskCheck` and `TimerDiagnostic` prefixes to taskflow category.

---

## [Unreleased]

### Fixed

- **WebSocket Auto-Reconnection**: Fixed issue where bidding-bot-server would not automatically reconnect when ws-proxy restarts
  - Handle `server_shutdown` message to trigger immediate reconnection instead of just emitting event
  - Add TCP keepalive (10s interval) for faster dead connection detection
  - Reduce heartbeat timeout from 15s to 10s
  - Reduce missed pong threshold from 6 to 3 for faster detection (~30s vs ~2.5min)

### Changed

- **Scheduled Jobs Priority**: Scheduled jobs (OPENSEA_SCHEDULE, BLUR_SCHEDULE, MAGICEDEN_SCHEDULE) now have the highest priority in BullMQ by not assigning a priority value. Previously, MAGICEDEN_SCHEDULE had `priority: 1`.

- **API Rate Limiter Priority**: Added optional `priority` parameter to API helper functions used by scheduled jobs. Scheduled job handlers now pass `priority: 0` (highest) to all API calls, ensuring scheduled tasks are processed before other jobs in the Bottleneck rate limiter queue.

### Added

- `scheduleWithPriority()` function in `src/init.ts` for priority-aware rate limiting

### Files Modified

- `src/init.ts` - Added `scheduleWithPriority` helper function
- `src/index.ts` - Removed priority from MAGICEDEN_SCHEDULE, updated scheduled job handlers
- `src/functions/collection.ts` - Added priority parameter to collection helper functions
- `src/marketplace/opensea/bid.ts` - Added priority parameter to OpenSea API functions
- `src/marketplace/blur/bid.ts` - Added priority parameter to Blur API functions
- `src/marketplace/magiceden/bid.ts` - Added priority parameter to MagicEden API functions
