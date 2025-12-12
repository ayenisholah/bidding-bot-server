# Changelog

All notable changes to this project will be documented in this file.

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
