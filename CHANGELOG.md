# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

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
