# Index.ts Refactoring Plan - Progress Tracker

**Goal**: Reduce index.ts from 28,827 lines to ~5,000 lines
**Current Status**: Planning Phase
**Started**: 2026-01-24

---

## Overview

| Phase | Services | Lines to Extract | Status |
|-------|----------|------------------|--------|
| Phase 1 | 4 services | ~2,855 lines | 🔴 Not Started |
| Phase 2 | 3 services | ~5,835 lines | 🔴 Not Started |
| Phase 3 | 3 services | ~14,000 lines | 🔴 Not Started |
| Console.log | 34 instances | N/A | 🔴 Not Started |

**Legend**: 🔴 Not Started | 🟡 In Progress | 🟢 Complete

---

## Phase 1: Low-Risk, High-Impact Extractions

**Priority**: CRITICAL
**Timeline**: Week 1-2
**Status**: 🔴 Not Started

### 1.1 REST API Routes Service (~2,000 lines)
- [ ] Create `src/services/api-routes.service.ts`
- [ ] Extract Health & Diagnostics endpoints (5 endpoints)
  - [ ] `GET /health`
  - [ ] `GET /metrics`
  - [ ] `GET /health/redis`
  - [ ] `GET /health/workers`
  - [ ] `GET /health/system`
- [ ] Extract Task Management endpoints (4 endpoints)
  - [ ] `GET /api/task/:taskId`
  - [ ] `GET /api/task/:taskId/status-history`
  - [ ] `POST /api/task/cancel/:taskId`
  - [ ] `POST /api/task/:taskId/reset-purchases`
- [ ] Extract Collection & Marketplace endpoints (6 endpoints)
  - [ ] `POST /api/floor`
  - [ ] `POST /api/collection`
  - [ ] `POST /api/collection/stats`
  - [ ] `POST /api/traits/count`
  - [ ] `POST /api/rarity`
  - [ ] `POST /api/currency/conversion`
- [ ] Extract Authentication endpoints (5 endpoints)
  - [ ] `POST /api/auth/password`
  - [ ] `GET /api/auth/password-status`
  - [ ] `POST /api/auth/renew-password`
  - [ ] `POST /api/auth/change-password`
  - [ ] `POST /api/auth/recover-password`
- [ ] Extract Blur-specific endpoints (9 endpoints)
  - [ ] `GET /api/blur-rate-limits`
  - [ ] `GET /api/blur-rate-limits/:wallet`
  - [ ] `POST /api/blur-rate-limits/:wallet/reset`
  - [ ] `POST /api/blur-auth/clear`
  - [ ] `GET /api/blur-health`
  - [ ] `GET /api/blur-timeout-metrics`
  - [ ] `GET /api/blur-rate-limiter/status`
  - [ ] `GET /api/blur-completion/status`
  - [ ] `GET /circuit-breaker/status`
- [ ] Extract Memory & Monitoring endpoints (7 endpoints)
  - [ ] `GET /api/memory-stats`
  - [ ] `GET /api/memory-snapshots/latest`
  - [ ] `GET /api/memory-snapshots`
  - [ ] `POST /api/memory-snapshots/trigger`
  - [ ] `GET /api/queue-monitor/status`
  - [ ] `GET /api/queue-health`
  - [ ] `POST /api/update-app`
- [ ] Extract Listing & NFT endpoints (3 endpoints)
  - [ ] `GET /api/wallet/nfts`
  - [ ] `GET /api/collection/:contractAddress/:tokenId/:slug/:walletAddress`
- [ ] Extract Debug endpoints (3 endpoints)
  - [ ] `POST /api/debug/task-status/:taskId`
  - [ ] `POST /api/debug/task-diagnostics/:taskId`
- [ ] Write unit tests
- [ ] Integration test all endpoints
- [ ] Update index.ts to use service
- [ ] Deploy to staging
- [ ] Monitor for 1 week
- [ ] Deploy to production

**Extracted Lines**: 0 / 2,000

---

### 1.2 WebSocket Service (~220 lines)
- [ ] Create `src/services/websocket.service.ts`
- [ ] Extract connection handler (`wss.on('connection')`)
- [ ] Extract message handler (`ws.onmessage`)
- [ ] Extract endpoint routing
  - [ ] `updated-task`
  - [ ] `new-task`
  - [ ] `import-tasks`
  - [ ] `toggle-status`
  - [ ] `stop-task`
  - [ ] `delete-task`
  - [ ] `update-multiple-tasks-status`
  - [ ] `globalMarketplaces`
  - [ ] `update-marketplace`
  - [ ] `create-listing`
  - [ ] `stop-listing`
  - [ ] `update-listing`
  - [ ] `subscribe`
  - [ ] `unsubscribe`
  - [ ] `catchup`
- [ ] Extract message idempotency logic
  - [ ] `isDuplicateMessage()`
  - [ ] `markMessageProcessed()`
- [ ] Replace console.log at line 6370 with errorLog()
- [ ] Write unit tests
- [ ] Integration test WebSocket flow
- [ ] Update index.ts to use service
- [ ] Deploy to staging
- [ ] Monitor for 1 week
- [ ] Deploy to production

**Extracted Lines**: 0 / 220
**Console.log Replaced**: 0 / 1

---

### 1.3 Counterbid Tracking Service (~435 lines)
- [ ] Create `src/services/counterbid-tracking.service.ts`
- [ ] Extract counterbid management functions
  - [ ] `markCounterbidPending()` (line 254)
  - [ ] `getPendingCounterbid()` (line 272)
  - [ ] `updatePendingCounterbidPrice()` (line 284)
  - [ ] `clearCounterbidPending()` (line 300)
- [ ] Extract top offer tracking functions
  - [ ] `getTopOfferCacheKey()` (line 340)
  - [ ] `getMarketplaceTopOffer()` (line 347)
  - [ ] `setMarketplaceTopOffer()` (line 355)
  - [ ] `updateTopOfferFromIncomingBid()` (line 394)
  - [ ] `clearTopOffersByTask()` (line 416)
- [ ] Extract counterbid decision logic
  - [ ] `checkShouldCounterbid()` (line 435)
- [ ] Extract deduplication tracking
  - [ ] `addSeenItem()` (line 472)
  - [ ] `hasSeenItem()` (line 480)
  - [ ] `markSeenIfNew()` (line 491)
- [ ] Write unit tests
- [ ] Integration test with bidding flow
- [ ] Update index.ts to use service
- [ ] Deploy to staging
- [ ] Monitor for 1 week
- [ ] Deploy to production

**Extracted Lines**: 0 / 435

---

### 1.4 Task Index Service (~200 lines)
- [ ] Create `src/services/task-index.service.ts`
- [ ] Extract task indexing functions
  - [ ] `updateTaskIndexes()` (line 1015)
  - [ ] `getRelevantTasks()` (line 1042)
  - [ ] `addTaskToIndexes()` (line 1062)
  - [ ] `removeTaskFromIndexes()` (line 1089)
- [ ] Add contract-to-task mapping logic
- [ ] Add slug-to-task mapping logic
- [ ] Write unit tests
- [ ] Integration test with task operations
- [ ] Update index.ts to use service
- [ ] Deploy to staging
- [ ] Monitor for 1 week
- [ ] Deploy to production

**Extracted Lines**: 0 / 200

---

### Console.log Replacement (34 instances)
- [ ] Replace startup logs (11 instances, lines 19, 164, 2304-2716)
  - [ ] Line 19: `console.log('[STARTUP] Environment configuration loaded')`
  - [ ] Line 164: `console.log('[STARTUP] API_KEY loaded successfully')`
  - [ ] Lines 2304-2716: Various startup messages
- [ ] Replace balance error logs (3 instances, lines 3919, 5793, 5812)
  - [ ] Line 3919: Insufficient balance logging
  - [ ] Line 5793: Balance error detection
  - [ ] Line 5812: Balance error detection
- [ ] Replace worker error logs (1 instance, line 6370)
  - [ ] Line 6370: WebSocket auto-subscribe failure
- [ ] Replace task update logs (scattered, lines 7491-7964)
  - [ ] Replace all task update console.log calls
- [ ] Verify all console.log/error/warn removed
- [ ] Test logging output
- [ ] Deploy to staging
- [ ] Monitor logs for 1 week
- [ ] Deploy to production

**Replaced**: 0 / 34

---

## Phase 2: Core Business Logic

**Priority**: HIGH
**Timeline**: Week 3-4
**Status**: 🔴 Not Started

### 2.1 Task Lifecycle Service (~3,000 lines)
- [ ] Create `src/services/task-lifecycle.service.ts`
- [ ] Extract task start functions
  - [ ] `startTask()` (line 9081)
  - [ ] `startTaskInternal()` (line 9151)
- [ ] Extract task stop functions
  - [ ] `stopTask()` (scattered)
- [ ] Extract task status functions
  - [ ] `updateStatus()` (scattered)
- [ ] Extract bulk operation functions
  - [ ] `startTasksInBatches()` (line 7052)
  - [ ] `stopTasksInBatches()` (line 7136)
- [ ] Extract state persistence functions
  - [ ] `persistTaskState()` (line 5173)
  - [ ] `restoreTaskState()` (line 5291)
  - [ ] `recreateTimersFromRedis()` (line 5336)
- [ ] Replace console.log instances (6 instances, lines 2506-2697)
- [ ] Write unit tests
- [ ] Integration test task lifecycle
- [ ] Update index.ts to use service
- [ ] Deploy to staging
- [ ] Monitor for 2 weeks
- [ ] Deploy to production

**Extracted Lines**: 0 / 3,000
**Console.log Replaced**: 0 / 6

---

### 2.2 Task Cancellation Service (~2,500 lines)
- [ ] Create `src/services/task-cancellation.service.ts`
- [ ] Extract cancellation tracking function
  - [ ] `updateTaskCancellationPercentage()` (line 10338)
- [ ] Extract job removal functions
  - [ ] `removeAllTaskJobsFromQueue()` (scattered)
- [ ] Extract marketplace-specific cancellation
  - [ ] `cancelOpenseaOrdersDirectly()` (scattered)
  - [ ] `cancelMagicedenOrdersDirectly()` (scattered)
  - [ ] `cancelBlurOrdersDirectly()` (scattered)
- [ ] Consider creating sub-handlers (optional)
  - [ ] `src/services/cancellation/opensea-cancellation.handler.ts`
  - [ ] `src/services/cancellation/blur-cancellation.handler.ts`
  - [ ] `src/services/cancellation/magiceden-cancellation.handler.ts`
- [ ] Write unit tests
- [ ] Integration test cancellation flow
- [ ] Update index.ts to use service
- [ ] Deploy to staging
- [ ] Monitor for 2 weeks
- [ ] Deploy to production

**Extracted Lines**: 0 / 2,500

---

### 2.3 Lock Management Service (~335 lines)
- [ ] Create `src/services/lock-management.service.ts`
- [ ] Extract task lock functions
  - [ ] `unlockTask()` (line 632)
- [ ] Extract loop scheduling lock functions
  - [ ] `acquireLoopSchedulingLock()` (line 640)
  - [ ] `releaseLoopSchedulingLock()` (line 659)
- [ ] Extract scheduled bid lock functions
  - [ ] `acquireScheduledBidLock()` (line 666)
  - [ ] `extendScheduledBidLock()` (line 691)
  - [ ] `releaseScheduledBidLock()` (line 711)
- [ ] Write unit tests
- [ ] Integration test lock coordination
- [ ] Update index.ts to use service
- [ ] Deploy to staging
- [ ] Monitor for 2 weeks
- [ ] Deploy to production

**Extracted Lines**: 0 / 335

---

## Phase 3: Marketplace Services (Optional)

**Priority**: MEDIUM
**Timeline**: Week 5-6
**Status**: 🔴 Not Started
**Risk**: HIGH - Only proceed if Phase 1-2 are stable

### 3.1 OpenSea Bidding Service (~5,000 lines)
- [ ] Create `src/services/marketplace/opensea-bidding.service.ts`
- [ ] Extract bid preprocessing
  - [ ] `preprocessOpenseaBid()` (scattered in lines 14241-20000)
- [ ] Extract scheduled bid processing
  - [ ] `processOpenseaScheduledBid()` (scattered)
- [ ] Extract token fetching logic
- [ ] Extract trait validation logic
- [ ] Extract floor price checks
- [ ] Extract stop option logic
- [ ] Write unit tests
- [ ] Integration test OpenSea bidding
- [ ] Update index.ts to use service
- [ ] Deploy to staging
- [ ] Monitor for 2 weeks
- [ ] Deploy to production

**Extracted Lines**: 0 / 5,000

---

### 3.2 Blur Bidding Service (~4,500 lines)
- [ ] Create `src/services/marketplace/blur-bidding.service.ts`
- [ ] Extract bid preprocessing
  - [ ] `preprocessBlurBid()` (scattered in lines 15000-22000)
- [ ] Extract scheduled bid processing
  - [ ] `processBlurScheduledBid()` (scattered)
- [ ] Extract authentication logic
- [ ] Extract rate limit coordination
- [ ] Extract token validation
- [ ] Write unit tests
- [ ] Integration test Blur bidding
- [ ] Update index.ts to use service
- [ ] Deploy to staging
- [ ] Monitor for 2 weeks
- [ ] Deploy to production

**Extracted Lines**: 0 / 4,500

---

### 3.3 MagicEden Bidding Service (~4,500 lines)
- [ ] Create `src/services/marketplace/magiceden-bidding.service.ts`
- [ ] Extract bid preprocessing
  - [ ] `preprocessMagicedenBid()` (scattered in lines 20000-25000)
- [ ] Extract scheduled bid processing
  - [ ] `processMagicedenScheduledBid()` (scattered)
- [ ] Extract token validation
- [ ] Extract trait filtering
- [ ] Write unit tests
- [ ] Integration test MagicEden bidding
- [ ] Update index.ts to use service
- [ ] Deploy to staging
- [ ] Monitor for 2 weeks
- [ ] Deploy to production

**Extracted Lines**: 0 / 4,500

---

## Phase 4: Supporting Services (Optional)

**Priority**: LOW
**Timeline**: Week 7+
**Status**: 🔴 Not Started

### 4.1 Connectivity Monitor Service (~200 lines)
- [ ] Create `src/services/connectivity-monitor.service.ts`
- [ ] Extract connectivity functions
  - [ ] `checkInternetConnectivity()` (line 1411)
  - [ ] `handleConnectivityRestored()` (line 1453)
  - [ ] `startConnectivityMonitoring()` (line 1496)
- [ ] Write unit tests
- [ ] Integration test connectivity monitoring
- [ ] Update index.ts to use service
- [ ] Deploy to staging
- [ ] Monitor for 1 week
- [ ] Deploy to production

**Extracted Lines**: 0 / 200

---

## Progress Summary

### Lines Extracted
```
Phase 1: 0 / 2,855 lines (0%)
Phase 2: 0 / 5,835 lines (0%)
Phase 3: 0 / 14,000 lines (0%)
Phase 4: 0 / 200 lines (0%)
Total:   0 / 22,890 lines (0%)

Target: Reduce index.ts from 28,827 to ~5,937 lines
```

### Console.log Replaced
```
Total: 0 / 34 instances (0%)
```

### Services Created
```
Phase 1: 0 / 4 services
Phase 2: 0 / 3 services
Phase 3: 0 / 3 services
Phase 4: 0 / 1 service
Total:   0 / 11 services
```

---

## Recent Updates

### 2026-01-24
- ✅ Created initial refactoring plan
- ✅ Created progress tracking document
- 🔴 Phase 1 work not started

---

## Notes

- Follow existing service patterns: `health-monitor.service.ts`, `queue.service.ts`
- Use debug-file-logger for all logging (no console.log)
- Follow singleton pattern with dependency injection
- Update CHANGELOG.md for all changes
- Test thoroughly before each deployment

---

## Quick Reference

**Service Pattern Template**:
```typescript
import { debugLog, errorLog, infoLog } from '../utils/debug-file-logger';

class ServiceName {
  private dependency: Type | null = null;

  public initialize(config: ConfigType): void {
    this.dependency = config.dependency;
    infoLog('ServiceName', 'Initialized');
  }

  public async coreMethod(): Promise<void> {
    // Implementation
  }

  public async shutdown(): Promise<void> {
    infoLog('ServiceName', 'Shutdown complete');
  }
}

export const serviceName = new ServiceName();
```

**Logging Replacement**:
```typescript
// BEFORE:
console.log('[STARTUP] Server started');
console.error('[API] Request failed', error);

// AFTER:
infoLog('Startup', 'Server started');
errorLog('API', 'Request failed', error);
```
