# Failure Modes Checklist & Hardening Recommendations

**Audit Date**: 2026-02-10
**Scope**: Rate limiter, queue system, retry logic, worker concurrency
**Codebase**: `bidding-bot-server/src/`

---

## Summary

| Risk Level | Count |
|------------|-------|
| CRITICAL   | 1     |
| HIGH       | 3     |
| MEDIUM     | 4     |
| LOW        | 3     |

---

### FM-1: Event-Loop Jitter at 10ms `minTime` Intervals

**Risk**: MEDIUM
**Location**: `init.ts:108` (`minTime: 1000 / rateLimit` where RATE_LIMIT=100 yields minTime=10ms)

**Description**: Node.js `setTimeout` guarantees a *minimum* delay, not an exact one. At 10ms intervals, expected jitter sources:
- **Timer resolution**: Node.js timers have ~1ms resolution. The actual callback fires in the 10-12ms range under light load.
- **Event-loop contention**: Under heavy I/O (Redis round-trips, HTTP responses, JSON parsing) or GC pauses, event-loop lag can spike to 20-50ms, effectively halving throughput.
- **GC pauses**: V8 incremental GC typically adds 1-5ms per minor GC. Major GC events on large heaps (>1GB) can add 50-200ms pauses.
- **Bottleneck internal overhead**: `trackDoneStatus: true` (init.ts:109) enables completion tracking. Bottleneck maintains a `DONE` counter via its internal job lifecycle hooks. This adds per-request bookkeeping (~0.01ms per job) which is negligible at 100 RPS.
- **Practical impact**: At RATE_LIMIT=100, the system targets 100 RPS. Jitter means actual throughput will fluctuate between 60-100 RPS under moderate load. The 1-second RPS averaging window (init.ts:337-395) already smooths this.

**Current Mitigation**: Bottleneck's internal timer management handles scheduling. The `trueRPS` calculation (init.ts:359) uses a 10-second averaging window with `Math.min(RATE_LIMIT, ...)` capping to account for jitter. The `refillLimiterQueueIfNeeded` mechanism (index.ts:3213) runs every 1 second to keep the pipeline fed.

**Recommendation**: Monitor event-loop lag with `perf_hooks.monitorEventLoopDelay()` and log warnings when p99 exceeds 50ms. This is primarily observability, not a code fix. Consider setting `--max-old-space-size` to prevent excessive GC on large heaps.

**Action Required**: MONITOR

---

### FM-2: Multi-Worker Shared Limiter -- Single-Process Assumption

**Risk**: LOW
**Location**: `init.ts:14` (singleton `limiter`), `index.ts:2200-2203` (worker creation), `queue.service.ts:231-246`

**Description**: The system creates 2 main workers + 2 counterbid workers (index.ts:2202-2203) plus a Blur worker. All BullMQ workers run **in-process** (they are `Worker` instances from the `bullmq` package, not child processes or cluster workers). They share the module-level `limiter` singleton exported from `init.ts`. This means all `limiter.schedule()` calls from all workers funnel through the same Bottleneck instance.

Bottleneck is designed for single-process, single-threaded concurrency control and uses an internal FIFO/priority queue with synchronous dispatching. There are **no race conditions** here because:
1. Node.js is single-threaded -- all `limiter.schedule()` calls are serialized by the event loop.
2. The Bottleneck instance uses no `maxConcurrent` limit (only `minTime`), so it acts as a pure rate-spacer.
3. The singleton pattern (`let limiter: Bottleneck` at module scope, exported via `init.ts`) guarantees a single instance per process.

**Caveat**: If the architecture ever moves to `cluster` mode or child processes, the shared limiter would break. Bottleneck provides a Redis-backed `Cluster` mode for this, but it is not currently needed.

Note: `queue.service.ts` has its own worker creation path (used for Blur workers). It also references `init.ts` imports for `orderCache`, confirming both code paths share the same process.

**Current Mitigation**: Single-process architecture with module-level singleton. BullMQ workers are in-process event-loop threads, not separate processes.

**Recommendation**: Add a startup assertion or comment documenting the single-process requirement. If cluster mode is ever considered, switch Bottleneck to its Redis-backed `Cluster` strategy.

**Action Required**: NO (acceptable as-is)

---

### FM-3: Retry Storm After Bulk 429 Responses

**Risk**: HIGH
**Location**: `init.ts:38-96` (`calculateRetryDelay`), `init.ts:114-125` (Bottleneck FAILED handler)

**Description**: If 50 in-flight requests receive HTTP 429 simultaneously with `retry-after: 2s`, all 50 will be retried after exactly 2 seconds (init.ts:69-71). The `retry-after` header path returns `parseInt(retryAfter) * 1000` with **zero jitter** -- it uses the server's value verbatim:

```typescript
if (retryAfter) {
  return parseInt(retryAfter) * 1000;  // NO jitter added
}
```

This creates a **thundering herd**: 50 requests all re-enter Bottleneck's queue at t+2000ms. While `minTime=10ms` will space them out (taking 500ms to drain), the API server sees a sudden spike from zero to full rate, which can trigger another 429 wave.

For the fallback path (no `retry-after` header), jitter ranges are:
- **429/430**: 100-400ms jitter on exponential backoff (2s, 4s, 8s base). Range at retry 1: 2100-2500ms. This is a **400ms spread for potentially 50+ requests**, which is insufficient. At 10ms spacing, 50 requests drain in 500ms, but they all *arrive* in the queue within 400ms of each other.
- **520**: 500-1000ms jitter on 5s/15s/30s base. Better spread.
- **502/504**: 200-600ms jitter on 3s/6s/12s base. Moderate spread.

**Quantitative analysis**: 50 retries with 400ms jitter spread, dispatched at 10ms intervals = ~50 slots filled in 500ms. The queue absorbs this as a brief burst. But the API server still sees 50 requests in ~500ms, which is effectively 100 RPS -- exactly the rate limit. If the server's 429 window is per-second, this will trigger another round.

**Current Mitigation**: Bottleneck's `minTime` enforcement prevents truly simultaneous dispatch. The exponential backoff increases delays on subsequent retries (retry 2: 4s, retry 3: 8s). MAX_RETRIES=3 caps total retry attempts.

**Recommendation**:
1. **Add jitter to the `retry-after` path**: When `retry-after` header is present, add 200-2000ms of random jitter on top.
2. **Widen jitter ranges for 429**: Increase from 100-400ms to 500-2000ms to spread retries over a full 1.5-second window.
3. Consider a **retry budget**: Track the number of concurrent retries pending and delay additional retries when budget is exceeded.

**Action Required**: YES (code change needed)

---

### FM-4: Queue Health Monitoring is Log-Only (Passive)

**Risk**: LOW
**Location**: `init.ts:337-395` (monitoring interval), `init.ts:187-254` (exported health functions), `index.ts:4945-4975` (`refillLimiterQueueIfNeeded`)

**Description**: The `setInterval` at init.ts:337 monitors queue health every 1 second but **only logs warnings** (errorLog/warnLog). However, the exported functions (`shouldRefillQueue()`, `shouldPauseQueueing()`, `getRecommendedRefillCount()`, `isBelowMinimum()`) **are actively consumed**:

- `refillLimiterQueueIfNeeded()` at index.ts:4945-4975 runs on a 1-second interval (index.ts:3213) and calls `getQueueHealth()`, which internally uses `isBelowMinimum()`, `shouldRefillQueue()`, `shouldPauseQueueing()`, and `getRecommendedRefillCount()`.
- When the queue is below the low water mark or minimum, it triggers `executeMarketplaceBidLoop()` for active candidates to refill the Bottleneck pipeline.

This means the system **does have an active feedback loop**, not just passive monitoring. The init.ts monitoring interval serves a secondary purpose: logging for observability.

One gap: `shouldPauseQueueing()` is computed in `getQueueHealth()` but `refillLimiterQueueIfNeeded()` only checks `needsRefill` and `isBelowMinimum` -- it never checks the pause signal. If the queue is overloaded (above HIGH_WATER_MARK = 3x RATE_LIMIT = 300), the bidding loop continues adding jobs anyway. However, the bidding loop is timer-based (not queue-triggered), so overloading is self-limiting -- new jobs only arrive on loop interval boundaries.

**Current Mitigation**: Active refill mechanism exists and runs every second. The `shouldPauseQueueing()` gap is mitigated by the timer-based loop architecture, which naturally limits inflow.

**Recommendation**: Wire `shouldPauseQueueing()` into the bidding loop's job-enqueue path as a defensive check. This is defense-in-depth, not critical.

**Action Required**: NO (acceptable as-is, refill mechanism is active)

---

### FM-5: Worker Concurrency Creates Massive Bottleneck Queue Depth

**Risk**: HIGH
**Location**: `index.ts:2200-2201` (concurrency config), `init.ts:107-110` (limiter config)

**Description**: The concurrency configuration is:
- **Main workers**: 2 workers x `RATE_LIMIT * 5` = 2 x 500 = **1000 concurrent BullMQ job slots**
- **Counterbid workers**: 2 workers x `RATE_LIMIT * 1` = 2 x 100 = **200 concurrent BullMQ job slots**
- **Total**: 1200 concurrent BullMQ jobs, all calling `limiter.schedule()`

Each `limiter.schedule()` call adds a pending job to Bottleneck's internal priority queue. At steady state, with 100 RPS throughput and ~300ms average API latency, ~30 jobs are in-flight (RUNNING) at any time. The remaining 1170 slots represent jobs that have been picked up by BullMQ workers, are waiting inside `limiter.schedule()`, and consume:
- **Memory per pending Bottleneck job**: ~500 bytes (closure + metadata + promise). 1000 pending jobs = ~500KB. This is negligible.
- **Priority queue operations**: Bottleneck uses a sorted insert for priority scheduling. At 1000 entries, each insert is O(n) in the worst case for its internal implementation. At 100 inserts/second this is still sub-millisecond.

However, the real concern is **BullMQ lock management**. Each of those 1200 concurrent jobs holds a BullMQ lock (lockDuration: 45s, stalledInterval: 15s). If a job sits in `limiter.schedule()` for more than 45 seconds waiting for its turn:
- The lock expires
- BullMQ marks the job as stalled
- After `maxStalledCount` (4) stall checks, the job is moved to failed
- The job never actually executed -- it was just waiting in the Bottleneck queue

With 1000 pending jobs and 100 RPS throughput, a newly queued job waits ~10 seconds for its turn (1000/100). This is within the 45s lock duration. But during a retry storm (FM-3) or rate limit event, wait times could spike to 30-60 seconds, causing lock expiration cascades.

**Current Mitigation**: The lockDuration=45s provides a 35-second buffer beyond the typical 10s wait. The `maxStalledCount=4` with `stalledInterval=15s` gives a 60-second total window before failure.

**Recommendation**:
1. Reduce main worker concurrency from `RATE_LIMIT * 5` to `RATE_LIMIT * 2`. The 5x headroom is excessive -- the `refillLimiterQueueIfNeeded` mechanism already keeps the pipeline fed. 2x provides sufficient buffer while keeping the Bottleneck queue shallower.
2. Monitor `getQueueDepth()` and alert when it exceeds 500 (5 seconds of buffer), indicating potential lock-expiry risk.

**Action Required**: YES (code change recommended -- reduce concurrency multiplier)

---

### FM-6: Custom RateLimiter Class is Dead Code with Initial Burst Risk

**Risk**: LOW
**Location**: `index.ts:377-405` (class definition)

**Description**: The `RateLimiter` class at index.ts:377 is a custom token-bucket implementation that:
1. Initializes `this.tokens = rate` (full bucket) -- allowing an initial burst equal to the full rate limit
2. Uses a polling loop with 10ms sleep (`setTimeout(resolve, 10)`) for token acquisition
3. Implements a simple token refill based on elapsed time

**Search results**: `new RateLimiter(` and `RateLimiter(` appear nowhere in the codebase except the class definition itself and `BlurRateLimiter` (a separate class in `utils/blur-rate-limiter.ts`). The active rate limiting is handled entirely by the Bottleneck limiter in `init.ts`.

This class is **dead code**. If ever re-activated, the `this.tokens = rate` initialization would allow an immediate burst of `rate` requests (e.g., 100 requests at once) before the rate limiter engages. This could overwhelm APIs at startup.

**Current Mitigation**: Class is unused. The Bottleneck limiter handles all rate limiting.

**Recommendation**: Remove the dead code to prevent accidental future use. If the class is kept for reference, add a `@deprecated` JSDoc tag.

**Action Required**: NO (dead code, remove at convenience)

---

### FM-7: `limiter.schedule()` Calls Without Priority -- Audit

**Risk**: HIGH
**Location**: Multiple files (see table below)

**Description**: The codebase has two scheduling patterns:
1. **`scheduleWithPriority(fn, priority)`** -- wrapper in init.ts:530 that passes priority to Bottleneck
2. **Direct `limiter.schedule(() => ...)`** -- bypasses the wrapper, uses Bottleneck's default priority (5)

Direct calls without priority were found in the following locations:

| File | Line(s) | Context | Default Priority Used |
|------|---------|---------|----------------------|
| `functions/tokenMetadata.ts` | 121, 206, 284 | OpenSea GraphQL metadata queries | 5 (default) |
| `functions/rarity.ts` | 61 | Rarity data fetch | 5 (default) |
| `functions/priceMonitor.ts` | 51 | Price monitoring | 5 (default) |
| `functions/list.ts` | 513, 607 | Listings queries | 5 (default) |
| `marketplace/magiceden/bid.ts` | 1217, 1255 | **Cancel operations** (cancel offer) | 5 (default) |
| `marketplace/blur/bid.ts` | 394, 405, 948 | Blur submit bid, **cancel bid** | 5 (default) |
| `marketplace/opensea/bid.ts` | 1148, 1200, 1299, 1673, 2334 | OpenSea fulfill, cancel, GraphQL | 5 (default) |
| `marketplace/blur/list.ts` | 157, 264 | Blur listing operations | 5 (default) |
| `marketplace/magiceden/list.ts` | 69 | MagicEden listing | 5 (default) |
| `utils/nft.ts` | 192 | NFT metadata fetch | 5 (default) |

**Key concern**: Cancel operations (MagicEden cancel at bid.ts:1217, Blur cancel at bid.ts:948) use default priority 5, which is the same as `PRIORITY.SCHEDULED_BID`. Cancel operations during task stop compete equally with new bids for rate limit slots. Per the PRIORITY constants (init.ts:519-527), cancels from the scheduled cancel path should use `PRIORITY.SCHEDULED_CANCEL = 3`.

Additionally, calls from `functions/tokenMetadata.ts`, `functions/list.ts`, and `functions/priceMonitor.ts` are fetch/query operations that should use `PRIORITY.FETCH_QUERY = 7` to yield to bid and cancel operations.

The `scheduleWithPriority` wrapper is only used by `collection-data-warmer.service.ts` (priority 999) and the wrapper itself. The `scheduleWithTaskValidation` function (init.ts:510) also uses no priority.

**Current Mitigation**: The PRIORITY constants exist (init.ts:519-527) and some callers use them correctly (e.g., counterbid bids use priority 0, fetch queries in index.ts use priority 7). But the coverage is incomplete.

**Recommendation**:
1. Add `PRIORITY.SCHEDULED_CANCEL` (3) to all cancel operations in `marketplace/*/bid.ts`.
2. Add `PRIORITY.FETCH_QUERY` (7) to all query/fetch calls in `functions/*.ts` and `utils/nft.ts`.
3. Update `scheduleWithTaskValidation` to accept a priority parameter.
4. Consider a lint rule or code review checklist item to catch direct `limiter.schedule()` calls without priority.

**Action Required**: YES (code change needed)

---

### FM-8: Retry-After Header Parsed Without Validation

**Risk**: MEDIUM
**Location**: `init.ts:68-71`

**Description**: The `retry-after` header is parsed with `parseInt(retryAfter) * 1000`:

```typescript
const retryAfter = error.response.headers['retry-after'];
if (retryAfter) {
  return parseInt(retryAfter) * 1000;
}
```

Issues:
1. **No upper bound**: A malicious or misconfigured server could send `retry-after: 86400` (24 hours), causing the request to be delayed indefinitely in Bottleneck's retry queue. The Bottleneck FAILED handler returns this value as the retry delay.
2. **`parseInt` edge cases**: `parseInt("2.5")` returns `2`, `parseInt("abc")` returns `NaN`, `parseInt("")` returns `NaN`. If `NaN` is returned, Bottleneck receives `NaN` as the delay, which may cause unpredictable behavior (likely immediate retry or error).
3. **RFC 7231 format**: The `retry-after` header can be a date string (e.g., `Wed, 21 Oct 2015 07:28:00 GMT`) instead of seconds. `parseInt` on a date string returns `NaN`.

**Current Mitigation**: None. The value is used verbatim.

**Recommendation**:
1. Clamp the parsed value: `Math.min(Math.max(parsedValue, 1), 30)` (1-30 second range).
2. Add `isNaN` check with fallback to exponential backoff.
3. Handle date-format `retry-after` headers (or at minimum, detect and fall through to exponential backoff).

**Action Required**: YES (code change needed)

---

### FM-9: `trackDoneStatus: true` Unbounded Counter Growth

**Risk**: MEDIUM
**Location**: `init.ts:109`

**Description**: Bottleneck's `trackDoneStatus: true` maintains a running count of completed jobs in its `DONE` counter (used at init.ts:340: `counts.DONE`). This counter is a simple integer that increments on every completed job and **never resets** for the lifetime of the limiter instance.

At 100 RPS, the counter reaches:
- 1 hour: 360,000
- 24 hours: 8,640,000
- 30 days: 259,200,000

JavaScript safely handles integers up to `Number.MAX_SAFE_INTEGER` (9,007,199,254,740,991), so integer overflow is not a risk. However, the `previousDoneCount` tracking at init.ts:340-341 computes `counts.DONE - previousDoneCount` for RPS calculation. Both values grow together, so the difference stays small.

The `rpsHistory` array (init.ts:20) is bounded to 10 entries (init.ts:347-349), so it does not grow.

**Current Mitigation**: The counter is a Number type that can safely hold values far exceeding realistic operation counts. The RPS calculation uses differences, not absolutes.

**Recommendation**: No immediate action needed. If the server runs continuously for months, consider periodically recreating the limiter (during a maintenance window) to reset counters, or switch to a modular RPS calculation that doesn't depend on `DONE` counts.

**Action Required**: NO (acceptable as-is)

---

### FM-10: Timeout Timer Leak in Worker Job Processing

**Risk**: MEDIUM
**Location**: `index.ts:2220-2229`, `queue.service.ts:271-277`

**Description**: Each BullMQ job creates a `setTimeout` for the 60-second job timeout:

```typescript
const timeoutPromise = new Promise((_, reject) => {
  setTimeout(() => {
    abortController.abort();
    reject(new Error(`Job ${customJobId} timed out after ${jobTimeout}ms`));
  }, jobTimeout);
});
```

When the job completes successfully (before timeout), the `setTimeout` is **never cleared**. The timer remains in Node.js's timer queue for the full 60 seconds before firing and rejecting a promise that nothing is listening to.

With 1200 concurrent BullMQ job slots and ~300ms average job duration, there are ~1200 orphaned timers at any time (each lasting 60s, new jobs create new timers constantly). Each timer holds a closure referencing `abortController`, `job`, `customJobId`, and the reject function.

Impact:
- **Memory**: ~1200 closures x ~1KB = ~1.2MB. Negligible.
- **Timer queue overhead**: Node.js efficiently handles thousands of timers. Negligible.
- **Unhandled rejection risk**: When the timeout fires after job completion, the rejected promise is not caught, potentially triggering `unhandledRejection` events. However, `Promise.race` resolves with the first settled promise, and the losing promise's rejection is silently ignored by the V8 engine (it does not trigger `unhandledRejection` for race losers).

**Current Mitigation**: V8/Node.js garbage collects the timer callbacks after they fire. `Promise.race` safely ignores the losing promise.

**Recommendation**: Store the timer ID and clear it on success: `const timerId = setTimeout(...)` then `clearTimeout(timerId)` in the `try` block after `Promise.race` resolves. This is a minor cleanup optimization.

**Action Required**: MONITOR (functionally safe, but clearing timers is good practice)

---

### FM-11: Duplicate `failed` Event Handlers on Workers

**Risk**: MEDIUM
**Location**: `index.ts:2283-2411` (main worker), `index.ts:2491-2580` (counterbid worker), `queue.service.ts:312-458`

**Description**: Both the main worker and counterbid worker register **two separate `failed` event handlers** on the same worker instance:

1. **First handler** (index.ts:2283): Handles logging, bid-job marking, cancellation tracking, cache cleanup
2. **Second handler** (index.ts:2394): Handles guaranteed cleanup for cancellation jobs

Both handlers fire for every failed job. For cancellation job failures, the cache cleanup runs **twice**:
- First handler (index.ts:2337-2350): `safeDeleteFromCache(job.data.orderKey, ...)`
- Second handler (index.ts:2400-2408): `safeDeleteFromCache(job.data.orderKey, ...)` again

The `queue.service.ts` file has the same pattern (lines 312-382 and 434-458).

Impact: The double deletion is idempotent (deleting a non-existent cache entry is a no-op), so there is no functional bug. However, it doubles the Redis operations for each failed cancellation job and generates duplicate log entries.

**Current Mitigation**: Cache deletion is idempotent. The `errorLogCache.addIfNotExists` deduplication prevents double logging in the first handler (but the second handler uses `warnLog` directly without dedup).

**Recommendation**: Consolidate into a single `failed` handler that handles both the tracking/marking logic and the guaranteed cleanup. This reduces Redis operations and simplifies the code.

**Action Required**: YES (code cleanup -- consolidate duplicate handlers)

---

## Priority Matrix

| Priority | Failure Mode | Action |
|----------|-------------|--------|
| P0 (Fix Now) | FM-3: Retry Storm (no jitter on retry-after) | Add jitter to retry-after path |
| P0 (Fix Now) | FM-7: Missing priorities on cancel/fetch calls | Apply PRIORITY constants consistently |
| P1 (Fix Soon) | FM-8: Unbounded retry-after parsing | Clamp and validate header value |
| P1 (Fix Soon) | FM-5: Excessive worker concurrency | Reduce from 5x to 2x RATE_LIMIT |
| P2 (Improve) | FM-11: Duplicate failed handlers | Consolidate handlers |
| P2 (Improve) | FM-10: Timeout timer leak | Clear timers on success |
| P3 (Monitor) | FM-1: Event-loop jitter | Add event-loop monitoring |
| P3 (Accept) | FM-2: Single-process assumption | Document requirement |
| P3 (Accept) | FM-4: Queue health passivity | Already actively consumed |
| P3 (Accept) | FM-6: Dead code RateLimiter class | Remove at convenience |
| P3 (Accept) | FM-9: Unbounded DONE counter | No action needed |
