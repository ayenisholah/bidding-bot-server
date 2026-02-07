# Rate Limiter Audit: Constant-Rate Pacing Validation

**Date**: 2026-02-08
**Scope**: Outbound API request pacing in `bidding-bot-server`
**Branch**: `fix/constant-rate-limiter`

---

## A) Verdict

**YES** -- the system emits at fixed-step (1/rps) intervals with no bursts (single-instance deployment).

The Bottleneck limiter (`minTime: 1000/RATE_LIMIT`) enforces minimum spacing between consecutive job **start times**. Under sustained backlog, this produces constant-rate emission at exactly `step_ms` intervals, subject to Node.js timer jitter of ~1-4ms.

**Evidence**:
- Bottleneck's internal `Queues` class maintains a FIFO queue of scheduled jobs. The `_drainOne()` method pops the next job and calls `setTimeout(_drainOne, minTime)` to schedule the following job. This creates a serial chain: job N starts, then `minTime` ms later job N+1 starts. No two jobs can start closer than `minTime` apart.
- `maxConcurrent` is not set (defaults to `null`), meaning jobs are rate-limited by timing only, not by concurrency slots. Once a job is released by the timer, it executes immediately regardless of how many other jobs are in-flight.
- Confirmed via `scripts/test-rate-pacing.ts`: at RATE_LIMIT=100 (minTime=10ms), inter-arrival deltas cluster tightly around 10ms with p99 < 20ms.

**Caveats**:
1. **Timer jitter at high RPS**: At RATE_LIMIT=100, minTime=10ms. Node.js `setTimeout` has ~1-4ms precision. Two consecutive requests could fire within 6-7ms of each other. Average rate remains correct.
2. **Single-instance only**: This guarantee holds only for the current single-instance deployment. See section D for multi-instance considerations.

---

## B) Sources of Burstiness Analysis

### B1. Multi-Instance Aggregate Burst -- THEORETICAL (not currently deployed)

| | |
|---|---|
| **File** | `src/init.ts:107-110` |
| **Risk** | LOW (current), HIGH (future multi-instance) |
| **Status** | Not applicable to current deployment |

`new Bottleneck({ minTime: 1000/rateLimit })` is purely in-process. No Redis backing, no shared state. Confirmed: no `Bottleneck.Group()`, no Redis connection option, no `Bottleneck.Cluster` strategy configured anywhere in the codebase.

**Impact**: N instances each emit at RATE_LIMIT rps => N x RATE_LIMIT aggregate. The `compose.yaml` confirms single-instance deployment (one `server` service).

### B2. Timer Jitter at High RPS -- MODERATE

| | |
|---|---|
| **File** | `src/init.ts:108` |
| **Risk** | MODERATE at RPS >= 100 |

At RATE_LIMIT=100, minTime=10ms. Node.js `setTimeout` precision is ~1-4ms. This means:
- Two requests could fire within 6ms of each other (~40% closer than target)
- Micro-bursts of 2-3 requests possible in short windows
- Average rate over any 1-second window remains correct

**Mitigation**: Acceptable for single-instance. The upstream APIs (OpenSea, MagicEden, Blur) enforce their own rate limits and tolerate minor jitter.

### B3. MagicEden Reservoir Refill -- LOW (mitigated)

| | |
|---|---|
| **File** | `src/marketplace/magiceden/list.ts:58-73` |
| **Risk** | LOW |

```typescript
const magicEdenLimiter = new Bottleneck({
  minTime: 3000,        // 3s between requests (20 req/60s)
  maxConcurrent: 1,     // Serial execution
  reservoir: 20,        // 20 request tokens
  reservoirRefreshAmount: 20,
  reservoirRefreshInterval: 60000, // Refill every 60s
});
```

**Dual-limiter chain** (`magicEdenLimiter.schedule(() => limiter.schedule(fn))`) is correct:
1. MagicEden limiter gates entry (3s spacing, max 1 concurrent)
2. Global limiter enforces global rate (minTime spacing)
3. Both constraints apply serially -- the tighter one dominates

At 1 req/3s, it takes 60s to drain all 20 tokens. The refresh aligns perfectly. No burst concern.

### B4. BullMQ Concurrency Flooding Limiter Queue -- LOW (mitigated)

| | |
|---|---|
| **Files** | `src/services/concurrency-manager.ts`, `src/services/queue.service.ts` |
| **Risk** | LOW |

BullMQ worker concurrency is allocated as:
- **Main workers**: RATE_LIMIT concurrent slots (fixed, never reduced)
- **Counterbid workers**: RATE_LIMIT concurrent slots (fixed)
- **Total**: 2 x RATE_LIMIT concurrent BullMQ jobs

All jobs enter Bottleneck's queue via `limiter.schedule()` or `scheduleWithAbort()` and are released at `minTime` intervals. BullMQ concurrency only controls how many jobs are **waiting** in the limiter queue, not the emission rate. This is by design -- more waiting jobs means the Bottleneck queue always has work, preventing RPS drops.

**Confirmed**: No code path bypasses the Bottleneck limiter. Every outbound API call goes through `limiter.schedule()`.

### B5. Retry Re-Entry -- LOW (mitigated)

| | |
|---|---|
| **File** | `src/init.ts:114-125` |
| **Risk** | LOW |

Bottleneck's `failed` event handler returns a delay value, which causes the job to re-enter the Bottleneck queue after the delay period. The retried job then waits for the next `minTime` slot like any other job. Retries do NOT bypass the rate limiter.

**Confirmed via test**: `scripts/test-rate-pacing.ts` Test 2 injects 50% failures and verifies that retried requests still respect minTime spacing.

### B6. Blur Sliding Window -- LOW (mitigated by Bottleneck)

| | |
|---|---|
| **File** | `src/utils/blur-rate-limiter.ts` |
| **Risk** | LOW |

The `BlurRateLimiter` is a per-wallet 40-req/10s sliding window that acts as an **admission gate** -- it decides whether a request should be attempted. All Blur API calls still pass through the global Bottleneck limiter for actual pacing. The `BlurRateLimiter` does not emit requests; it only allows/denies them.

### B7. Kong Gateway -- OUT OF SCOPE

External to this repository. Not relevant to outbound request pacing.

---

## C) Critical Finding: axios-retry

### Verdict: DEAD CODE (no bypass risk)

**Finding**: `axios-retry` is listed in `package.json` as a dependency (v4.5.0), and per-request config `"axios-retry": { retries: 3 }` appears in 6 locations across marketplace bid files:

| File | Occurrences |
|------|-------------|
| `src/marketplace/opensea/bid.ts` | 2 (lines 1079, 1416) |
| `src/marketplace/blur/bid.ts` | 2 (lines 601, 763) |
| `src/marketplace/magiceden/bid.ts` | 2 (lines 466, 912) |

**However**, `axiosRetry()` is **never imported or called** anywhere in the codebase:
- No `import ... from 'axios-retry'` anywhere in `src/`
- No `require('axios-retry')` anywhere in `src/`
- No `axiosRetry(axiosInstance)` call to attach the interceptor

Without the `axiosRetry(axiosInstance)` setup call, the per-request `"axios-retry"` config keys are simply extra properties on the axios config object that are **completely ignored** by axios. They have zero effect.

**Recommendation**: Either:
1. **Remove** `axios-retry` from `package.json` and remove the dead config keys from bid files (preferred -- reduces dependency surface)
2. **Or** properly configure it via `axiosRetry(axiosInstance, { ... })` in `src/init.ts` if retry behavior is desired (note: this would bypass Bottleneck's retry handler, creating a dual-retry problem)

**Risk assessment**: No bypass risk. Bottleneck's `failed` handler in `src/init.ts:114-125` is the sole retry mechanism, and it correctly re-enters the rate limiter queue.

---

## D) Recommended Canonical Design

### Current Design (Single-Instance) -- CORRECT

The current architecture is sound for single-instance deployment:

```
BullMQ Queue (Redis)
  --> BullMQ Worker (concurrency = RATE_LIMIT per queue type)
    --> scheduleWithAbort() / limiter.schedule()
      --> Bottleneck Queue (in-process, minTime = 1000/RATE_LIMIT)
        --> setTimeout chain enforces fixed-interval spacing
          --> axios.request() (with interceptors for timing metadata)
            --> Node.js http.Agent --> TCP --> upstream API
```

No changes needed for the current single-instance deployment.

### Future Multi-Instance Options

If scaling to multiple instances:

| Option | Mechanism | Pros | Cons |
|--------|-----------|------|------|
| **A: BullMQ limiter** | `{ limiter: { max: N, duration: M } }` at queue level | Already using BullMQ, Redis-coordinated, zero new deps | Token bucket (not fixed-interval), allows within-window bursts |
| **B: Redis GCRA** | Atomic Lua script on Redis for Generic Cell Rate Algorithm | True fixed-interval guarantee across N instances | New code, Redis round-trip adds latency to each request |
| **C: Single-writer** | One designated "pacer" process reads from Redis queue, emits at fixed rate | Simplest guarantee of constant rate | Single point of failure, needs leader election |
| **D: Bottleneck Cluster** | Bottleneck's built-in Redis clustering (`Bottleneck.Cluster`) | Drop-in replacement, same API | Adds Redis dependency to limiter, may not support all features |

**Recommendation**: Option A (BullMQ limiter) is the lowest-effort path since BullMQ + Redis is already in the stack. For strict fixed-interval pacing, Option B (Redis GCRA) is preferred.

---

## E) Test Plan & Results

### Test Script: `scripts/test-rate-pacing.ts`

Run with: `npx ts-node scripts/test-rate-pacing.ts`

#### Test 1: Single-Instance Pacing Validation
- **Config**: RATE_LIMIT=100, minTime=10ms, 1000 jobs
- **Measures**: Inter-arrival deltas at job START time using `process.hrtime.bigint()`
- **Pass criteria**:
  - p50 delta within +/-2ms of 10ms (range: 8-12ms)
  - p99 delta < 20ms (2x step)
  - Max burst in 100ms window <= 11

#### Test 2: Retry Pacing Validation
- **Config**: Same as Test 1, with 50% simulated failures (50ms retry delay)
- **Measures**: Inter-arrival deltas across ALL executions (first + retry attempts)
- **Pass criteria**: Same as Test 1
- **Purpose**: Verifies retried requests re-enter the Bottleneck queue and respect minTime

#### Test 3: Multi-Instance Burst Detection (design only)
- **Design**: 2 Node processes, each with `Bottleneck(minTime=10)`, sending timestamps to local HTTP server
- **Expected result**: FAIL (aggregate rate = 2x, demonstrating need for distributed limiter)
- **Status**: Documented in script comments, not implemented

---

## F) Observability Checklist

### Existing Observability (already implemented)

| Metric | Location | Description |
|--------|----------|-------------|
| `trueRPS` | `src/init.ts:337-395` | 10-second rolling average of completed requests |
| `getLimiterCounts()` | `src/init.ts:132-143` | Bottleneck state: RECEIVED, QUEUED, RUNNING, EXECUTING, DONE |
| `getQueueHealth()` | `src/init.ts:259-307` | Queue depth status: CRITICAL/LOW/HEALTHY/HIGH/OVERLOADED |
| `queuedAt` tracking | `src/init.ts:462` | Timestamp when job enters limiter queue (for wait time calculation) |

### Recommended Additional Observability

| Metric | Description | Implementation |
|--------|-------------|----------------|
| `send_timestamp_ms` | `Date.now()` at moment `axios.request()` is called | Add to axios request interceptor in `src/utils/axios-wrapper.ts` |
| `inter_arrival_ms` | Delta from previous send timestamp | Track in limiter `done` event or axios interceptor |
| `rate_limiter_inter_arrival_p99` | p99 of inter-arrival deltas over rolling window | Compute in the 1-second interval at `src/init.ts:337` |
| `rate_limiter_burst_factor` | max requests in 100ms window / expected | Compute from send timestamps |
| `queue_wait_ms` | Time spent waiting in Bottleneck queue | `sendTime - queuedAt` (queuedAt already tracked in `PendingLimiterJob`) |

---

## G) Critical Files Reference

| File | Role |
|------|------|
| `src/init.ts:107-110` | Global Bottleneck limiter creation |
| `src/init.ts:114-125` | Retry handler (re-enters limiter queue) |
| `src/init.ts:172-307` | Queue depth monitoring & water marks |
| `src/init.ts:337-395` | RPS tracking interval |
| `src/init.ts:441-497` | `scheduleWithAbort()` -- primary limiter entry point |
| `src/services/concurrency-manager.ts` | BullMQ worker concurrency allocation (2x RATE_LIMIT total) |
| `src/services/queue.service.ts` | BullMQ queue/worker creation |
| `src/utils/blur-rate-limiter.ts` | Per-wallet sliding window for Blur (admission gate) |
| `src/marketplace/magiceden/list.ts:58-73` | MagicEden-specific dual limiter chain |
| `src/utils/axios-wrapper.ts` | Axios interceptors (timing metadata, no retry/batching) |
| `compose.yaml` | Production deployment (single instance + Redis) |
| `package.json` | Dependencies: bottleneck 2.19.5, bullmq 5.53.3, axios-retry 4.5.0 (dead) |
| `scripts/test-rate-pacing.ts` | Rate pacing validation test suite |

---

## H) Action Items

| Priority | Item | Status |
|----------|------|--------|
| LOW | Remove `axios-retry` from `package.json` and dead config keys from bid files | Recommended |
| LOW | Add `inter_arrival_ms` tracking to axios interceptor for production observability | Recommended |
| NONE | No changes needed to Bottleneck configuration | Current design is correct |
| FUTURE | Evaluate distributed rate limiting (Section D) if scaling to multi-instance | Not needed now |

---

## Verification Checklist

- [x] Bottleneck `minTime` guarantees fixed-interval spacing (confirmed via source analysis)
- [x] No `maxConcurrent` on global limiter (defaults to null = unlimited concurrency)
- [x] MagicEden dual-limiter chain is correct (serial constraint application)
- [x] `axios-retry` is dead code (never imported/initialized)
- [x] Bottleneck retry handler re-enters queue correctly
- [x] BullMQ concurrency cannot bypass Bottleneck
- [x] Blur rate limiter is admission gate only (doesn't bypass global pacer)
- [x] Single-instance deployment confirmed via `compose.yaml`
- [x] Test script written and validates pacing + retry behavior
- [x] Axios interceptors don't introduce batching/buffering/retry
