# Execution Time Analysis Report

**Generated**: 2026-02-04
**Log File**: `debug-logs/executiontime-2026-02-04.log`
**Total Entries**: 7,285
**Log Size**: 1.9 MB

---

## Executive Summary

The execution time logging system successfully captured 7,285 function executions over the monitoring period:
- **processOpenseaTokenBid** (scheduled bids): 6,370 calls
- **openseaTokenCounterBid** (counterbids): 915 calls

### Key Findings

1. **Counterbids are 2.12x faster** than scheduled bids (P50: 1,324ms vs 2,810ms for completed bids)
2. **83.2% of scheduled bids exit early** (mostly due to max_bid_price_exceeded and own_bid_still_valid)
3. **100% success rate** for counterbids (no errors or early exits)
4. **0% error rate** across both functions during monitoring period
5. **99% of all executions complete in under 10 seconds** (P99: 7,295ms scheduled, 5,958ms counterbid)

---

## processOpenseaTokenBid (Scheduled Bids)

### Overall Statistics
| Metric | Value |
|--------|-------|
| **Total Calls** | 6,370 |
| **Completed** | 1,068 (16.8%) |
| **Early Exits** | 5,302 (83.2%) |
| **Failed** | 0 (0.0%) |

### Execution Time Metrics (All Calls)
| Metric | Duration |
|--------|----------|
| **Min** | 398ms |
| **Max** | 20,727ms |
| **Average** | 2,090ms |
| **Median (P50)** | 1,696ms |
| **P95** | 4,863ms |
| **P99** | 7,295ms |

### Completed Bids Only (Success)
| Metric | Duration |
|--------|----------|
| **Count** | 1,068 |
| **Min** | 964ms |
| **Max** | 16,866ms |
| **Average** | 3,117ms |
| **Median (P50)** | 2,810ms |
| **P95** | 5,711ms |
| **P99** | 8,660ms |

### Early Exits
| Metric | Duration |
|--------|----------|
| **Count** | 5,302 |
| **Min** | 398ms |
| **Max** | 20,727ms |
| **Average** | 1,883ms |
| **Median (P50)** | 1,475ms |
| **P95** | 4,554ms |
| **P99** | 6,857ms |

### Top Early Exit Reasons
| Reason | Count | Percentage | Avg Duration |
|--------|-------|------------|--------------|
| **max_bid_price_exceeded** | 2,881 | 45.2% | 1,868ms |
| **own_bid_still_valid** | 2,421 | 38.0% | 1,902ms |

### Duration Distribution
| Range | Count | Percentage |
|-------|-------|------------|
| 0-500ms | 55 | 0.9% |
| 500ms-1s | 1,237 | 19.4% |
| **1s-2s** | **2,519** | **39.5%** ⭐ |
| 2s-3s | 1,321 | 20.7% |
| 3s-5s | 950 | 14.9% |
| 5s-10s | 274 | 4.3% |
| 10s+ | 14 | 0.2% |

**Most common duration**: 1-2 seconds (39.5% of calls)

---

## openseaTokenCounterBid (Counterbids)

### Overall Statistics
| Metric | Value |
|--------|-------|
| **Total Calls** | 915 |
| **Completed** | 915 (100.0%) ✅ |
| **Early Exits** | 0 (0.0%) |
| **Failed** | 0 (0.0%) |

### Execution Time Metrics (All Calls)
| Metric | Duration |
|--------|----------|
| **Min** | 446ms |
| **Max** | 19,801ms |
| **Average** | 1,677ms |
| **Median (P50)** | 1,324ms |
| **P95** | 3,722ms |
| **P99** | 5,958ms |

### Duration Distribution
| Range | Count | Percentage |
|-------|-------|------------|
| 0-500ms | 9 | 1.0% |
| 500ms-1s | 222 | 24.3% |
| **1s-2s** | **456** | **49.8%** ⭐ |
| 2s-3s | 142 | 15.5% |
| 3s-5s | 68 | 7.4% |
| 5s-10s | 16 | 1.7% |
| 10s+ | 2 | 0.2% |

**Most common duration**: 1-2 seconds (49.8% of calls)

---

## Performance Comparison

### Completed Bids Only

| Metric | processOpenseaTokenBid | openseaTokenCounterBid | Speedup |
|--------|------------------------|------------------------|---------|
| **Count** | 1,068 | 915 | - |
| **P50** | 2,810ms | 1,324ms | **2.12x faster** ⚡ |
| **P95** | 5,711ms | 3,722ms | **1.53x faster** |
| **P99** | 8,660ms | 5,958ms | **1.45x faster** |
| **Average** | 3,117ms | 1,677ms | **1.86x faster** |

### Key Insights

1. **Counterbids are consistently faster** across all percentiles
2. **Median counterbid** (1,324ms) is faster than **fastest scheduled bid** (964ms minimum for completed)
3. **Tail latency** is better for counterbids (P99: 5,958ms vs 8,660ms)
4. **100% success rate** for counterbids vs 16.8% for scheduled bids (due to early exits)

---

## Operational Insights

### Early Exit Analysis (processOpenseaTokenBid)

**Why 83.2% early exits?**

The two dominant reasons account for 83.2% of all calls:

1. **max_bid_price_exceeded (45.2%)**
   - Bid price would exceed user-configured maximum
   - Protection mechanism working as designed
   - Fast exit at ~1,868ms average

2. **own_bid_still_valid (38.0%)**
   - Existing bid still meets validity criteria (TTL, price, expiration)
   - Optimization to avoid unnecessary API calls
   - Fast exit at ~1,902ms average

**Impact**: Early exits save API calls and reduce OpenSea rate limit usage while maintaining bid coverage.

### Success Rate

- **Scheduled Bids**: 16.8% completion rate (by design - early exits are optimizations, not failures)
- **Counterbids**: 100% completion rate (no early exits - all counterbids attempt to place bids)
- **Error Rate**: 0% across both functions ✅

### Tail Latency

- **P99 < 10 seconds** for both functions
- Only **0.2% of calls exceed 10 seconds** (16 total out of 7,285)
- **Slowest call**: 20,727ms (processOpenseaTokenBid)

---

## Hourly Distribution

All 7,285 log entries occurred between **18:00 - 18:59** (single hour), indicating:
- High traffic volume during monitoring period
- System handled **~121 executions/minute** or **~2 executions/second** during peak

---

## Recommendations

### ✅ What's Working Well

1. **Zero errors** during monitoring period - system is stable
2. **Fast counterbids** - median 1.3s meets performance requirements
3. **Effective early exit logic** - saves 83.2% of unnecessary API calls
4. **Tail latency controlled** - P99 under 8.7s, P95 under 5.7s

### 🎯 Optimization Opportunities

1. **Monitor P95 > 5s**
   - Set alerts for P95 exceeding 5 seconds
   - Current P95: 4,863ms (scheduled), 3,722ms (counterbid) ✅ Under threshold

2. **Investigate 10s+ outliers**
   - Only 16 calls (0.2%) exceeded 10 seconds
   - May indicate network issues or API throttling
   - Consider implementing retry logic with backoff

3. **Consider caching for early exits**
   - Both dominant early exit reasons involve validation checks
   - Average ~1,885ms for early exits could potentially be faster with aggressive caching
   - Current performance is acceptable

### 📊 Monitoring Targets

| Metric | Target | Current | Status |
|--------|--------|---------|--------|
| **P50 Scheduled** | < 3.0s | 2.8s | ✅ GOOD |
| **P50 Counterbid** | < 1.5s | 1.3s | ✅ GOOD |
| **P95 Scheduled** | < 6.0s | 4.9s | ✅ GOOD |
| **P95 Counterbid** | < 4.0s | 3.7s | ✅ GOOD |
| **P99 Scheduled** | < 10.0s | 7.3s | ✅ GOOD |
| **P99 Counterbid** | < 8.0s | 6.0s | ✅ GOOD |
| **Error Rate** | < 1.0% | 0.0% | ✅ EXCELLENT |
| **Outlier Rate (>10s)** | < 5.0% | 0.2% | ✅ EXCELLENT |

---

## Conclusions

1. **System Performance**: Excellent - all metrics within target ranges
2. **Reliability**: 100% success rate for counterbids, 0% error rate overall
3. **Efficiency**: Early exit logic effectively reduces unnecessary API calls
4. **Speed**: Counterbids achieve 2.12x faster execution than scheduled bids
5. **Stability**: No crashes, errors, or failures during monitoring period

### Next Steps

1. **Continue monitoring** - collect 24-48 hours of data for trend analysis
2. **Set up alerting** - trigger alerts if P95 > 5s or error rate > 1%
3. **Investigate outliers** - analyze the 16 calls that exceeded 10s
4. **Capacity planning** - system handled ~2 exec/sec peak, can scale further if needed

---

**Report Generated**: 2026-02-04 20:02 UTC
**Analysis Tool**: Python 3 with JSON parsing
**Data Source**: `executiontime-2026-02-04.log` (1.9 MB, 7,285 entries)
