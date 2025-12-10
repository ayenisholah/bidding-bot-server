# Error Tracking Documentation

This document catalogs known error types, their root causes, and troubleshooting steps for the Bidding Bot Server.

---

## Error Catalog

### 1. Wallet Not Found

**Error Pattern:**
```
[ERROR][system][BulkCancelOpensea][error] Wallet 0x...XXXX not found for task <taskId>
```

**Severity:** Critical

**Source:** `src/index.ts:18628-18630` (`bulkCancelOpenseaBid` function)

**Root Causes:**
| Cause | Likelihood | Description |
|-------|------------|-------------|
| Deleted Wallet | High | Wallet was removed from MongoDB but tasks still reference it |
| Cache Expiration | Medium | Wallet cache (30-min TTL) expired and DB lookup failed |
| Invalid walletId | Low | Task created with non-existent walletId |

**Cascade Effect:**
This error triggers a cascade of related errors:
```
Wallet Not Found
    ↓
[ERROR][system][BulkCancelOpensea][error] Failed to cancel OpenSea order
    ↓
[ERROR][system][CancelOpenseaBid][error] Failed to cancel order
    ↓
[ERROR][system][Worker][error] Job CANCEL_OPENSEA_BID failed
```

**Troubleshooting:**
1. Check if wallet exists in MongoDB:
   ```javascript
   db.wallets.findOne({ _id: ObjectId("<walletId>") })
   ```
2. Verify task's walletId reference:
   ```javascript
   db.tasks.findOne({ _id: ObjectId("<taskId>") }, { walletId: 1 })
   ```
3. Check wallet cache status in server logs

**Resolution:**
- If wallet was deleted: Stop affected tasks and reassign to valid wallet
- If cache issue: Restart server to refresh wallet cache
- If invalid walletId: Update task with valid walletId

---

### 2. Database Failed

**Error Pattern:**
```
[ERROR] titan_SERVER_DATABASE_FAILED
```

**Severity:** Medium

**Source:** `src/index.ts:18845` (`validateBidCount` function)

**Root Causes:**
| Cause | Likelihood | Description |
|-------|------------|-------------|
| MongoDB Connection | High | Connection pool exhausted or network issue |
| Query Timeout | Medium | Large aggregation queries timing out |
| Collection Lock | Low | Write operations blocking reads |

**Troubleshooting:**
1. Check MongoDB connection status:
   ```bash
   docker logs mongodb-container | tail -50
   ```
2. Monitor MongoDB metrics:
   ```javascript
   db.serverStatus().connections
   db.serverStatus().opcounters
   ```
3. Check for slow queries:
   ```javascript
   db.currentOp({ "secs_running": { "$gt": 5 } })
   ```

**Resolution:**
- Increase MongoDB connection pool size
- Add appropriate indexes for frequent queries
- Implement retry logic with exponential backoff

---

### 3. OpenSea API AxiosError

**Error Pattern:**
```
[ERROR] titan_SERVER_OPENSEA_BID_OPENSEA_BIDDING_AxiosError
```

**Severity:** Low

**Source:** OpenSea API interactions throughout `src/index.ts`

**Root Causes:**
| Cause | Likelihood | HTTP Code | Description |
|-------|------------|-----------|-------------|
| Rate Limiting | High | 429 | Too many API requests |
| Network Timeout | Medium | ETIMEDOUT | Connection timeout |
| Invalid Order | Low | 400/404 | Order already cancelled/expired |
| Auth Failure | Low | 401 | API key expired |

**Troubleshooting:**
1. Check rate limit status in logs
2. Verify API key validity
3. Check OpenSea status page: https://status.opensea.io

**Resolution:**
- Reduce request rate if hitting 429 errors
- Renew API key if authentication fails
- Skip invalid orders gracefully

---

### 4. Cancel Order Failed

**Error Pattern:**
```
[ERROR][system][CancelOpenseaBid][error] Failed to cancel order
```

**Severity:** High

**Source:** `src/index.ts` (multiple cancel functions)

**Root Causes:**
- Upstream wallet lookup failure (most common)
- Order already cancelled on marketplace
- Network/API errors

**Troubleshooting:**
1. Check for preceding "Wallet not found" errors
2. Verify order status on OpenSea directly
3. Check network connectivity to OpenSea API

---

### 5. Worker Job Failed

**Error Pattern:**
```
[ERROR][system][Worker][error] Worker 1 - Task <taskId> - Job CANCEL_OPENSEA_BID-... failed
```

**Severity:** High

**Source:** BullMQ worker in `src/index.ts:2486`

**Root Causes:**
- Downstream error from job handler (wallet not found, API error, etc.)
- Job timeout
- Worker crash/restart

**Troubleshooting:**
1. Look for preceding errors in the same task context
2. Check BullMQ queue status:
   ```javascript
   // Redis CLI
   LLEN bull:cancel-opensea-bid:wait
   LLEN bull:cancel-opensea-bid:failed
   ```

---

## Error Prevention Checklist

### Before Starting a Task
- [ ] Verify wallet exists and has valid private key
- [ ] Confirm wallet has sufficient balance
- [ ] Check marketplace API connectivity

### During Operation
- [ ] Monitor error rates in logs
- [ ] Watch for cascade error patterns
- [ ] Check database connection health

### After Errors Occur
- [ ] Identify root cause (usually first error in cascade)
- [ ] Stop affected tasks if necessary
- [ ] Clear failed jobs from queue
- [ ] Restart with corrected configuration

---

## Error Metrics

### Log Analysis Commands

**Count errors by type:**
```bash
grep '^\[ERROR\]' logs.txt | sed 's/0x[a-fA-F0-9]\{40,\}/0x.../g' | sed 's/[a-f0-9]\{24\}/.../g' | sort | uniq -c | sort -rn
```

**Find affected tasks:**
```bash
grep 'for task' logs.txt | grep -oP 'task \K[a-f0-9]{24}' | sort | uniq -c | sort -rn
```

**Count errors per hour:**
```bash
grep '^\[ERROR\]' logs.txt | cut -d'T' -f1-2 | uniq -c
```

---

## Fix Status

| Issue | Status | Fix Location | PR/Commit |
|-------|--------|--------------|-----------|
| Wallet not found cascade | Fixed | `src/index.ts:18628` | - |
| Pre-validation before bulk cancel | Fixed | `src/index.ts` | - |
| Task start wallet check | Fixed | `src/index.ts` (startTask) | - |
| Database retry logic | Fixed | `src/index.ts:18845` | - |

---

## Related Documentation

- [CHANGELOG.md](../CHANGELOG.md) - Version history and fixes
- [Wallet Cache](../src/utils/wallet-cache.ts) - Wallet caching implementation
- [Logger](../src/utils/logger.ts) - Error logging system
