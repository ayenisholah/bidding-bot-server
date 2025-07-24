# Timeout Analysis Implementation

## Overview
I've implemented a comprehensive timeout analysis system to track down the exact root cause of SCHEDULE job timeouts in the NFT bidding bot.

## What Was Implemented

### 1. Timeout Logger (`src/utils/timeout-logger.ts`)
A dedicated logging system that tracks:
- **Job lifecycle phases** - Start to completion with timestamps
- **API call tracking** - Every external API request with duration and status
- **Memory snapshots** - Heap usage at start and end of job
- **Queue statistics** - Job counts (waiting, active, completed, failed)
- **Automatic analysis** - Identifies slow API calls, failed requests, queue congestion

### 2. Axios Request Interceptor (`src/utils/axios-wrapper.ts`)
- Intercepts all HTTP requests/responses made by axios
- Tracks request duration, status codes, and errors
- Associates API calls with specific job IDs
- Logs response sizes to identify large payloads

### 3. Enhanced Job Processing
Updated the worker to use timeout tracking:
- Starts tracking when SCHEDULE jobs begin
- Sets job context for API call association
- Logs comprehensive details on timeout
- Generates detailed analysis files

### 4. Phase Tracking in SCHEDULE Jobs

#### MAGICEDEN_SCHEDULE phases:
1. `MAGICEDEN_SCHEDULE_START` - Job initialization
2. `WALLET_TOKEN_FETCH_START` - Beginning wallet/token fetch
3. `WALLET_TOKEN_FETCH_COMPLETE` - Completed with token count
4. `TRAIT_FETCH_START` - Starting trait data fetch (if applicable)
5. `TRAIT_FETCH_COMPLETE` - Completed with token/skip counts
6. `STALE_TOKEN_CANCEL_START` - Beginning cancellation of old bids
7. `TOKEN_BID_START` - Starting to process token bids
8. `MAGICEDEN_SCHEDULE_COMPLETE` - Job finished successfully
9. `MAGICEDEN_SCHEDULE_ERROR` - Job failed with error

#### OPENSEA_SCHEDULE phases:
1. `OPENSEA_SCHEDULE_START` - Job initialization
2. `COLLECTION_DETAILS_START` - Fetching collection metadata
3. `COLLECTION_DETAILS_COMPLETE` - Got collection details
4. `OPENSEA_LISTINGS_START` - Fetching OpenSea listings
5. `OPENSEA_LISTINGS_COMPLETE` - Got listing count

### 5. Log Analysis Tools
Created `scripts/show-timeout-logs.js` to:
- List all timeout log files
- Show recent log entries
- Display detailed timeout analysis
- Identify root causes

## How to Use

### 1. View Timeout Logs
```bash
npm run timeout:logs
```

This shows:
- Available log files
- Latest log entries
- Most recent timeout analysis
- Slowest API calls
- Possible timeout causes

### 2. Log File Locations
- Main log: `timeout-logs/timeout-analysis-YYYY-MM-DD.log`
- Analysis files: `timeout-logs/timeout-{jobId}-{time}.json`

### 3. Real-time Monitoring
When a job times out, you'll see:
```
[Timeout] SCHEDULE job 3019 timed out - will retry indefinitely
[TIMEOUT ANALYSIS] Written to timeout-logs/timeout-3019-142530.json
[TIMEOUT SUMMARY] MAGICEDEN_SCHEDULE - Duration: 180003ms, API Calls: 15, Last Phase: TRAIT_FETCH_START
```

## What This Reveals

The timeout analysis will show:
1. **Which phase takes the longest** - Identifies bottlenecks
2. **Slow API endpoints** - Which external calls are problematic
3. **Failed requests** - API errors that might cause retries
4. **Queue congestion** - If too many jobs are competing
5. **Memory usage** - If memory pressure is causing slowdowns

## Example Analysis Output
```json
{
  "summary": {
    "jobId": "3019",
    "jobName": "MAGICEDEN_SCHEDULE",
    "contractSlug": "good-vibes-club",
    "totalDuration": 180003,
    "lastPhase": "TRAIT_FETCH_START"
  },
  "apiCalls": {
    "total": 45,
    "slowest": [
      {
        "endpoint": "https://api.opensea.io/v2/collection/good-vibes-club/items",
        "method": "GET",
        "duration": 15234,
        "status": 200
      }
    ]
  },
  "possibleCauses": [
    "5 API calls took over 10 seconds",
    "Phase 'TRAIT_FETCH_START' took 165000ms"
  ]
}
```

## Next Steps

1. **Run the server** and wait for timeouts to occur
2. **Check logs** with `npm run timeout:logs`
3. **Analyze patterns** to identify:
   - Specific API endpoints that are slow
   - Collections that consistently timeout
   - Phases where jobs get stuck
4. **Share the analysis files** to diagnose the root cause

The system will automatically generate detailed reports for every timeout, making it easy to identify exactly where the 180-second timeout is being consumed.