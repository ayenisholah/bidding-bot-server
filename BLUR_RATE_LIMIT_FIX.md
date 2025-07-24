# Blur Rate Limit Fix Implementation

## Problem
The system was getting stuck with hundreds of waiting jobs due to Blur API rate limits (40 bids per 10 seconds). Jobs were failing with "Blur bidding rate limit exceeded" errors and clogging the queue.

**UPDATE**: Fixed header parsing issue - the Blur API returns rate limit headers without the 'x-' prefix (e.g., 'ratelimit-limit' instead of 'x-ratelimit-limit') and returns 400 status code with rate limit errors in a data array instead of 429.

## Solution Implemented

### 1. BlurRateLimiter Class (`src/utils/blur-rate-limiter.ts`)
- Tracks rate limits per wallet address
- Enforces 40 requests per 10-second window
- Implements backoff when rate limited
- Provides status checking and monitoring
- Event-based notifications for rate limit events
- **NEW**: Syncs with actual Blur API response headers for accurate tracking

### 2. Pre-emptive Rate Limiting
Before adding Blur jobs to the queue, the system now:
- Checks if the wallet can make a request using `canMakeRequest()`
- If rate limited, waits for the appropriate time before adding jobs
- Prevents jobs from failing due to rate limits
- Integrated into `processBulkJobs` function for better control

### 3. Reactive Rate Limiting
When a Blur API request fails with rate limit error:
- The error is caught and standardized
- The wallet is marked as rate limited
- Actual rate limit values from response headers are used to update the limiter
- Uses `x-ratelimit-remaining`, `x-ratelimit-reset`, and `x-ratelimit-limit` headers

### 4. API Response Header Integration
The system now extracts and uses actual rate limit information from Blur API responses:
```typescript
const headers = response.headers;
// Try both header formats (with and without x- prefix)
const remaining = parseInt(headers['ratelimit-remaining'] || headers['x-ratelimit-remaining'] || headers['x-ratelimit-remaining-second'] || '40', 10);
const reset = parseInt(headers['ratelimit-reset'] || headers['x-ratelimit-reset'] || '10', 10);
const limit = parseInt(headers['ratelimit-limit'] || headers['x-ratelimit-limit'] || headers['x-ratelimit-limit-second'] || '100', 10);

// Update rate limiter with actual values from API
if (!isNaN(remaining) && !isNaN(reset) && !isNaN(limit)) {
  blurRateLimiter.updateFromHeaders(walletAddress, { remaining, reset, limit });
}
```

**Note**: Blur API returns rate limit headers in multiple formats:
- `ratelimit-limit`, `ratelimit-remaining`, `ratelimit-reset` (primary)
- `x-ratelimit-limit-second`, `x-ratelimit-remaining-second` (secondary)
- Rate limit errors come as 400 status with message in data array

### 5. Progress Tracking Integration
Updated `processBulkJobs` to properly track progress:
- Syncs with `taskStatus` for accurate progress reporting
- Updates `runningPercentage` and `processingPercentage` as jobs are added
- Separate progress tracking for Blur vs non-Blur jobs
- Real-time updates as Blur jobs are processed with delays

### 6. API Endpoints
Added monitoring endpoints:
- `GET /api/blur-rate-limits` - View all wallet rate limit statuses
- `GET /api/blur-rate-limits/:wallet` - View specific wallet status
- `POST /api/blur-rate-limits/:wallet/reset` - Reset rate limit for testing (dev mode or with API key)
- Health endpoint now includes Blur rate limit summary

### 7. Job Options Update
Modified job creation to include:
- 3 retry attempts
- Exponential backoff starting at 8 seconds
- Proper delay handling for rate-limited jobs

## Key Changes

### Modified Files:
1. `src/utils/blur-rate-limiter.ts` - New rate limiter class with header sync
2. `src/marketplace/blur/bid.ts` - Integrated rate limiting with header extraction
3. `src/index.ts` - Updated job processing with progress tracking
4. Removed old `blurRateLimitInfo` global variable

### How It Works:
1. **Prevention**: Check rate limits before creating jobs
2. **Detection**: Catch rate limit errors from Blur API
3. **Recovery**: Add delays and retry with backoff
4. **Monitoring**: Track and report rate limit status

## Benefits
- No more stuck waiting jobs
- Efficient use of Blur API rate limits
- Automatic recovery from rate limit errors
- Better visibility into rate limit status
- Prevents queue congestion

## Testing
Run the test script to verify functionality:
```bash
npx ts-node test-blur-rate-limiter.ts
```

## Monitoring
Check rate limit status:
```bash
# View all wallets
curl http://localhost:3003/api/blur-rate-limits

# View specific wallet
curl http://localhost:3003/api/blur-rate-limits/0xWALLET_ADDRESS

# Reset rate limit (for testing)
curl -X POST http://localhost:3003/api/blur-rate-limits/0xWALLET_ADDRESS/reset
```