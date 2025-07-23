# Blur Rate Limit Fix Implementation

## Problem
The system was getting stuck with hundreds of waiting jobs due to Blur API rate limits (40 bids per 10 seconds). Jobs were failing with "Blur bidding rate limit exceeded" errors and clogging the queue.

## Solution Implemented

### 1. BlurRateLimiter Class (`src/utils/blur-rate-limiter.ts`)
- Tracks rate limits per wallet address
- Enforces 40 requests per 10-second window
- Implements backoff when rate limited
- Provides status checking and monitoring
- Event-based notifications for rate limit events

### 2. Pre-emptive Rate Limiting
Before adding Blur jobs to the queue, the system now:
- Checks if the wallet can make a request
- If rate limited, adds the job with a delay
- Prevents jobs from failing due to rate limits

### 3. Reactive Rate Limiting
When a Blur API request fails with rate limit error:
- The error is caught and standardized
- The wallet is marked as rate limited
- The job is retried with exponential backoff
- Proper delay is added based on retry-after header

### 4. Worker Error Handling
Updated worker to handle rate limit errors:
- Recognizes `RATE_LIMITED` error code
- Throws error with proper retry delay
- Uses BullMQ's built-in retry mechanism with backoff

### 5. API Endpoints
Added monitoring endpoints:
- `GET /api/blur-rate-limits` - View all wallet rate limit statuses
- `GET /api/blur-rate-limits/:wallet` - View specific wallet status
- `POST /api/blur-rate-limits/:wallet/reset` - Reset rate limit for testing
- Health endpoint now includes Blur rate limit summary

### 6. Job Options Update
Modified job creation to include:
- 3 retry attempts
- Exponential backoff starting at 8 seconds
- Proper delay handling for rate-limited jobs

## Key Changes

### Modified Files:
1. `src/utils/blur-rate-limiter.ts` - New rate limiter class
2. `src/marketplace/blur/bid.ts` - Integrated rate limiting
3. `src/index.ts` - Updated job processing and error handling
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