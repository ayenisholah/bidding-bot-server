# NFT Trading Bot - Complete Bidding System Documentation

## Table of Contents
1. [System Overview](#system-overview)
2. [Scheduled Bidding](#scheduled-bidding)
3. [Counterbid System](#counterbid-system)
4. [Marketplace-Specific Bidding](#marketplace-specific-bidding)
5. [Advanced Features](#advanced-features)
6. [Performance & Optimization](#performance--optimization)

---

## System Overview

The NFT Trading Bot is a sophisticated automated bidding system that operates across three major NFT marketplaces: OpenSea, Magic Eden, and Blur. It provides scheduled bidding, instant counterbidding, and comprehensive task management.

### Key Components

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   WebSocket     │────▶│   BullMQ Queue   │────▶│  Marketplace    │
│   Messages      │     │   (Redis-backed) │     │   Adapters      │
└─────────────────┘     └──────────────────┘     └─────────────────┘
         │                       │                         │
         ▼                       ▼                         ▼
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Counterbid     │     │  Worker Pool     │     │  External APIs  │
│  Detection      │     │  (Dynamic)       │     │  (OS/ME/Blur)   │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

---

## Scheduled Bidding

### How It Works

1. **Task Creation**
   - Tasks are created via WebSocket with bidding configuration
   - Each task specifies marketplaces, bid amount, duration, and loop interval
   - Tasks can target collections, specific tokens, or traits

2. **Scheduling Process**
   ```
   Task Created → Schedule Jobs → Loop Timer → Execute Bid → Reschedule
   ```

3. **Timing Configuration**
   - **Loop Interval**: How often the task runs (e.g., every 5 minutes)
   - **Bid Duration**: How long bids remain active (e.g., 24 hours)
   - **Supported Units**: minutes, hours, days

### Example Task Configuration
```javascript
{
  bidType: "collection",
  loopInterval: { value: 5, unit: "minutes" },
  bidDuration: { value: 24, unit: "hours" },
  bidPrice: 0.1,
  currency: "ETH",
  marketplaces: ["opensea", "magiceden", "blur"]
}
```

### Job Queue Processing

1. **Queue Architecture**
   - Single Redis-backed queue named `BIDDING_BOT`
   - Worker count: `Math.max(2, CPU_cores * 0.75)`
   - Concurrency per worker: `RATE_LIMIT / WORKER_COUNT`

2. **Job Types**
   - `OPENSEA_SCHEDULE` - Schedules OpenSea bidding
   - `MAGICEDEN_SCHEDULE` - Schedules Magic Eden bidding
   - `BLUR_SCHEDULE` - Schedules Blur bidding
   - Each creates specific bid jobs when executed

3. **Priority System**
   - Regular bids: Priority 2
   - Counterbids: Priority 3 (higher)
   - LIFO processing for counterbids

---

## Counterbid System

### Detection Mechanism

1. **WebSocket Connection**
   - Persistent connection to `wss://nfttools-ws-proxy.nfttools.io`
   - Monitors real-time marketplace events
   - Filters for `BID_CREATED` events

2. **Message Processing Flow**
   ```
   WebSocket Event → Parse Message → Identify Outbid → Match Tasks → Execute Counterbid
   ```

### Counterbid Logic

#### OpenSea Dynamic Margins
```
< 0.1 ETH:     +0.0001 ETH
0.1-1 ETH:     +0.001 ETH
> 1 ETH:       +0.01 ETH
USDC bids:     +0.1 USDC
```

#### Magic Eden & Blur
- Magic Eden: Default +0.0001 ETH (configurable)
- Blur: Default +0.01 ETH (configurable)

### Instant Response Features

1. **Performance Optimizations**
   - Pre-cached wallet keys (5-minute TTL)
   - Collection data caching (30-second TTL)
   - Parallel marketplace processing
   - Processing time: 1-15ms (was 200-1000ms)

2. **Circuit Breaker Protection**
   - Opens after 5 consecutive failures
   - 1-minute timeout before retries
   - 5-minute reset time
   - Per-task-per-marketplace granularity

### Important: No Auto-Cancellation
The system does NOT automatically cancel previous bids before counterbidding. This means:
- Multiple active bids can exist for the same collection
- Risk of winning multiple NFTs
- Manual management or `cancelMaxWhenOutbid` option required

---

## Marketplace-Specific Bidding

### OpenSea Bidding

1. **Protocol**: Seaport v1.6
2. **Supported Bid Types**:
   - Collection offers (zone: `0x004C00500000aD104D7DBd00e3ae0A5C00560C00`)
   - Token offers (zone: `0x000056f7000000ece9003ca63978907a00ffd100`)
   - Trait-based offers

3. **Process Flow**:
   ```
   Validate Task → Check Balance → Build Offer → Sign EIP-712 → Submit to API
   ```

4. **Key Details**:
   - OpenSea Fee: 0.5% (50 basis points)
   - Supports WETH and USDC
   - Creator royalties included

### Magic Eden Bidding

1. **Protocols**:
   - Collection: `payment-processor-v2`
   - Token/Trait: `seaport-v1.6`

2. **Unique Features**:
   - Automated royalties support
   - Off-chain cancellation
   - Batch processing for efficiency

3. **Process Flow**:
   ```
   Get Bid Data → Extract Sign Info → Sign with Wallet → Submit Order
   ```

### Blur Bidding

1. **Authentication Required**:
   - Challenge/response flow
   - Token cached for 5 minutes
   - Distributed lock for auth operations

2. **Currency**: BETH (Blur ETH)
   - Minimum bid: 0.01 BETH
   - Pool address: `0x0000000000A39bb272e79075ade125fd351887Ac`

3. **Rate Limiting**:
   - 40 requests per window
   - Automatic throttling when < 10 requests remain
   - Per-wallet tracking

---

## Advanced Features

### Trait-Based Bidding

Supports complex trait combinations:
```javascript
{
  bidType: "traits",
  traits: '[{"type":"Background","value":"Blue"},{"type":"Eyes","value":"Laser"}]',
  operator: "AND" // or "OR"
}
```

### Price Strategies

1. **Fixed Price**: Bid exact amount
2. **Percentage-Based**: Bid % of floor price
3. **Min/Max Boundaries**: Respect price limits
4. **Dynamic Decimal Rounding**:
   - < 1: 4 decimals
   - 1-10: 3 decimals
   - 10-100: 2 decimals
   - > 100: 2 decimals

### Task Management

1. **Status Tracking**:
   - `activeTasks`: Currently running tasks
   - `taskStatus`: Task state (running/stopped)
   - `bidStats`: Bid count per task

2. **Balance Management**:
   - Real-time balance checking
   - Multi-wallet support
   - Currency-specific validation

### Error Handling

1. **Categorized Errors**:
   - WebSocket errors
   - Marketplace-specific errors
   - Database errors
   - Blockchain errors

2. **Recovery Strategies**:
   - Exponential backoff (5 attempts)
   - Request deduplication (5-second cache)
   - Abort controllers for cleanup

---

## Performance & Optimization

### System Scaling

1. **Worker Pool**:
   - Dynamic scaling based on CPU cores
   - Min: 2 workers
   - Max: CPU cores * 0.75

2. **Queue Management**:
   - Drain threshold: 5000 active jobs
   - Memory threshold: 75%
   - Automatic pausing/resuming

### Caching Strategy

1. **Redis Caching**:
   - Collection data: 30-second TTL
   - Auth tokens: 5-minute TTL
   - Distributed locks for coordination

2. **In-Memory Caching**:
   - Wallet instances
   - Request deduplication
   - Bounded caches (max 1000 items)

### Resource Optimization

1. **Connection Pooling**:
   - Separate Redis connections for queue/workers
   - RPC provider connection reuse
   - WebSocket connection persistence

2. **Batch Processing**:
   - Database writes batched (100ms windows)
   - Bulk job processing (100 jobs/batch)
   - Parallel marketplace operations

### Monitoring & Health

1. **Health Check Endpoint** (`/health`):
   ```json
   {
     "status": "ok",
     "queue": {
       "active": 10,
       "waiting": 5,
       "completed": 100
     },
     "workers": {
       "count": 4,
       "concurrency": 25
     }
   }
   ```

2. **Metrics Tracking**:
   - Queue metrics (active/waiting/completed/failed)
   - Worker performance
   - Memory usage
   - Connection status

---

## Best Practices

1. **Task Configuration**:
   - Set reasonable loop intervals (5+ minutes)
   - Use min/max price boundaries
   - Enable `cancelMaxWhenOutbid` for protection

2. **Performance**:
   - Monitor queue health regularly
   - Adjust worker count based on load
   - Use caching effectively

3. **Security**:
   - Keep passwords in memory only
   - Use encrypted wallet storage
   - Monitor for suspicious activity

4. **Reliability**:
   - Set up monitoring alerts
   - Regular health checks
   - Backup critical data

---

## Troubleshooting

### Common Issues

1. **High Queue Backlog**:
   - Check worker count
   - Verify Redis connection
   - Monitor memory usage

2. **Missed Counterbids**:
   - Verify WebSocket connection
   - Check circuit breaker status
   - Review task configuration

3. **Failed Bids**:
   - Check wallet balances
   - Verify marketplace approvals
   - Review error logs

### Debug Tools

1. **Logging**:
   - Structured logging to Axiom
   - Error categorization
   - Performance metrics

2. **Monitoring**:
   - Queue dashboard
   - WebSocket status
   - Bid statistics

---

This documentation provides a comprehensive understanding of the NFT Trading Bot's bidding system. The system is designed for high-performance, fault-tolerant operation across multiple marketplaces with sophisticated scheduling and counterbidding capabilities.