# Always-Running NFT Listing Strategy

## Table of Contents
1. [Overview](#overview)
2. [Raw Strategy](#raw-strategy)
3. [Refined Implementation](#refined-implementation)
4. [Architecture](#architecture)
5. [Configuration](#configuration)
6. [Usage Guide](#usage-guide)
7. [Best Practices](#best-practices)

## Overview

The Always-Running Listing Strategy is a comprehensive automated system that maintains NFT listings across multiple marketplaces (OpenSea, Blur, Magic Eden) 24/7. It features intelligent price matching, automatic expiration handling, and real-time competitive adjustments.

## Raw Strategy

### Core Concepts

1. **Continuous Listing Loop**
   - Listings run indefinitely when `running: true`
   - Re-list at intervals defined by `loopInterval`
   - Check and handle expired listings automatically

2. **Instant Price Matching**
   - Subscribe to WebSocket listing events when `instantPriceMatching: true`
   - Monitor competitor listings in real-time
   - Adjust prices immediately when undercut

3. **Floor Price Monitoring**
   - Check floor prices at each loop interval
   - Compare against current listing price
   - Adjust if necessary to stay competitive

4. **Expiration Management**
   - Track expiration per marketplace
   - Only relist on expired marketplaces
   - Preserve active listings

### Strategy Flow

```
Start Listing
    ↓
Schedule in Queue (loopInterval)
    ↓
Check if Running → No → Skip
    ↓ Yes
Fetch Floor Prices
    ↓
Calculate Optimal Price
    ↓
List on Marketplaces
    ↓
If instantPriceMatching → Subscribe to WebSocket
    ↓
Monitor for Price Changes → Adjust if Undercut
    ↓
Check Expiration (every 5 min) → Relist if Expired
    ↓
Loop back to Schedule
```

## Refined Implementation

### 1. Listing Scheduler (`listingScheduler.ts`)

The scheduler manages the continuous listing loop:

```typescript
interface SchedulerConfig {
  listing: IListing;
  loopInterval: number; // in seconds
  jobId: string;
}
```

**Key Features:**
- BullMQ job queue for reliable scheduling
- Automatic re-scheduling after each run
- Graceful handling of listing stop/start
- Concurrent processing (up to 5 listings)

### 2. Expiration Monitor (`relistExpired.ts`)

Monitors and handles expired listings:

```typescript
interface ExpirationCheck {
  contractAddress: string;
  tokenId: string;
  marketplaces: {
    opensea?: Date;
    blur?: Date;
    magiceden?: Date;
  };
}
```

**Key Features:**
- Runs every 5 minutes
- Checks expiration per marketplace
- Only relists where expired
- Updates expiration timestamps

### 3. Price Monitor (`priceMonitor.ts`)

Real-time competitive pricing:

```typescript
interface PriceUpdate {
  marketplace: string;
  contractAddress: string;
  tokenId: string;
  competitorPrice: number;
  ourPrice: number;
  newPrice: number;
}
```

**Key Features:**
- WebSocket subscription to listing events
- Immediate price adjustments
- Respects `underCutMargin` setting
- Validates against `minListingPrice`

### 4. Floor Price Integration

Enhanced floor price monitoring across all marketplaces:

```typescript
interface FloorPrices {
  opensea: number;
  blur: number;
  magiceden: number;
  lowest: number;
  timestamp: Date;
}
```

## Architecture

### Component Diagram

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│                 │     │                  │     │                 │
│  Listing Model  │────▶│ Listing Scheduler│────▶│  BullMQ Queue   │
│                 │     │                  │     │                 │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                                │                          │
                                ▼                          ▼
                        ┌──────────────────┐     ┌─────────────────┐
                        │                  │     │                 │
                        │ Expiration Check │     │ Listing Worker  │
                        │                  │     │                 │
                        └──────────────────┘     └─────────────────┘
                                                          │
                    ┌──────────────────────────────────────┘
                    ▼                    ▼                    ▼
            ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
            │   OpenSea    │    │     Blur     │    │  Magic Eden  │
            │   Listing    │    │   Listing    │    │   Listing    │
            └──────────────┘    └──────────────┘    └──────────────┘
```

### WebSocket Integration

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│                 │     │                  │     │                 │
│  WebSocket Mgr  │────▶│  Price Monitor   │────▶│ Listing Update  │
│                 │     │                  │     │                 │
└─────────────────┘     └──────────────────┘     └─────────────────┘
         │                                                │
         ▼                                                ▼
┌─────────────────┐                              ┌─────────────────┐
│ Marketplace WS  │                              │  Price Adjust   │
│     Events      │                              │     Logic       │
└─────────────────┘                              └─────────────────┘
```

## Configuration

### Listing Configuration

```typescript
{
  // Core settings
  running: boolean;              // Enable/disable listing
  loopInterval: {               // Re-listing frequency
    value: number;
    unit: "minutes" | "hours" | "days";
  };
  
  // Pricing
  pricingStrategy: "FIXED_PRICING" | "DYNAMIC_PRICING";
  listingPriceType: "GENERAL_LISTING_PRICE" | "MARKETPLACE_LISTING_PRICE";
  basePrice: string;            // Base listing price
  underCutMargin: string;       // Amount to undercut competitors
  minListingPrice: string;      // Minimum acceptable price
  minimumProfit: string;        // Minimum profit margin
  
  // Marketplace-specific prices
  openseaListingPrice: string;
  blurListingPrice: string;
  magicEdenListingPrice: string;
  
  // Features
  instantPriceMatching: boolean; // Enable real-time price matching
  
  // Duration
  listDuration: {               // How long listings stay active
    value: number;
    unit: "minutes" | "hours" | "days";
  };
}
```

### Example Configurations

#### Conservative Strategy
```json
{
  "running": true,
  "loopInterval": { "value": 1, "unit": "hours" },
  "instantPriceMatching": false,
  "pricingStrategy": "FIXED_PRICING",
  "listingPriceType": "GENERAL_LISTING_PRICE",
  "basePrice": "1.5",
  "underCutMargin": "0.01",
  "minListingPrice": "1.0",
  "minimumProfit": "0.2",
  "listDuration": { "value": 7, "unit": "days" }
}
```

#### Aggressive Strategy
```json
{
  "running": true,
  "loopInterval": { "value": 15, "unit": "minutes" },
  "instantPriceMatching": true,
  "pricingStrategy": "DYNAMIC_PRICING",
  "listingPriceType": "MARKETPLACE_LISTING_PRICE",
  "basePrice": "0",
  "underCutMargin": "0.001",
  "minListingPrice": "0.5",
  "minimumProfit": "0.1",
  "openseaListingPrice": "1.2",
  "blurListingPrice": "1.15",
  "magicEdenListingPrice": "1.18",
  "listDuration": { "value": 1, "unit": "hours" }
}
```

## Usage Guide

### Starting a Listing

1. Create listing configuration in database
2. Set `running: true`
3. System automatically schedules and starts listing

### Monitoring

Check listing status:
- `listed`: Currently listed on at least one marketplace
- `listingExpiration`: Expiration times per marketplace
- `lastListedAt`: Last listing time per marketplace
- `finalListingPrice`: Current prices per marketplace

### WebSocket Commands

```javascript
// Start listing
ws.send(JSON.stringify({
  event: 'start-listing',
  listingId: 'listing_id_here'
}));

// Stop listing
ws.send(JSON.stringify({
  event: 'stop-listing',
  listingId: 'listing_id_here'
}));

// Update listing
ws.send(JSON.stringify({
  event: 'update-listing',
  listingId: 'listing_id_here',
  updates: {
    basePrice: '1.25',
    instantPriceMatching: true
  }
}));
```

## Best Practices

### 1. Loop Interval Selection

- **High-value NFTs**: 15-30 minutes
- **Medium-value NFTs**: 1-2 hours
- **Low-value NFTs**: 4-6 hours
- **Collection floor items**: 30 minutes - 1 hour

### 2. Price Matching Settings

- **Competitive collections**: Enable instant price matching
- **Stable collections**: Disable for reduced API calls
- **Undercut margin**: 0.001 - 0.01 ETH typically
- **Minimum price**: Set to break-even + desired profit

### 3. Duration Settings

- **Fast-moving items**: 1-4 hours
- **Standard items**: 1-3 days
- **Rare items**: 7-14 days
- **Match marketplace defaults** for best visibility

### 4. Performance Optimization

- Limit concurrent listings to 10-20 per wallet
- Use marketplace-specific pricing for better control
- Monitor gas costs and adjust frequency accordingly
- Enable instant price matching selectively

### 5. Error Handling

The system includes:
- Automatic retry on failures
- Exponential backoff for rate limits
- Error logging and monitoring
- Graceful degradation (continues other marketplaces if one fails)

### 6. Security Considerations

- Private keys are encrypted at rest
- WebSocket connections use secure protocols
- Rate limiting prevents abuse
- Access control for listing management

## Monitoring & Troubleshooting

### Key Metrics

1. **Listing Success Rate**: Successful listings / Total attempts
2. **Average Time to Relist**: Time between expiration and relisting
3. **Price Adjustment Frequency**: Number of price changes per hour
4. **WebSocket Connection Uptime**: Connection stability percentage

### Common Issues

1. **Listings Not Appearing**
   - Check `running` status
   - Verify wallet has NFT ownership
   - Check marketplace approvals
   - Review error logs

2. **Price Not Updating**
   - Verify `instantPriceMatching` is enabled
   - Check WebSocket connection
   - Ensure `underCutMargin` is reasonable
   - Verify floor price data availability

3. **Excessive Relisting**
   - Increase `loopInterval`
   - Check for marketplace API limits
   - Review expiration settings

4. **WebSocket Disconnections**
   - Check network stability
   - Review firewall settings
   - Monitor WebSocket health endpoint

## Conclusion

The Always-Running Listing Strategy provides a robust, automated solution for maintaining NFT listings across multiple marketplaces. With proper configuration and monitoring, it ensures maximum visibility and competitive pricing for NFT assets while minimizing manual intervention.