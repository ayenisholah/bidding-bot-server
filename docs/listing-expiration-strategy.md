# Listing Expiration and Relisting Strategy

## Overview

This document outlines the strategy implemented for tracking listing expirations and automatically relisting NFTs on marketplaces when their listings expire.

## Database Schema Changes

### Added Fields to Listing Model

```typescript
listingExpiration: {
  opensea: { type: Date },
  blur: { type: Date },
  magiceden: { type: Date },
},
lastListedAt: {
  opensea: { type: Date },
  blur: { type: Date },
  magiceden: { type: Date },
},
```

## Core Components

### 1. Expiration Tracking (`/src/functions/relistExpired.ts`)

- **`isListingExpired(expiresAt)`**: Checks if a listing has expired
- **`getListingExpirationStatus(listingId)`**: Returns expiration status for all marketplaces
- **`relistExpiredMarketplaces(listingId)`**: Relists only expired marketplaces for a specific listing
- **`processExpiredListings()`**: Processes all active listings and relists expired ones

### 2. Scheduler (`/src/functions/listingScheduler.ts`)

- **`startListingScheduler(intervalMinutes)`**: Starts automatic processing every N minutes (default: 5)
- **`stopListingScheduler()`**: Stops the scheduler
- **`getSchedulerStatus()`**: Returns current scheduler status
- **`clearExpiredListingPrices()`**: Sets finalListingPrice to 0 for expired marketplaces
- **`getUpcomingExpirations(hoursAhead)`**: Returns listings expiring within specified hours

### 3. Marketplace Integration

Each marketplace listing function now tracks:
- **Expiration time**: Calculated from `durationInSeconds`
- **Last listed time**: Current timestamp when successfully listed
- **Final listing price**: Price at which the item was listed

## Workflow

### 1. Initial Listing
```typescript
// When listing on any marketplace
const expirationTime = new Date(Date.now() + durationInSeconds * 1000);

await Listing.findOneAndUpdate(query, {
  $set: {
    "finalListingPrice.{marketplace}": price,
    "listingExpiration.{marketplace}": expirationTime,
    "lastListedAt.{marketplace}": new Date(),
    listed: true
  }
});
```

### 2. Expiration Detection
```typescript
// Scheduler runs every 5 minutes
function isListingExpired(expiresAt: Date): boolean {
  return new Date() > new Date(expiresAt);
}
```

### 3. Automatic Relisting
```typescript
// Only relist on expired marketplaces
const expiredMarketplaces = statuses
  .filter(status => status.isExpired)
  .map(status => status.marketplace);

// Clear expired prices first
marketplacesToRelist.forEach(marketplace => {
  updateFields[`finalListingPrice.${marketplace}`] = 0;
});

// Then relist with fresh data
await createListing({
  ...originalListingData,
  selectedMarketplaces: expiredMarketplaces
});
```

## API Endpoints

### Manual Controls
- `POST /api/listings/process-expired` - Manually trigger expired listing processing
- `POST /api/listings/:listingId/relist-expired` - Relist specific listing's expired marketplaces

### Monitoring
- `GET /api/listings/upcoming-expirations?hours=24` - View listings expiring soon
- `GET /api/listings/scheduler-status` - Check scheduler status

## Benefits

1. **Automatic Relisting**: No manual intervention needed for expired listings
2. **Marketplace-Specific**: Only relists on expired marketplaces, not all
3. **Price Reset**: Clears expired prices to maintain accurate state
4. **Monitoring**: API endpoints for tracking and manual control
5. **Configurable**: Scheduler interval can be adjusted
6. **Resilient**: Continues running even if individual listings fail

## Configuration

### Scheduler Interval
```typescript
// In src/index.ts
startListingScheduler(5); // Check every 5 minutes
```

### Duration Calculation
```typescript
// In src/functions/list.ts
function convertToSeconds(value: number, unit: string): number {
  switch (unit.toLowerCase()) {
    case 'minutes': return value * 60;
    case 'hours': return value * 60 * 60;
    case 'days': return value * 60 * 60 * 24;
    default: return 900; // 15 minutes default
  }
}
```

## Error Handling

- Individual listing failures don't stop the scheduler
- Database errors are logged but don't crash the system
- API endpoints provide detailed error messages
- Scheduler continues running even after errors

## Monitoring and Debugging

### Logs
- Successful relisting with marketplace and expiration details
- Error logs for failed operations
- Scheduler start/stop events

### Metrics
- Number of listings processed
- Expired marketplaces detected
- Relisting success/failure rates

### Manual Intervention
- API endpoints allow manual triggering
- Scheduler can be stopped/started programmatically
- Individual listings can be processed independently

## Future Enhancements

1. **Price Adjustment**: Implement price adjustments based on market conditions
2. **Retry Logic**: Add exponential backoff for failed relisting attempts
3. **Notification System**: Alert on repeated failures or high expiration rates
4. **Analytics**: Track relisting success rates and market performance
5. **Batch Processing**: Optimize for high-volume listing management