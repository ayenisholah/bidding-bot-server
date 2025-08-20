# WebSocket Listing Subscription for Instant Price Matching

## Overview
This document describes the WebSocket subscription mechanism for instant price matching in the NFT listing system.

## How It Works

### 1. Automatic Subscription on Listing Creation
When a listing is created with `instantPriceMatching` enabled, the system automatically subscribes to WebSocket events for that listing:

```typescript
// In src/functions/list.ts
if (freshData.instantPriceMatching && wsManager?.isConnected()) {
  // Subscribe to each selected marketplace
  for (const marketplace of freshData.selectedMarketplaces) {
    await wsManager.subscribe(
      contractSlug,
      contractAddress,
      marketplace.toLowerCase(),
      'listing' // subscription type
    );
  }
}
```

### 2. Subscription Types
- **'bidding'**: For monitoring bid events (used by tasks)
- **'listing'**: For monitoring listing events (used by instant price matching)

### 3. WebSocket Events Monitored
When subscribed to listing events, the system monitors:
- `LISTING_CREATED`: New listings on the marketplace
- Price updates from competitors
- Used to trigger instant price adjustments

### 4. Price Adjustment Flow
1. Competitor creates a listing at a lower price
2. WebSocket receives `LISTING_CREATED` event
3. `handleListingPriceUpdate` in `priceMonitor.ts` processes the event
4. If our listing has `instantPriceMatching` enabled:
   - Calculate new undercut price
   - Queue a job to update our listing price
   - Relist at the new competitive price

### 5. Subscription Management

#### On Startup (listingScheduler.ts)
- All active listings with `instantPriceMatching` are subscribed
- Ensures existing listings maintain their subscriptions

#### On New Listing (list.ts)
- Immediate subscription when listing is created
- Only if `instantPriceMatching` is enabled
- Only if WebSocket is connected

#### On Listing Stop
- Call `unsubscribeListingFromWebSocket` to clean up
- Prevents unnecessary WebSocket traffic

## Configuration

### Listing Configuration
```typescript
interface IListing {
  instantPriceMatching: boolean;  // Enable/disable instant price matching
  underCutMargin: string;         // Amount to undercut competitors (e.g., "0.0001")
  minListingPrice: string;        // Minimum price constraint
  // ... other fields
}
```

### WebSocket Connection
- URL: Configured in `MARKETPLACE_WS_URL`
- Auto-reconnection with exponential backoff
- Persistent connection for real-time updates

## Benefits

1. **Real-time Competition**: Instantly respond to competitor listings
2. **Automated Pricing**: No manual intervention needed
3. **Market Leadership**: Always maintain competitive pricing
4. **Resource Efficient**: Only subscribe to listings that need it

## Troubleshooting

### WebSocket Not Connected
- Check server logs for connection errors
- Verify `MARKETPLACE_WS_URL` is correct
- Ensure network connectivity

### Subscriptions Not Working
- Verify `instantPriceMatching` is enabled on the listing
- Check selected marketplaces match subscription
- Look for subscription confirmation in logs

### Price Not Updating
- Check `underCutMargin` is set correctly
- Verify minimum price constraints
- Check queue processing for `PROCESS_LISTING_UPDATE` jobs

## Related Files
- `src/functions/list.ts`: Handles subscription on listing creation
- `src/functions/priceMonitor.ts`: Processes price update events
- `src/functions/listingScheduler.ts`: Manages listing lifecycle
- `src/utils/websocket-manager.ts`: WebSocket connection management