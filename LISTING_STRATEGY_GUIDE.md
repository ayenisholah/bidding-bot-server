# NFT Listing Strategy Guide

## Overview

The NFT listing system is an automated solution for listing NFTs across multiple marketplaces (OpenSea, Blur, Magic Eden) with support for dynamic pricing, expiration handling, and instant price matching.

## Core Components

### 1. **Listing Scheduler** (`listingScheduler.ts`)
- Uses BullMQ job queue with Redis for reliable scheduling
- Creates repeating jobs based on `loopInterval` (e.g., every 15 minutes)
- Maintains active job tracking to prevent duplicates
- Automatically cancels jobs when listings are stopped

### 2. **Listing Creation** (`list.ts`)
- Handles the actual listing process on each marketplace
- Supports concurrent listing on multiple marketplaces
- Calculates prices based on pricing strategy
- Updates expiration dates after successful listings

### 3. **Expiration Monitor** (`relistExpired.ts`)
- Runs every 5 minutes to check for expired listings
- Only relists on marketplaces where the listing has expired
- Preserves active listings on other marketplaces

### 4. **Price Monitor** (`priceMonitor.ts`)
- Monitors WebSocket events for competitive listings
- Triggers instant price adjustments when undercut
- Respects minimum price and profit constraints

## How It Works

### Initial Listing Flow

1. **User creates a listing** with:
   - Token ID and contract address
   - Selected marketplaces (OpenSea, Blur, Magic Eden)
   - Pricing strategy (Fixed or Dynamic)
   - Loop interval (how often to relist)
   - List duration (how long listings stay active)

2. **Scheduler creates a repeating job**:
   ```javascript
   // Example: List every 15 minutes
   scheduleListing(listing) → BullMQ job with repeat.every = 15 minutes
   ```

3. **Worker processes the job**:
   - Fetches latest listing data
   - Calls `createListing()` which:
     - Gets collection details and creator fees
     - Calculates price for each marketplace
     - Lists concurrently on all selected marketplaces
     - Updates expiration dates

### Pricing Strategies

#### Fixed Pricing
- **GENERAL_LISTING_PRICE**: Same price on all marketplaces
- **MARKETPLACE_LISTING_PRICE**: Different price per marketplace

```javascript
// Example configuration:
{
  pricingStrategy: "FIXED_PRICING",
  listingPriceType: "MARKETPLACE_LISTING_PRICE",
  openseaListingPrice: "1.5",
  blurListingPrice: "1.45", 
  magicEdenListingPrice: "1.48"
}
```

#### Dynamic Pricing (Future Enhancement)
- Based on floor prices, trait floors, or buy price
- Automatically adjusts to market conditions

### Instant Price Matching

When enabled (`instantPriceMatching: true`):

1. **WebSocket monitors listing events**
2. **Detects competitor listings** for same token
3. **Checks if we should undercut**:
   - Competitor price < our price
   - New price > minimum listing price
   - Maintains minimum profit margin
4. **Immediately relists** with adjusted price
5. **Reschedules regular listing job**

### Expiration Handling

The system intelligently handles marketplace-specific expirations:

1. **Tracks expiration per marketplace**:
   ```javascript
   listingExpiration: {
     opensea: "2024-01-15T10:30:00Z",
     blur: "2024-01-15T11:00:00Z",
     magiceden: "2024-01-15T10:45:00Z"
   }
   ```

2. **Checks every 5 minutes** for expired listings
3. **Only relists where expired** - preserves active listings
4. **Updates expiration dates** after successful relisting

## Example Scenarios

### Scenario 1: Basic Fixed Price Listing
```javascript
{
  tokenId: "1234",
  selectedMarketplaces: ["OPENSEA", "BLUR"],
  pricingStrategy: "FIXED_PRICING",
  basePrice: "1.5",
  loopInterval: { value: 30, unit: "minutes" },
  listDuration: { value: 1, unit: "hours" }
}
```
- Lists at 1.5 ETH on both marketplaces
- Relists every 30 minutes
- Each listing expires after 1 hour

### Scenario 2: Competitive Price Matching
```javascript
{
  tokenId: "5678",
  selectedMarketplaces: ["OPENSEA", "BLUR", "MAGICEDEN"],
  instantPriceMatching: true,
  basePrice: "2.0",
  underCutMargin: "0.01",
  minListingPrice: "1.5",
  minimumProfit: "0.5"
}
```
- Initial listing at 2.0 ETH
- If competitor lists at 1.95 ETH → relists at 1.94 ETH
- Won't go below 1.5 ETH minimum
- Maintains 0.5 ETH profit margin

### Scenario 3: Marketplace-Specific Pricing
```javascript
{
  listingPriceType: "MARKETPLACE_LISTING_PRICE",
  openseaListingPrice: "1.5",
  blurListingPrice: "1.45",
  magicEdenListingPrice: "1.48"
}
```
- Different prices optimized per marketplace
- Accounts for different fee structures

## Testing the System

1. **Create a test listing**:
   - Start with a single marketplace
   - Use a longer loop interval (e.g., 1 hour)
   - Enable logging to monitor behavior

2. **Monitor the logs**:
   - Check for successful listing creation
   - Verify expiration checks are running
   - Watch for WebSocket price updates

3. **Test instant price matching**:
   - Enable `instantPriceMatching`
   - Have someone list the same token at a lower price
   - Verify automatic price adjustment

4. **Test expiration handling**:
   - Set short list duration (e.g., 15 minutes)
   - Wait for expiration
   - Verify only expired marketplaces are relisted

## Common Issues and Solutions

1. **Listings not creating**:
   - Check wallet has sufficient balance
   - Verify marketplace approvals
   - Check Redis is running

2. **Instant price matching not working**:
   - Ensure WebSocket connection is active
   - Verify `instantPriceMatching` is enabled
   - Check minimum price constraints

3. **Duplicate listings**:
   - System prevents this with job tracking
   - Check `activeJobs` map in scheduler

4. **Expiration monitor not running**:
   - Verify `initializeExpirationMonitor()` is called
   - Check logs for "Checking for expired listings"

## Architecture Benefits

1. **Reliability**: BullMQ ensures jobs are processed even after restarts
2. **Efficiency**: Concurrent marketplace operations save time
3. **Flexibility**: Per-marketplace configurations
4. **Intelligence**: Only relists where needed
5. **Competitiveness**: Real-time price adjustments