# Trait-Based Filtering Implementation for NFT Listing Wars

## Overview
This implementation adds trait-based filtering capabilities to NFT listing wars, allowing listings to compete only with other tokens that have the same trait combination. This is particularly useful for collections where trait rarity significantly impacts value.

## Key Features

### 1. **Token Trait Cache System** (`/src/utils/token-trait-cache.ts`)
- Two-tier caching (memory + Redis) for token trait data
- LRU eviction policy for memory cache
- 7-day Redis TTL for persistent storage
- Efficient trait lookup and filtering capabilities

### 2. **Token Metadata Fetching** (`/src/functions/tokenMetadata.ts`)
- GraphQL integration with OpenSea API
- Fetches token attributes and metadata
- Trait floor price retrieval
- Trait-filtered listing queries
- Batch prefetching support

### 3. **Trait Floor Price Caching** (`/src/utils/opensea-trait-floor.ts`)
- In-memory cache for trait floor prices
- 5-minute TTL for fresh pricing data
- Automatic cache cleanup

### 4. **Updated Price Monitor** (`/src/functions/priceMonitor.ts`)
- Trait filtering in `processListingUpdate()`
- Only processes competitor listings with matching traits
- Uses trait floor prices when applicable

### 5. **Enhanced Listing Logic** (`/src/functions/list.ts`)
- Trait floor price enforcement in `calculateListingPrice()`
- Trait-filtered undercut checking in `shouldSkipListingWithUndercutCheck()`
- Support for trait-based dynamic pricing strategies

### 6. **Listing Scheduler Updates** (`/src/functions/listingScheduler.ts`)
- Pre-fetches trait data on startup for active listings
- Optimizes initial loading performance

## Configuration

Listings can be configured with trait-based pricing using:

```javascript
{
  dynamicPricingBasis: 'HIGHEST_TRAIT_FLOOR' | 'SPECIFIC_TRAIT',
  selectedTrait: {
    trait_type: 'Background',
    value: 'Gray'
  },
  doNotListBelowFloor: true  // Uses trait floor instead of collection floor
}
```

## How It Works

### Listing War with Trait Filtering

1. **WebSocket Event**: When a new listing is detected
2. **Trait Check**: System checks if the competitor token has the required trait
3. **Filter**: Only tokens with matching traits trigger listing war responses
4. **Price Adjustment**: Uses trait floor prices instead of collection floor
5. **Competition**: Only competes with tokens having the same trait combination

### Example Flow
```
Token A (Background: Gray) lists at 10 ETH
↓
System detects listing via WebSocket
↓
Checks all active instant price matching listings
↓
Finds Listing X configured for Background: Gray
↓
Verifies Token A has trait Background: Gray ✓
↓
Triggers listing war response
↓
Lists at 9.99999 ETH (with undercut margin)
```

## API Usage

### Check if Token Has Trait
```typescript
const hasTrait = await tokenHasTrait(
  contractAddress,
  tokenId,
  'Background',
  'Gray'
);
```

### Get Trait Floor Price
```typescript
const floorPrice = await getOpenseaTraitFloor(
  'boredapeyachtclub',
  'Background',
  'Gray'
);
```

### Get Trait-Filtered Listings
```typescript
const listings = await getTraitFilteredListings(
  'boredapeyachtclub',
  'Background',
  'Gray',
  10 // limit
);
```

## Performance Considerations

1. **Caching**: All trait data is cached to minimize API calls
2. **Pre-fetching**: Traits are pre-fetched on startup for active listings
3. **Memory Management**: LRU eviction prevents memory bloat
4. **Batch Operations**: Supports batch fetching for efficiency

## Testing

Use the test script to verify functionality:
```bash
npm run ts-node src/scripts/test-trait-filtering.ts
```

## Future Enhancements

1. **Multi-Trait Support**: Support filtering by multiple trait combinations
2. **Trait Rarity Weighting**: Consider trait rarity in pricing decisions
3. **Dynamic Trait Selection**: Automatically select most profitable traits
4. **Trait Analytics**: Track performance by trait combinations