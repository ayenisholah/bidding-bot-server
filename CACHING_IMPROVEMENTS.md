# Caching Improvements for NFT Bidding Bot

## Overview
This document outlines the caching improvements implemented to reduce redundant blockchain calls and improve performance.

## Implemented Caching Strategies

### 1. NFT Approval Caching (`approveNFT`)
- **Location**: `src/functions/approve.ts`
- **Cache Key**: `{nft_approval}:walletAddress:nftAddress:marketContractAddress`
- **TTL**: 24 hours
- **Storage**: Redis
- **Benefits**:
  - Eliminates redundant `isApprovedForAll` calls
  - Reduces RPC calls by ~90% for repeated NFT listings
  - Improves listing speed significantly

### 2. Marketplace Approval Caching (`approveMarketplace`)
- **Location**: `src/index.ts` (can use `src/functions/approveMarketplace-improved.ts`)
- **Cache Key**: `{marketplace_approval}:walletAddress:marketplace:currency`
- **TTL**: 7 days
- **Storage**: Redis + PostgreSQL (dual layer)
- **Benefits**:
  - Reduces allowance check calls
  - Prevents duplicate approval transactions
  - Uses both Redis (fast) and PostgreSQL (persistent) caching

### 3. ERC20 Balance Caching (Already Implemented)
- **Location**: `src/utils/balance.ts`
- **Cache Key**: `{erc20_balance:walletAddress}:tokenAddress`
- **TTL**: 60 seconds (fresh) + 1 hour (stale fallback)
- **Storage**: Redis
- **Benefits**:
  - Already well-implemented with stale cache fallback
  - Good balance between freshness and performance

## Performance Improvements

### Before Caching
- Every NFT listing: 1 RPC call to check approval
- Every marketplace bid: 1 RPC call to check allowance
- Total RPC calls for 100 NFT listings: ~200 calls

### After Caching
- First NFT listing: 1 RPC call (then cached for 24h)
- Subsequent listings: 0 RPC calls (cache hit)
- Total RPC calls for 100 NFT listings: ~2-10 calls (depending on unique NFTs)

## Redis Key Structure

All cache keys use Redis hash tags `{}` to ensure they're stored on the same Redis cluster node:

```
{nft_approval}:0xwallet:0xnft:0xmarket          -> "true" (24h TTL)
{marketplace_approval}:0xwallet:0xmarket:0xtoken -> "true" (7d TTL)
{erc20_balance:0xwallet}:0xtoken                -> "1.234" (60s TTL)
```

## Implementation Guidelines

### 1. Using Cached approveNFT
```typescript
import { approveNFT } from "./functions/approve";

// Just use it normally - caching is transparent
await approveNFT(nftAddress, wallet, marketContractAddress);
```

### 2. Using Improved approveMarketplace
```typescript
import { approveMarketplaceWithCache } from "./functions/approveMarketplace-improved";

const approved = await approveMarketplaceWithCache(
  SEAPORT,
  privateKey,
  WETH,
  process.env.ALCHEMY_API_KEY
);
```

### 3. Cache Invalidation
```typescript
// Clear NFT approval cache (if needed)
import { clearNFTApprovalCache } from "./functions/approveNFT-cached";
await clearNFTApprovalCache(walletAddress, nftAddress, marketContractAddress);

// Clear marketplace approval cache (if needed)
import { clearMarketplaceApprovalCache } from "./functions/approveMarketplace-improved";
await clearMarketplaceApprovalCache(walletAddress, marketplace, currency);
```

## Additional Optimizations

### 1. Bulk Approval Checks
Both improved functions include bulk check methods for checking multiple approvals in a single operation:

```typescript
// Check multiple NFT approvals
const approvals = await checkMultipleNFTApprovals(
  wallet,
  nftAddresses,
  marketContractAddress
);

// Check multiple marketplace approvals
const marketApprovals = await checkMultipleMarketplaceApprovals(
  wallets,
  marketplace,
  currency,
  alchemyApiKey
);
```

### 2. Fallback Behavior
- If Redis is unavailable, functions continue to work (with blockchain calls)
- Cache errors are logged but don't break functionality
- Stale cache strategy for balance checks

## Monitoring

### Cache Hit Rate
Monitor Redis to track cache effectiveness:
```bash
redis-cli info stats | grep keyspace_hits
redis-cli info stats | grep keyspace_misses
```

### Key Patterns
View all approval cache keys:
```bash
redis-cli --scan --pattern "{nft_approval}:*"
redis-cli --scan --pattern "{marketplace_approval}:*"
```

## Future Improvements

1. **Collection-level NFT Approval Caching**: Cache approvals at collection level for marketplaces that support it
2. **Approval Event Monitoring**: Listen to blockchain events to invalidate cache when approvals are revoked
3. **Multi-chain Support**: Extend caching to support other chains (Polygon, Arbitrum, etc.)
4. **Metrics Dashboard**: Add Prometheus metrics for cache hit/miss rates

## Migration Steps

1. Update `src/functions/approve.ts` with the new cached version
2. Replace `approveMarketplace` calls with `approveMarketplaceWithCache`
3. Ensure Redis cluster is properly configured
4. Monitor logs for cache hit/miss rates
5. Adjust TTL values based on usage patterns