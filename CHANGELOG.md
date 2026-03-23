# Changelog

## [Unreleased]

### Fixed
- **Active bid counter stuck at 0 for token bids**: `trackBidRate` decoupled from `itemOrderHash` — now fires on any non-null `postItemOffer` response. Expanded hash extraction to check `order.order_hash`, `order_hash`, `order.orderHash`, `orderHash`. Added warnLog when hash is missing.
  - **Files**: `src/marketplace/opensea/bid.ts`

### Added
- Abstract chain support with Alchemy network configuration (`abstract-mainnet`)
- Chain-specific conduit key for OpenSea bids on Abstract chain
  - Abstract uses conduit key: `0x61159fefdfada89302ed55f8b9e89e2d67d8258712b3a3f89aa88525877f1d5e`
  - Other chains continue using default conduit key

### Changed
- Updated `chains.ts` to use Alchemy for Abstract chain with fallback to public RPC (`https://api.mainnet.abs.xyz`)
- Modified `bid.ts` to dynamically select conduit key based on chain (affects token bids and collection bids)
- Updated Abstract chain marketplace spender to use correct conduit address for WETH approvals
