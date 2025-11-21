# Changelog

All notable changes to the Bidding Bot Server will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Bulletproof Task Update System**: Implemented comprehensive reliability improvements for bid price updates
  - Added MongoDB transaction support in `updateTaskData()` for atomic database-memory synchronization
  - Database updates now persist BEFORE in-memory state changes, ensuring data consistency
  - Implemented rollback mechanism if in-memory updates fail after database commit
  - Added WebSocket message idempotency tracking with 5-minute TTL to prevent duplicate processing
  - Implemented acknowledgment protocol for all WebSocket messages with success/error/duplicate status
  - Added message validation to ensure required fields are present before processing
  - Server now sends acknowledgments for every processed message including duplicates
  - Idempotency cache automatically cleans up entries older than 5 minutes
  - Files modified: `src/index.ts`
  - Resolves: Message loss on WebSocket disconnect, database-memory state divergence, duplicate message processing

### Fixed
- **OpenSea Listing Fee Validation Error**: Fixed OpenSea marketplace listing failures caused by incorrect fee configuration
  - Updated hardcoded OpenSea fee from 50 basis points (0.5%) to 100 basis points (1%)
  - Integrated marketplace fee cache system (`getOpenseaMarketplaceFee()`) for dynamic fee management
  - Fixed hardcoded `BigInt(50)` values in `startAmount` and `endAmount` calculations
  - Files modified: `src/marketplace/opensea/list.ts`
  - Resolves error: "Validation error: An incorrect OpenSea fee has been provided. Expecting 100 basis points, but received 50."

- **API Key Validation and Debugging**: Enhanced API key configuration validation and debugging capabilities
  - Added startup validation to ensure `API_KEY` environment variable is properly loaded
  - Added startup logging showing first 8 characters of loaded API key for verification
  - Added debug logging before OpenSea API calls to track API key presence and request details
  - Removed redundant `dotenv.config()` calls from `collection.ts` to prevent configuration conflicts
  - Files modified: `src/index.ts`, `src/functions/collection.ts`
  - Helps diagnose HTTP 401 Unauthorized errors related to missing or invalid API keys

## [2025-11-20]

### Fixed
- **TypeScript Type Errors**: Fixed type mismatches after MagicEden API return type changes
  - Updated `collection.ts` to parse string stats directly as number
  - Updated `index.ts` to convert `magicEdenFloorPrice` string to number
  - Commit: `c92a573` (2025-11-20)

- **MagicEden Collection Stats API Errors**: Migrated to v4 API endpoint to resolve HTTP 400 errors
  - Updated `fetchMagicEdenCollectionStats` to use `/magiceden/v4/assets` instead of deprecated `/magiceden/v3/rtp/ethereum/tokens/v7`
  - Updated query parameters to match v4 API requirements (`includeCollectionStats`, `includeMarket`, `sortBy`, `sortDir`, `collectionId`)
  - Changed response parsing to use `priceV2.amount.native` instead of `price.amount.decimal`
  - Changed return type to `string` to preserve full precision without floating point loss
  - Files modified: `src/marketplace/magiceden/bid.ts`
  - Commit: `2ab1f80` (2025-11-20)

### Changed
- **Memory Optimization**: Reduced Node.js memory allocation for improved resource management
  - Decreased `max-old-space-size` from 4096MB to 2048MB in nodemon configuration
  - Updated package dependencies with peer dependency markers
  - Files modified: `nodemon.json`, `package-lock.json`
  - Commit: `338a580` (2025-11-20)
