# OpenSea Processing Functions Analysis

This document provides a comprehensive list of all functions called within the four main OpenSea processing functions in the NFT bidding bot server.

## Overview

The four OpenSea processing functions analyzed are:
1. `processOpenseaScheduledBid` - Handles scheduled bidding operations
2. `processOpenseaCollectionBid` - Processes collection-wide bids
3. `processOpenseaTraitBid` - Manages trait-specific bids
4. `processOpenseaTokenBid` - Handles individual token bids

## Function Call Analysis

### 1. processOpenseaScheduledBid (lines 8294-8593)

This function orchestrates the scheduled bidding process and calls the following functions:

- `activeTasks.get(task._id)` - Retrieves the current task state
- `transformNewTask(filteredTasks)` - Transforms task data format
- `walletCache.getWalletById(currentTask.walletId)` - Gets wallet information
- `decryptPrivateKey(wallets?.encryptedPrivateKey)` - Decrypts wallet private key
- `fetchOpenseaListings(taskId, slug, amount)` - Fetches OpenSea listings
- `transformSelectedTraitsToAttributes(selectedTraits)` - Converts traits to attributes
- `expandAttributes(attributes)` - Expands attribute combinations
- `getTraitCount(traits, attribute)` - Gets trait count information
- `fetchOpenSeaCollectionItems(variable)` - Fetches collection items from OpenSea
- `pQueue.addAll(expandedAttributes.map(...))` - Parallel processing queue
- `logBidError(taskId, type, message, level, marketplace)` - Error logging
- `calculateBidPrice(task, floorPrice, marketplace)` - Price calculations
- `prisma.order.findMany(where)` - Database query for orders
- `queue.addBulk(cancelData)` - Bulk queue operations
- `transformOpenseaTraits(selectedTraits)` - Trait transformation
- `getCollectionDetails(slug, contractAddress)` - Collection metadata
- `processBulkJobs(jobs, immediate)` - Bulk job processing
- `parentJobs.set(parentJobId, jobData)` - Parent job tracking
- `queue.add(jobName, data, options)` - Queue job addition
- `updateTaskIndexes(currentTasks)` - Task index updates

### 2. processOpenseaCollectionBid (lines 8624-8819)

This function handles collection-level bidding with these function calls:

- `activeTasks.get(task._id)` - Current task retrieval
- `walletCache.getWalletById(task.walletId)` - Wallet information
- `decryptPrivateKey(wallets?.encryptedPrivateKey)` - Private key decryption
- `getOpenseaFloor(currentTask.contract.slug)` - Floor price data
- `calculateBidPrice(currentTask, floor_price, "opensea")` - Bid price calculation
- `stopOption(currentTask, 'opensea', Number(data), false)` - Stop condition check
- `getExpiry(currentTask.bidDuration)` - Expiration calculation
- `roundToValidPrecision(offerPriceEth)` - Price precision rounding
- `prisma.order.findMany(where)` - Order database queries
- `queue.addBulk(cancelData)` - Bulk cancellation
- `walletCache.getAllWallets()` - All wallets retrieval
- `fetchOpenseaOffers(type, slug, contractAddress, params)` - Offer fetching
- `calculateOutbidMargin(highestBid, currency)` - Outbid margin calculation
- `getIncrementedBidCount(marketplace, slug, taskId)` - Bid counting
- `logBidError(taskId, type, message, level, marketplace)` - Error logging
- `bidOnOpensea(...)` - Main bidding execution

### 3. processOpenseaTraitBid (lines 9199-9423)

This function processes trait-specific bids using:

- `transformOpenseaTraits(selectedTraits)` - Trait transformation
- `activeTasks.get(_id)` - Task state retrieval
- `getOpenseaFloor(slug)` - Floor price fetching
- `calculateBidPrice(currentTask, floor_price, "opensea")` - Price calculation
- `getExpiry(currentTask.bidDuration)` - Expiration time
- `openseaTraitFloor(slug, {traitType, values})` - Trait floor price
- `stopOption(currentTask, 'opensea', floor_price, false, traitFloor, identifier)` - Stop check
- `prisma.order.findMany(where)` - Order queries
- `queue.addBulk(cancelData)` - Bulk operations
- `roundToValidPrecision(offerPriceEth)` - Price rounding
- `fetchOpenseaOffers('TRAIT', slug, contractAddress, trait)` - Trait offers
- `walletCache.getAllWallets()` - Wallet list
- `calculateOutbidMargin(highestBid, currency)` - Margin calculation
- `getIncrementedBidCount(OPENSEA, slug, _id)` - Bid increment
- `logBidError(_id, type, message, level, marketplace)` - Error logging
- `bidOnOpensea(...)` - Bid execution

### 4. processOpenseaTokenBid (lines 9425-9621)

This function handles individual token bidding with:

- `tradingDisabledCache.isDisabled(contractAddress, tokenId, 'opensea')` - Trading status check
- `logBidError(_id, type, message, level, marketplace)` - Error logging
- `activeTasks.get(_id)` - Task retrieval
- `getOpenseaFloor(slug)` - Floor price
- `calculateBidPrice(currentTask, floor_price, "opensea")` - Price calculation
- `getExpiry(currentTask.bidDuration)` - Expiration
- `prisma.order.findMany(where)` - Order queries
- `stopOption(currentTask, 'opensea', floor_price, false)` - Stop condition
- `queue.addBulk(cancelData)` - Bulk cancellation
- `walletCache.getAllWallets()` - All wallets
- `fetchItemOffers(contractAddress, tokenId)` - Token offers
- `calculateOutbidMargin(highestBid, currency)` - Outbid margin
- `getIncrementedBidCount(OPENSEA, slug, _id)` - Bid counting
- `bidOnOpensea(...)` - Bid execution

## Common Functions Across Processors

### Core Functions Used by All:
- **Task Management**: `activeTasks.get()`, `logBidError()`
- **Wallet Operations**: `walletCache.getWalletById()`, `walletCache.getAllWallets()`, `decryptPrivateKey()`
- **Price Operations**: `getOpenseaFloor()`, `calculateBidPrice()`, `calculateOutbidMargin()`
- **Database**: `prisma.order.findMany()`
- **Queue Management**: `queue.addBulk()`, `getIncrementedBidCount()`
- **Bidding**: `bidOnOpensea()`

### Specialized Functions:
- **Scheduled Bid Only**: `fetchOpenseaListings()`, `transformSelectedTraitsToAttributes()`, `expandAttributes()`, `fetchOpenSeaCollectionItems()`, `getCollectionDetails()`
- **Collection/Trait/Token**: `stopOption()`, `getExpiry()`, `fetchOpenseaOffers()` or `fetchItemOffers()`
- **Trait Bid Only**: `openseaTraitFloor()`, `transformOpenseaTraits()`
- **Token Bid Only**: `tradingDisabledCache.isDisabled()`

## Function Categories

### 1. Task & State Management
- `activeTasks.get()`
- `updateTaskIndexes()`
- `parentJobs.set()`

### 2. Wallet & Security
- `walletCache.getWalletById()`
- `walletCache.getAllWallets()`
- `decryptPrivateKey()`

### 3. Market Data
- `getOpenseaFloor()`
- `fetchOpenseaListings()`
- `fetchOpenseaOffers()`
- `fetchItemOffers()`
- `openseaTraitFloor()`
- `getCollectionDetails()`

### 4. Price Calculations
- `calculateBidPrice()`
- `calculateOutbidMargin()`
- `roundToValidPrecision()`

### 5. Database Operations
- `prisma.order.findMany()`

### 6. Queue Management
- `queue.add()`
- `queue.addBulk()`
- `processBulkJobs()`
- `pQueue.addAll()`

### 7. Bid Control
- `stopOption()`
- `getExpiry()`
- `getIncrementedBidCount()`

### 8. Trait Processing
- `transformNewTask()`
- `transformSelectedTraitsToAttributes()`
- `expandAttributes()`
- `getTraitCount()`
- `transformOpenseaTraits()`

### 9. Caching
- `tradingDisabledCache.isDisabled()`

### 10. Logging & Monitoring
- `logBidError()`

### 11. Core Bidding
- `bidOnOpensea()`

## Notes

- The `processOpenseaScheduledBid` function is the most complex, handling the initial scheduling and routing to specific bid types
- All functions implement similar patterns for wallet management, price calculation, and order cancellation
- The functions use a combination of database queries, external API calls, and queue management for reliable bid processing
- Error handling and logging are consistent across all processors

## OpenSea Bid Module Functions (src/marketplace/opensea/bid.ts)

This section documents all functions defined in the OpenSea bid module that handles the low-level marketplace interactions.

### Exported Functions

1. **`bidOnOpensea()`** (lines 245-496)
   - Main function for creating OpenSea bids
   - Handles both collection and token offers
   - Validates wallet balance before bidding
   - Signs and submits offers to OpenSea API

2. **`cancelOrder()`** (lines 635-659)
   - Cancels an existing OpenSea order
   - Uses offerer signature for cancellation
   - Decrements bid count after successful cancellation

3. **`fetchItemOffers()`** (lines 767-951)
   - Fetches offers for a specific NFT token
   - Aggregates data from multiple sources (OpenSea GraphQL, Magic Eden)
   - Returns top 2 offers sorted by USD value

4. **`fetchOpenseaOffers()`** (lines 953-1023)
   - Fetches offers based on type (COLLECTION, TRAIT, TOKEN)
   - Filters for OpenSea-specific offers
   - Returns sorted offers by USD value

5. **`openseaTraitFloor()`** (lines 1025-1056)
   - Gets floor price for specific trait combinations
   - Uses OpenSea GraphQL API
   - Returns the lowest listed price for trait

6. **`fetchOpenseaListings()`** (lines 1058-1089)
   - Fetches active listings for a collection
   - Handles pagination to get required number of listings
   - Returns unique token IDs

7. **`cancelOpenseaRequest()`** (lines 1246-1302)
   - Cancels active HTTP requests for a task
   - Manages AbortController instances
   - Returns success status and cancelled count

### Internal Helper Functions

1. **`extractOpenseaErrorMessage()`** (lines 17-52)
   - Extracts meaningful error messages from OpenSea API responses
   - Handles various error formats (arrays, single messages, details)

2. **`getWallet()`** (lines 77-82)
   - Caches wallet instances to avoid recreation
   - Returns cached or new wallet instance

3. **`buildItemOffer()`** (lines 194-233)
   - Builds offer parameters for individual NFT tokens
   - Constructs consideration items and timing parameters

4. **`submitOfferToOpensea()`** (lines 502-566)
   - Posts constructed offer to OpenSea API
   - Handles response and stores order in database
   - Logs success messages

5. **`buildOffer()`** (lines 572-633)
   - Builds collection/trait offers with request deduplication
   - Caches promises to prevent duplicate API calls
   - Manages AbortController for cancellation

6. **`signCancelOrder()`** (lines 661-682)
   - Signs cancellation order with wallet
   - Uses EIP-712 typed data signing

7. **`signOffer()`** (lines 684-686)
   - Signs offer data with wallet
   - Wrapper for typed data signing

8. **`getOffer()`** (lines 688-698)
   - Constructs offer item array for ERC20 tokens
   - Formats price and token information

9. **`postItemOffer()`** (lines 700-732)
   - Posts signed item offer to OpenSea
   - Handles errors and logging

10. **`getItemConsideration()`** (lines 734-745)
    - Creates consideration array for item offers
    - Calls getItemTokenConsideration

11. **`getSalt()`** (lines 747-749)
    - Generates random salt for order uniqueness

12. **`getItemTokenConsideration()`** (lines 751-765)
    - Creates token consideration item
    - Specifies NFT transfer details

13. **`addActiveRequest()`** (lines 1304-1309)
    - Manages active request controllers
    - Adds controller to task-specific array

### Key Dependencies Used

#### External Libraries
- `ethers` - Ethereum wallet and contract interactions
- `axios` - HTTP requests with cancellation support
- `@prisma/client` - Database operations
- `dotenv` - Environment configuration

#### Internal Modules
- `axiosInstance, limiter` - Rate-limited axios instance
- `activeTasks, API_KEY, trackBidRate` - Global state and tracking
- `USDC, WETH` - Currency constants
- `fetchERC20Balance` - Balance checking utility
- `logger` - Structured logging
- `InsufficientBalanceError` - Custom error types

### Constants and Configuration

- **Ethereum Addresses**:
  - `OPENSEA_ITEM_ZONE` - Zone for item offers
  - `OPENSEA_COLLECTION_ZONE` - Zone for collection offers
  - `OPENSEA_FEE_ADDRESS` - OpenSea fee recipient
  - `SEAPORT_1_6` - Seaport contract address

- **Fee Configuration**:
  - `OPENSEA_FEE_BIGINT = 50n` - 0.5% fee
  - `DIVIDER = 10000n` - Basis points divider

- **Caching**:
  - `walletCache` - Wallet instance cache
  - `buildOfferCache` - Request deduplication cache
  - `CACHE_TTL = 5000` - 5 second cache TTL

### Request Management

The module implements sophisticated request management:
- **Deduplication**: Prevents duplicate API calls using cache
- **Cancellation**: AbortController for request cancellation
- **Rate Limiting**: Uses limiter for API rate limits
- **Error Handling**: Comprehensive error extraction and logging

### Database Operations

- Creates order records with:
  - Collection slug
  - Order hash
  - Expiration time
  - Offer price
  - Identifier (collection/trait/token)
  - Task ID
  - Marketplace

### Type Definitions

The module defines numerous TypeScript interfaces:
- `OpenseaOffer`, `OpenseaOrder`, `OpenseaListingData`
- `IFee`, `IPayload`, `ICriteria`, `IProtocolData`
- `ItemOfferSpecification`, `PartialParameters`
- `GraphQLItemOfferResponse` and related types

### Error Handling Patterns

- Balance validation before bidding
- Task state validation throughout execution
- Comprehensive error logging with context
- Graceful handling of cancellations
- Timeout protection for abort operations