# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
This is an NFT Trading Bot Server that automates both bidding and listing on NFT marketplaces (OpenSea, Magic Eden, and Blur). It manages bidding tasks, NFT listings with automated expiration handling, monitors collections/traits/tokens, and handles automated order placement and cancellation.

## Development Commands

### Initial Setup
```bash
npm install
```

### Running in Development
```bash
npm run dev  # Starts Redis, resets DB, runs migrations, and starts nodemon
```

### Building for Production
```bash
npm run build  # Cleans DB, runs migrations, generates Prisma client, and compiles TypeScript
npm run start  # Runs production server with memory optimizations
```

### Docker Commands
```bash
npm run dc:up     # Start all services
npm run dc:down   # Stop all services
npm run dc:clean  # Clean containers and volumes
```

### Testing
```bash
npm test  # Runs test.ts with nodemon
```

## Architecture

### Core Components
1. **Express Server** (`src/index.ts`): Main entry point with WebSocket support
2. **Queue System**: Redis + BullMQ for task management with optimized queue handling
3. **Database**: PostgreSQL with Prisma ORM (models: Order, Wallet) + MongoDB for user data
4. **Marketplace Adapters**: Located in `src/marketplace/` for OpenSea, Magic Eden, and Blur
5. **Listing System**: Automated NFT listing with expiration tracking and relisting
6. **Collection Manager**: Advanced collection data fetching with caching and floor price monitoring

### Key Services
- **WebSocket Adapter** (`src/adapter/websocket.ts`): Real-time marketplace monitoring
- **Task Model** (`src/models/task.model.ts`): Manages bidding tasks with queue integration
- **Listing Model** (`src/models/listing.model.ts`): Manages NFT listings with expiration tracking
- **Marketplace Functions** (`src/marketplace/*/`): Platform-specific bidding and listing logic
- **Collection Functions** (`src/functions/collection.ts`): Collection data fetching and caching
- **Listing Scheduler** (`src/functions/listingScheduler.ts`): Automated expiration monitoring and relisting
- **Crypto Utils** (`src/utils/crypto.ts`): Wallet encryption and security

### Database Schema

**PostgreSQL (Prisma)**:
- **Order**: Tracks NFT orders with marketplace, task ID, collection, price, and timestamps
- **Wallet**: Manages wallet addresses and marketplace-specific approval states

**MongoDB Models**:
- **Task**: Bidding configurations with trait filtering and marketplace selection
- **Listing**: NFT listing configurations with marketplace-specific pricing and expiration
- **User**: Authentication and user management
- **Logs**: Bid history and activity tracking

## Key Features

### NFT Listing System
- **Automated Listing**: Cross-marketplace listing with dynamic pricing
- **Expiration Tracking**: Monitors and automatically relists expired listings
- **Loss Protection**: Warns when listing below purchase price
- **Marketplace-Specific Pricing**: Individual configurations per platform

### Advanced Bidding
- **Trait-Based Filtering**: Complex trait combinations with AND/OR operators
- **Floor Price Protection**: Prevents overbidding on WETH collection offers
- **Instant Counterbidding**: Real-time outbid detection and response
- **Multi-marketplace**: Simultaneous bidding across platforms

### Security & Performance
- **Wallet Encryption**: AES-256-GCM with in-memory key management
- **Redis Caching**: Collection data and floor prices with TTL
- **Rate Limiting**: Sophisticated API throttling
- **Queue Optimization**: Prevents duplicate processing

## Important Notes
- Redis is required for queue operations (started automatically in dev mode)
- PostgreSQL database is migrated on each build
- WebSocket connections are managed per marketplace for real-time updates
- Listing scheduler runs every 5 minutes for expiration monitoring

## Claude Code Settings
- NEVER include Claude Code attribution or co-author lines in git commit messages