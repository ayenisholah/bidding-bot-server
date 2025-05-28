# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
This is an NFT Bidding Bot Server that automates bidding on NFT marketplaces (OpenSea, Magic Eden, and Blur). It manages bidding tasks, monitors collections/traits/tokens, and handles automated order placement and cancellation.

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
3. **Database**: SQLite with Prisma ORM (models: Order, Wallet)
4. **Marketplace Adapters**: Located in `src/marketplace/` for OpenSea, Magic Eden, and Blur

### Key Services
- **WebSocket Adapter** (`src/adapter/websocket.ts`): Real-time marketplace monitoring
- **Task Model** (`src/models/task.model.ts`): Manages bidding tasks with queue integration
- **Marketplace Functions** (`src/marketplace/*/bid.ts`): Platform-specific bidding logic

### Database Schema
- **Order**: Tracks NFT orders with marketplace, task ID, collection, price, and timestamps
- **Wallet**: Manages wallet addresses and marketplace-specific approval states

## Important Notes
- Server runs with memory optimizations: `--expose-gc --max-old-space-size=8192`
- Redis is required for queue operations (started automatically in dev mode)
- Database is reset on each `npm run dev` execution
- WebSocket connections are managed per marketplace for real-time updates