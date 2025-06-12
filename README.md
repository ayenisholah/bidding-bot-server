# NFTTools Bidding Bot

An automated bidding system for NFT marketplaces (OpenSea, Magic Eden, Blur) with advanced security, real-time monitoring, and intelligent bidding strategies.

## 🚀 Features

- **Multi-Marketplace Support**: Automated bidding on OpenSea, Magic Eden, and Blur
- **Advanced Security**: Password-based encryption for wallet private keys with zero-persistence policy
- **Real-Time Monitoring**: WebSocket connections for instant bid updates and notifications
- **Intelligent Bidding**: Collection offers, token-specific bids, and trait-based targeting
- **Smart Stop Controls**: Automatic task pausing based on floor price changes and purchase limits
- **Outbid Protection**: Automatic counter-bidding with customizable margins
- **Bulk Operations**: Manage multiple tasks and wallets efficiently
- **Docker Deployment**: Platform-specific images with Redis cluster support

## 📋 Table of Contents

- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage Guide](#usage-guide)
- [Development](#development)
- [API Reference](#api-reference)
- [Security](#security)
- [Troubleshooting](#troubleshooting)

## 🏗️ Architecture

### System Components

- **Client**: Next.js 14 web interface (port 3001)
- **Server**: Express API with WebSocket support (port 3003)
- **MongoDB**: User data, tasks, and wallet information
- **SQLite**: Server-side order tracking via Prisma
- **Redis Cluster**: 6-node cluster for queue management and caching
- **BullMQ**: Reliable task processing and job management

### Technology Stack

- **Frontend**: Next.js 14, React, Tailwind CSS, Framer Motion
- **Backend**: Node.js, Express, TypeScript, Prisma
- **Real-time**: Socket.io, WebSocket adapters
- **Queue**: BullMQ with Redis cluster
- **Security**: AES-256-GCM encryption, bcrypt, WebAuthn support

## 🚀 Quick Start

### Using Docker (Recommended)

```bash
# Clone the repository
git clone https://github.com/nfttools/bidding-bot.git
cd bidding-bot

# Set up environment variables
cp .env.example .env
# Edit .env with your configuration

# Start all services
npm run dc:up

# Access the application
# Client: http://localhost:3001
# Server: http://localhost:3003
```

### Using Install Script

```bash
curl -s "https://raw.githubusercontent.com/nfttools/bidding-bot/main/install.sh" | bash
```

## 📦 Installation

### Prerequisites

- Node.js 20.x or higher
- Docker and Docker Compose
- MongoDB (or use Docker container)
- Redis (or use Docker cluster)

### Manual Installation

#### 1. Clone and Install Dependencies

```bash
# Clone repository
git clone https://github.com/nfttools/bidding-bot.git
cd bidding-bot

# Install root dependencies
npm install

# Install client dependencies
cd client && npm install

# Install server dependencies
cd ../server && npm install
```

#### 2. Set Up Databases

```bash
# Start MongoDB
cd client && npm run dc:mongo

# Start Redis cluster
cd ../server && docker compose -f compose.redis.yaml up -d

# Run database migrations
cd server && npx prisma migrate deploy
```

#### 3. Configure Environment

Create `.env` files in both client and server directories:

**Client `.env`:**
```env
MONGODB_URI=mongodb://localhost:27017/BIDDING_BOT
JWT_SECRET=your-secure-jwt-secret
CLIENT_URL=http://localhost:3001/
NEXT_PUBLIC_SERVER_WEBSOCKET=ws://localhost:3003
NEXT_PUBLIC_ALCHEMY_API_KEY=your-alchemy-api-key
```

**Server `.env`:**
```env
PORT=3003
MONGODB_URI=mongodb://localhost:27017/BIDDING_BOT
ALCHEMY_API_KEY=your-alchemy-api-key
REDIS_NODES=[{"host":"localhost","port":6379}]
```

#### 4. Start the Application

```bash
# Terminal 1 - Start server
cd server && npm run dev

# Terminal 2 - Start client
cd client && npm run dev
```

## ⚙️ Configuration

### Environment Variables

#### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `MONGODB_URI` | MongoDB connection string | `mongodb://localhost:27017/BIDDING_BOT` |
| `JWT_SECRET` | Secret for JWT tokens | `your-secure-secret` |
| `ALCHEMY_API_KEY` | Alchemy API key for blockchain data | `your-api-key` |

#### Optional Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | Server port | `3003` |
| `CLIENT_URL` | Client application URL | `http://localhost:3001/` |
| `REDIS_NODES` | Redis cluster configuration | `[{"host":"localhost","port":6379}]` |

### Rate Limiting

Configure rate limits to prevent API throttling:

1. Navigate to Settings in the web interface
2. Enter your desired requests per second (RPS)
3. The system shows your maximum allowed limit
4. Click "Update" to save

## 📖 Usage Guide

### Creating Your First Task

1. **Login/Register**: Create an account or sign in
2. **Add Wallet**: Import or generate a wallet
3. **Create Task**:
   - Click "+ New Task"
   - Enter collection slug (e.g., "boredapeyachtclub")
   - Select wallet and marketplaces
   - Configure bid type and pricing
   - Set optional smart stop conditions
   - Click "Create Task"
4. **Start Bidding**: Click the play button to activate

### Task Types

#### Collection Offers
Bid on any NFT in the collection:
```
- Bid Type: Collection Offer
- Price: 80% of floor
- Outbidding: Enabled
```

#### Token Offers
Target specific token IDs:
```
- Bid Type: Token Offer
- Tokens: 1-100, bot20, 500
- Price: 0.5 ETH
```

#### Trait Offers
Bid on NFTs with specific traits:
```
- Bid Type: Token Offer
- Traits: Gold Background AND Laser Eyes
- Price: 90% of trait floor
```

### Managing Tasks

- **Start/Stop**: Use play/stop buttons
- **Bulk Actions**: Select multiple tasks for batch operations
- **Export/Import**: Save and load task configurations
- **Monitoring**: Click task name for detailed bid history

## 🛠️ Development

### Project Structure

```
bidding-bot/
├── client/                 # Next.js frontend
│   ├── app/               # App router pages
│   ├── components/        # React components
│   ├── models/           # MongoDB schemas
│   └── utils/            # Utility functions
├── server/                # Express backend
│   ├── src/
│   │   ├── marketplace/  # Marketplace adapters
│   │   ├── models/       # Database models
│   │   ├── utils/        # Server utilities
│   │   └── index.ts      # Main entry point
│   └── prisma/           # Database schema
└── docker/               # Docker configurations
```

### Development Commands

```bash
# Root level
npm run dc:build          # Build and start with Docker
npm run dc:watch          # Development with hot reload
npm run dc:destroy        # Clean everything

# Client
cd client
npm run dev               # Start development server
npm run build             # Production build
npm run lint              # Run ESLint

# Server
cd server
npm run dev               # Development mode
npm run build             # Production build
npm test                  # Run tests
```

### Adding a New Marketplace

1. Create adapter in `server/src/marketplace/[name]/`
2. Implement bid.ts with marketplace-specific logic:
```typescript
export async function placeBid(params: BidParams): Promise<BidResult> {
  // Marketplace-specific implementation
}
```
3. Add WebSocket adapter for real-time monitoring
4. Update client UI to support new marketplace

## 📡 API Reference

### REST Endpoints

#### Authentication
```
POST /api/auth/login     - User login
POST /api/auth/register  - User registration
POST /api/auth/logout    - User logout
```

#### Tasks
```
GET    /api/tasks        - List all tasks
POST   /api/tasks        - Create new task
PUT    /api/tasks/:id    - Update task
DELETE /api/tasks/:id    - Delete task
POST   /api/tasks/bulk   - Bulk operations
```

#### Wallets
```
GET    /api/wallets      - List wallets
POST   /api/wallets      - Add wallet
DELETE /api/wallets/:id  - Remove wallet
```

### WebSocket Events

#### Client → Server
```javascript
socket.emit('subscribe', { taskId, collections });
socket.emit('unsubscribe', { taskId });
socket.emit('task:status', { taskId, status });
```

#### Server → Client
```javascript
socket.on('bid:placed', { taskId, marketplace, tokenId, price });
socket.on('bid:cancelled', { taskId, orderId });
socket.on('error', { message, taskId });
```

## 🔐 Security

### Wallet Encryption

The system uses advanced password-based encryption:

- **Algorithm**: AES-256-GCM with scrypt key derivation
- **Zero-Persistence**: Keys exist only in memory
- **Auto-Lock**: System pauses on server restart
- **Re-authentication**: Required after any restart

### Security Best Practices

1. **Never share private keys**
2. **Use strong passwords**
3. **Enable 2FA when available**
4. **Regularly backup wallets**
5. **Monitor wallet activities**

### Password Security Flow

```
User Password → scrypt → Encryption Key → AES-256-GCM → Encrypted Private Key
     ↓                         ↓
   bcrypt                 Memory Only
     ↓                         ↓
  Database              Server Restart
                              ↓
                        Keys Cleared
```

## 🐛 Troubleshooting

### Common Issues

#### Bot Not Bidding
- ✓ Check wallet balance (ETH/WETH)
- ✓ Verify task status is "Running"
- ✓ Ensure marketplaces are enabled
- ✓ Confirm price settings are competitive

#### Connection Errors
- ✓ Check WebSocket connection status
- ✓ Verify server is running
- ✓ Confirm firewall allows ports 3001/3003

#### Redis Cluster Issues
```bash
# Check cluster status
docker exec redis_node_1 redis-cli cluster info

# Reset cluster
npm run dc:destroy && npm run dc:up
```

### Debug Mode

Enable detailed logging:
```bash
# Server
DEBUG=* npm run dev

# Check queue metrics
npm run scripts:logLimiterCounts
```

### Health Checks

- Server: `http://localhost:3003/health`
- Client: `http://localhost:3001`
- Redis: `docker exec redis_node_1 redis-cli ping`
- MongoDB: `docker exec mongodb mongosh --eval "db.adminCommand('ping')"`

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- OpenSea, Magic Eden, and Blur for marketplace APIs
- The Web3 community for continuous support
- All contributors who help improve this project

---

For more detailed information, check our [User Guide](docs/USER_GUIDE.md) or visit our [Documentation](https://docs.nfttools.com).