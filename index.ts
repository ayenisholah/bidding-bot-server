import WebSocket, { WebSocketServer } from "ws";
import http from "http";
import express from "express";
import bodyParser from "body-parser";
import cors from "cors";
import { config } from "dotenv";
import winston from "winston";

config();

export const MAGENTA = '\x1b[35m';
export const BLUE = '\x1b[34m';
export const RESET = '\x1b[0m';
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const RED = '\x1b[31m';

const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => {
      const color = level === 'error' ? RED : level === 'warn' ? YELLOW : level === 'debug' ? BLUE : GREEN;
      return `${timestamp} ${color}${level.toUpperCase()}: ${message} ${RESET}`;
    })
  ),
  transports: [
    new winston.transports.Console()
  ]
});

const port = process.env.PORT || 8080;
const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({
  server,
  clientTracking: true,
  perMessageDeflate: {
    zlibDeflateOptions: {
      chunkSize: 1024,
      memLevel: 7,
      level: 3
    },
    zlibInflateOptions: {
      chunkSize: 10 * 1024
    },
    clientNoContextTakeover: true,
    serverNoContextTakeover: true,
    serverMaxWindowBits: 10,
    concurrencyLimit: 10,
    threshold: 1024
  }
});

app.use(bodyParser.json());
app.use(cors());
app.use(express.static('.')); // Serve static files including monitoring-ui.html


// Root endpoint - comprehensive server information
app.get('/', async (_, res) => {
  try {
    // Get all WebSocket clients
    const wsClients = Array.from(wss.clients) as WebSocketWithClient[];

    // Build detailed client information
    const clientDetails = await Promise.all(wsClients.map(async ws => {
      const queueSize = ws.clientId ? (clientMessageQueues.get(ws.clientId)?.length || 0) : 0;

      // Get client stats from memory
      let clientStats = {};
      if (ws.clientId) {
        const data = clientData.get(ws.clientId);
        const stats = getClientStats(ws.clientId);
        clientStats = {
          reconnectCount: data?.reconnectCount || 0,
          disconnectCount: data?.disconnectCount || 0,
          messagesReceived: stats.messagesReceived,
          droppedMessages: stats.droppedMessages,
          connectedAt: data?.connectedAt || null
        };
      }

      return {
        clientId: ws.clientId || 'unknown',
        readyState: ws.readyState,
        readyStateText: ['CONNECTING', 'OPEN', 'CLOSING', 'CLOSED'][ws.readyState] || 'UNKNOWN',
        bufferedAmount: ws.bufferedAmount,
        // Get subscriptions from memory
        bidSubscriptions: ws.clientId ? Array.from(clientSubscriptions.get(ws.clientId) || []) : [],
        listingSubscriptions: ws.clientId ? Array.from(clientListingSubscriptions.get(ws.clientId) || []) : [],
        queueSize: queueSize,
        ...clientStats
      };
    }));

    // Get all client data from memory
    const memoryClients: Record<string, any> = {};

    for (const [clientId, data] of clientData.entries()) {
      const subscriptions = Array.from(clientSubscriptions.get(clientId) || []);
      const listingSubscriptions = Array.from(clientListingSubscriptions.get(clientId) || []);
      const queueLength = clientMessageQueues.get(clientId)?.length || 0;

      memoryClients[clientId] = {
        lastPing: data.lastPing,
        subscriptions: subscriptions.map(s => {
          try { return JSON.parse(s); } catch { return s; }
        }),
        listingSubscriptions: listingSubscriptions.map(s => {
          try { return JSON.parse(s); } catch { return s; }
        }),
        queuedMessages: queueLength,
        isConnected: clientWebSocketMap.has(clientId)
      };
    }

    // Get marketplace connection details
    const marketplaceDetails = await Promise.all(marketplaces.map(async (m) => ({
      name: m.name,
      url: m.url,
      connections: m.connections.map((ws, index) => ({
        index,
        connected: !!ws && ws.readyState === WebSocket.OPEN,
        readyState: ws?.readyState,
        health: ws ? connectionHealthMap.get(ws) : null
      })),
      stats: {
        processed: getWebsocketStats(m.name).processed,
        errors: getWebsocketStats(m.name).errors,
        lastMessage: connectionQuality[m.name]?.lastMessage,
        messagesTotal: connectionQuality[m.name]?.messages || 0
      }
    })));

    // Memory and optimization metrics
    const memoryUsage = process.memoryUsage();
    const optimizationStats = {
      deduplicationCache: {
        size: messageDeduplicationCache.size,
        maxSize: DEDUP_CACHE_MAX_SIZE,
        utilization: ((messageDeduplicationCache.size / DEDUP_CACHE_MAX_SIZE) * 100).toFixed(2) + '%'
      },
      subscriptionIndexes: {
        contracts: contractSubscriptionIndex.size,
        slugs: slugSubscriptionIndex.size,
        totalMappings: Array.from(contractSubscriptionIndex.values()).reduce((sum, set) => sum + set.size, 0) +
          Array.from(slugSubscriptionIndex.values()).reduce((sum, set) => sum + set.size, 0)
      },
      messageQueues: {
        activeQueues: clientMessageQueues.size,
        totalQueuedMessages: Array.from(clientMessageQueues.values()).reduce((sum, queue) => sum + queue.length, 0),
        largestQueue: Math.max(...Array.from(clientMessageQueues.values()).map(q => q.length), 0),
        averageQueueSize: clientMessageQueues.size > 0
          ? Array.from(clientMessageQueues.values()).reduce((sum, queue) => sum + queue.length, 0) / clientMessageQueues.size
          : 0
      },
    };

    // Server configuration
    const serverConfig = {
      port,
      connectionsPerMarketplace: CONNECTIONS_PER_MARKETPLACE,
      pingTimeout: PING_TIMEOUT,
      inactiveClientThreshold: INACTIVE_CLIENT_THRESHOLD,
      environment: process.env.NODE_ENV || 'development',
      logLevel: process.env.LOG_LEVEL || 'info'
    };

    res.json({
      server: {
        status: 'ok',
        uptime: process.uptime(),
        timestamp: new Date().toISOString(),
        version: process.version,
        memory: {
          heapUsed: `${(memoryUsage.heapUsed / 1024 / 1024).toFixed(2)} MB`,
          heapTotal: `${(memoryUsage.heapTotal / 1024 / 1024).toFixed(2)} MB`,
          external: `${(memoryUsage.external / 1024 / 1024).toFixed(2)} MB`,
          rss: `${(memoryUsage.rss / 1024 / 1024).toFixed(2)} MB`
        }
      },
      config: serverConfig,
      websockets: {
        totalClients: wss.clients.size,
        connectedClients: clientDetails.filter(c => c.readyState === 1).length,
        clients: clientDetails
      },
      memory: {
        totalClients: Object.keys(memoryClients).length,
        clients: memoryClients
      },
      marketplaces: marketplaceDetails,
      optimization: optimizationStats,
      summary: {
        activeConnections: clientDetails.filter(c => c.readyState === 1).length,
        totalSubscriptions: Object.values(memoryClients).reduce((sum, client) =>
          sum + client.subscriptions.length + client.listingSubscriptions.length, 0),
        messagesProcessed: marketplaceDetails.reduce((sum, m) => sum + m.stats.processed, 0),
        errorRate: (() => {
          const total = marketplaceDetails.reduce((sum, m) => sum + m.stats.processed, 0);
          const errors = marketplaceDetails.reduce((sum, m) => sum + m.stats.errors, 0);
          return total > 0 ? ((errors / total) * 100).toFixed(2) + '%' : '0%';
        })()
      }
    });
  } catch (error) {
    logger.error('Error generating root endpoint response:', error);
    res.status(500).json({ error: 'Internal server error', message: (error as Error).message });
  }
});

// Delivery stats endpoint
app.get('/delivery-stats', async (_, res) => {
  try {
    // Get message processing stats from memory
    const stats: Record<string, string> = {};
    for (const [marketplace, data] of inMemoryStats.websocket.entries()) {
      stats[`${marketplace}:processed`] = data.processed.toString();
      stats[`${marketplace}:errors`] = data.errors.toString();
    }

    // Get all client message counts from memory
    let totalMessagesDelivered = 0;
    const clientDeliveryStats = [];

    for (const [clientId, data] of clientData.entries()) {
      const stats = getClientStats(clientId);
      const messagesReceived = stats.messagesReceived;
      totalMessagesDelivered += messagesReceived;

      if (messagesReceived > 0) {
        clientDeliveryStats.push({
          clientId,
          messagesReceived,
          reconnectCount: data.reconnectCount,
          disconnectCount: data.disconnectCount
        });
      }
    }

    // Calculate delivery rate
    const totalProcessed = Object.entries(stats)
      .filter(([key]) => key.endsWith(':processed'))
      .reduce((sum, [, value]) => sum + parseInt(value || '0'), 0);

    const deliveryRate = totalProcessed > 0
      ? ((totalMessagesDelivered / totalProcessed) * 100).toFixed(2)
      : '0.00';

    res.json({
      totalMessagesProcessed: totalProcessed,
      totalMessagesDelivered: totalMessagesDelivered,
      deliveryRate: `${deliveryRate}%`,
      byMarketplace: {
        opensea: {
          processed: parseInt(stats['OPENSEA:processed'] || '0'),
          errors: parseInt(stats['OPENSEA:errors'] || '0')
        },
        // magiceden: {
        //   processed: parseInt(stats['MAGICEDEN:processed'] || '0'),
        //   errors: parseInt(stats['MAGICEDEN:errors'] || '0')
        // },
        blur: {
          processed: parseInt(stats['BLUR:processed'] || '0'),
          errors: parseInt(stats['BLUR:errors'] || '0')
        }
      },
      clientDeliveryStats: clientDeliveryStats.sort((a, b) => b.messagesReceived - a.messagesReceived),
      possibleIssues: {
        noSubscribedClients: clientDeliveryStats.length === 0,
        highDisconnectRate: clientDeliveryStats.some(c => c.disconnectCount > c.reconnectCount * 2),
        deduplicationActive: true,
        subscriptionMismatches: totalProcessed > 0 && totalMessagesDelivered === 0
      }
    });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// Collection stats endpoint
app.get('/collection-stats', async (_, res) => {
  try {
    const stats: Record<string, string> = {};
    for (const [key, count] of inMemoryStats.collection.entries()) {
      stats[key] = count.toString();
    }

    if (!stats || Object.keys(stats).length === 0) {
      return res.json({
        message: 'No collection statistics yet. Stats will be collected as messages are processed.',
        totalCollections: 0,
        totalMessages: 0,
        collections: []
      });
    }

    // Parse and sort by message count
    const collections = Object.entries(stats)
      .map(([key, value]) => {
        const [marketplace, ...collectionParts] = key.split(':');
        const collection = collectionParts.join(':');
        return {
          marketplace,
          collection,
          count: parseInt(value)
        };
      })
      .sort((a, b) => b.count - a.count);

    const totalMessages = collections.reduce((sum, c) => sum + c.count, 0);

    // Get client subscriptions for comparison from memory
    const allSubscriptions = new Set<string>();

    for (const [clientId] of clientData.entries()) {
      const bidSubs = Array.from(clientSubscriptions.get(clientId) || []);
      const listingSubs = Array.from(clientListingSubscriptions.get(clientId) || []);

      [...bidSubs, ...listingSubs].forEach(sub => {
        try {
          const parsed = JSON.parse(sub);
          if (parsed.slug) allSubscriptions.add(parsed.slug);
          if (parsed.contractAddress) allSubscriptions.add(parsed.contractAddress.toLowerCase());
        } catch (e) { }
      });
    }

    res.json({
      totalCollections: collections.length,
      totalMessages,
      clientSubscribedCollections: Array.from(allSubscriptions),
      topCollections: collections.slice(0, 50).map(c => ({
        ...c,
        percentage: ((c.count / totalMessages) * 100).toFixed(2),
        isSubscribed: allSubscriptions.has(c.collection)
      })),
      summary: {
        averageMessagesPerCollection: (totalMessages / collections.length).toFixed(2),
        collectionsWithSubscribers: collections.filter(c => allSubscriptions.has(c.collection)).length,
        messagesForSubscribedCollections: collections
          .filter(c => allSubscriptions.has(c.collection))
          .reduce((sum, c) => sum + c.count, 0)
      }
    });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// Health check endpoint
app.get('/health', async (_, res) => {
  const connectedClients = Array.from(wss.clients).filter(
    (client: WebSocket) => client.readyState === WebSocket.OPEN
  ).length;

  const marketplaceStatus = marketplaces.map(m => ({
    name: m.name,
    connections: m.connections.map((c, i) => ({
      index: i,
      connected: !!c && c.readyState === WebSocket.OPEN
    }))
  }));

  // Get active client IDs from memory
  const activeClientIds = Array.from(clientData.keys());

  // Get processing stats from memory
  const stats: Record<string, any> = {};
  for (const marketplace of ['OPENSEA', /* 'MAGICEDEN', */ 'BLUR']) {
    const marketplaceStats = getWebsocketStats(marketplace);
    stats[marketplace] = {
      processed: marketplaceStats.processed,
      errors: marketplaceStats.errors,
      errorRate: marketplaceStats.processed > 0 ? ((marketplaceStats.errors / marketplaceStats.processed) * 100).toFixed(2) + '%' : '0%'
    };
  }

  // Add connection quality data
  const connectionHealth = Object.entries(connectionQuality).reduce((acc, [marketplace, quality]) => {
    const timeSinceLastMessage = quality.lastMessage ? Date.now() - quality.lastMessage : null;
    acc[marketplace] = {
      ...quality,
      timeSinceLastMessage,
      status: timeSinceLastMessage && timeSinceLastMessage > 300000 ? 'STALE' : 'HEALTHY'
    };
    return acc;
  }, {} as Record<string, any>);


  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    connectedClients,
    marketplaces: marketplaceStatus,
    activeClientIds,
    processingStats: stats,
    connectionHealth,
    optimizationMetrics: {
      connectionsPerMarketplace: CONNECTIONS_PER_MARKETPLACE,
      deduplicationCacheSize: messageDeduplicationCache.size,
      activeWebSocketMap: clientWebSocketMap.size,
      messageQueues: clientMessageQueues.size,
      contractIndexes: contractSubscriptionIndex.size,
      slugIndexes: slugSubscriptionIndex.size,
    }
  });
});

server.listen(port, () => {
  logger.info(`WEBSOCKET SERVER IS RUNNING ON ws://localhost:${port}`);
});

const OPENSEA_API_KEY = process.env.OPENSEA_API_KEY;
// OpenSea Stream API requires a valid API key with Stream API access enabled
// Contact OpenSea support to enable Stream API access for your API key
const OPENSEA_WS_URL = `wss://stream.openseabeta.com/socket/websocket?token=${OPENSEA_API_KEY}`;
const OPENSEA = "OPENSEA";
const MAGICEDEN = "MAGICEDEN";
const BLUR = "BLUR";
const MAGICEDEN_WS_URL = `wss://wss-mainnet.magiceden.io`;
const BLUR_WS_URL = "wss://feeds.prod.blur.io/socket.io/?tabId=ApAVers0Cfxd&storageId=FeigI7a9xqRU&EIO=4&transport=websocket";

const CONNECTIONS_PER_MARKETPLACE = 3; // Reduced for stable production deployment
const PING_TIMEOUT = 120000; // 120 seconds
const INACTIVE_CLIENT_THRESHOLD = 300000; // 5 minutes in milliseconds - aligned with Redis expiry

// Add connection quality tracking
const connectionQuality: Record<string, { messages: number; errors: number; lastMessage: number | null }> = {
  OPENSEA: { messages: 0, errors: 0, lastMessage: null },
  // MAGICEDEN: { messages: 0, errors: 0, lastMessage: null },
  BLUR: { messages: 0, errors: 0, lastMessage: null }
};

// Message deduplication cache
// Uses order_hash for OpenSea and id for MagicEden to prevent duplicate messages
// This prevents duplicate messages from multiple WebSocket connections to the same marketplace
// while still allowing legitimate repeated bid amounts or similar events
const messageDeduplicationCache = new Map<string, number>();
const DEDUP_CACHE_TTL = 60000; // 60 seconds - reasonable window for order deduplication
const DEDUP_CACHE_MAX_SIZE = 100000; // 100k entries - sufficient for deduplication

// Client WebSocket map for fast lookups (O(1) instead of O(n))
const clientWebSocketMap = new Map<string, WebSocketWithClient>();

// Rate limiting for ping messages - prevent spam
const clientLastPing = new Map<string, number>();
const PING_RATE_LIMIT = 1000; // Minimum 1 second between pings

// Subscription indexes for O(1) lookups
const contractSubscriptionIndex = new Map<string, Set<string>>(); // contract -> clientIds
const slugSubscriptionIndex = new Map<string, Set<string>>(); // slug -> clientIds

// Message queue for handling slow clients
const clientMessageQueues = new Map<string, any[]>();

// Connection health monitoring
interface ConnectionHealth {
  messagesPerSecond: number;
  lastReset: number;
  messageCount: number;
  isHealthy: boolean;
}
const connectionHealthMap = new Map<WebSocket, ConnectionHealth>();


type MarketplaceName = "OPENSEA" | "MAGICEDEN" | "BLUR";
interface MarketplaceConnection {
  name: MarketplaceName;
  url: string;
  connections: WebSocket[];
}

// Mock Redis object to prevent errors in legacy code
const redis = {
  keys: (..._args: any[]): Promise<string[]> => Promise.resolve([]),
  hgetall: (..._args: any[]): Promise<Record<string, string>> => Promise.resolve({}),
  hget: (..._args: any[]): Promise<string | null> => Promise.resolve(null),
  hset: (..._args: any[]): Promise<number> => Promise.resolve(0),
  hincrby: (..._args: any[]): Promise<number> => Promise.resolve(0),
  expire: (..._args: any[]): Promise<number> => Promise.resolve(1),
  del: (..._args: any[]): Promise<number> => Promise.resolve(0),
  exists: (..._args: any[]): Promise<number> => Promise.resolve(0),
  smembers: (..._args: any[]): Promise<string[]> => Promise.resolve([]),
  sadd: (..._args: any[]): Promise<number> => Promise.resolve(0),
  scard: (..._args: any[]): Promise<number> => Promise.resolve(0),
  lpush: (..._args: any[]): Promise<number> => Promise.resolve(0),
  ltrim: (..._args: any[]): Promise<string> => Promise.resolve('OK'),
  llen: (..._args: any[]): Promise<number> => Promise.resolve(0),
  quit: (..._args: any[]): Promise<string> => Promise.resolve('OK')
};

// In-memory client tracking (replaces Redis completely)
const clientData = new Map<string, {
  lastPing: number;
  connectedAt: number;
  reconnectCount: number;
  disconnectCount: number;
}>();

// In-memory subscription tracking (replaces Redis)
const clientSubscriptions = new Map<string, Set<string>>(); // clientId -> Set of subscription strings
const clientListingSubscriptions = new Map<string, Set<string>>(); // clientId -> Set of listing subscription strings

// High-performance reverse indexes for O(1) subscription lookups
const contractAddressToClients = new Map<string, Set<string>>(); // contractAddress -> Set<clientId>
const slugToClients = new Map<string, Set<string>>(); // slug -> Set<clientId>
const contractAddressToListingClients = new Map<string, Set<string>>(); // contractAddress -> Set<clientId> for listings
const slugToListingClients = new Map<string, Set<string>>(); // slug -> Set<clientId> for listings

// Pre-parsed subscription cache for ultra-fast lookups
const parsedSubscriptionCache = new Map<string, {
  contractAddress?: string;
  slug?: string;
  parsedAt: number;
}>();

// Client cleanup configuration
const CLIENT_INACTIVE_THRESHOLD = 5 * 60 * 1000; // 5 minutes
const marketplaces: MarketplaceConnection[] = [
  { name: OPENSEA, url: OPENSEA_WS_URL, connections: [] },
  { name: MAGICEDEN, url: MAGICEDEN_WS_URL, connections: [] },
  { name: BLUR, url: BLUR_WS_URL, connections: [] }
];


// Message queue removed - forwarding messages directly to clients

// Client tracking is now stored in Redis instead of in-memory
// Client tracking now handled entirely in memory
const clientLastResponse = new Map<string, number>();

// In-memory statistics for low latency (non-persistent)
const inMemoryStats = {
  websocket: new Map<string, { processed: number; errors: number }>(),
  collection: new Map<string, number>(),
  clients: new Map<string, { messagesReceived: number; droppedMessages: number; lastUpdate: number }>()
};

// Cleanup intervals for in-memory data
const STATS_CLEANUP_INTERVAL = 30 * 60 * 1000; // 30 minutes
const CLIENT_STATS_TTL = 10 * 60 * 1000; // 10 minutes

// Cache for client subscriptions to reduce Redis calls
interface SubscriptionCacheEntry {
  bids: Set<string>;
  listings: Set<string>;
  parsedBids: Map<string, { contractAddress?: string; slug?: string }>;
  parsedListings: Map<string, { contractAddress?: string; slug?: string }>;
  lastUpdated: number;
}
const subscriptionCache = new Map<string, SubscriptionCacheEntry>();
const CACHE_TTL = 300000; // 5 minutes cache TTL - subscriptions change rarely, optimize for performance

// Buffer management constants
const MAX_BUFFER_SIZE = 1024 * 1024; // 1MB max buffer

// Helper functions for in-memory stats (low latency alternatives to Redis)
function incrementWebsocketStats(marketplace: string, field: 'processed' | 'errors'): void {
  const stats = inMemoryStats.websocket.get(marketplace) || { processed: 0, errors: 0 };
  stats[field]++;
  inMemoryStats.websocket.set(marketplace, stats);
}

function incrementCollectionStats(key: string): void {
  const current = inMemoryStats.collection.get(key) || 0;
  inMemoryStats.collection.set(key, current + 1);
}

function incrementClientStats(clientId: string, field: 'messagesReceived' | 'droppedMessages', amount: number = 1): void {
  const stats = inMemoryStats.clients.get(clientId) || { messagesReceived: 0, droppedMessages: 0, lastUpdate: Date.now() };
  stats[field] += amount;
  stats.lastUpdate = Date.now();
  inMemoryStats.clients.set(clientId, stats);
}

function getWebsocketStats(marketplace: string): { processed: number; errors: number } {
  return inMemoryStats.websocket.get(marketplace) || { processed: 0, errors: 0 };
}

function getClientStats(clientId: string): { messagesReceived: number; droppedMessages: number } {
  const stats = inMemoryStats.clients.get(clientId);
  return stats ? { messagesReceived: stats.messagesReceived, droppedMessages: stats.droppedMessages } : { messagesReceived: 0, droppedMessages: 0 };
}

// Cleanup old client stats to prevent memory leaks
function cleanupOldClientStats(): void {
  const now = Date.now();
  for (const [clientId, stats] of inMemoryStats.clients.entries()) {
    if (now - stats.lastUpdate > CLIENT_STATS_TTL) {
      inMemoryStats.clients.delete(clientId);
    }
  }
}

// Start cleanup interval
setInterval(cleanupOldClientStats, STATS_CLEANUP_INTERVAL);

// Client data cleanup - remove inactive clients from memory
function cleanupInactiveClients(): void {
  const now = Date.now();
  for (const [clientId, data] of clientData.entries()) {
    if (now - data.lastPing > CLIENT_INACTIVE_THRESHOLD) {
      clientData.delete(clientId);
      clientSubscriptions.delete(clientId);
      clientListingSubscriptions.delete(clientId);
      inMemoryStats.clients.delete(clientId);
    }
  }
}

// Start client cleanup (every 2 minutes)
setInterval(cleanupInactiveClients, 2 * 60 * 1000);

// Legacy constant for backward compatibility
const REDIS_EXPIRY = 300; // 5 minutes in seconds

// High-performance reverse index management functions
function addToReverseIndex(
  clientId: string,
  contractAddress: string | undefined,
  slug: string | undefined,
  isListing: boolean = false
): void {
  if (contractAddress) {
    const normalizedAddress = contractAddress.toLowerCase();
    const index = isListing ? contractAddressToListingClients : contractAddressToClients;

    if (!index.has(normalizedAddress)) {
      index.set(normalizedAddress, new Set());
    }
    index.get(normalizedAddress)!.add(clientId);
  }

  if (slug) {
    const index = isListing ? slugToListingClients : slugToClients;

    if (!index.has(slug)) {
      index.set(slug, new Set());
    }
    index.get(slug)!.add(clientId);
  }
}

function removeFromReverseIndex(
  clientId: string,
  contractAddress: string | undefined,
  slug: string | undefined,
  isListing: boolean = false
): void {
  if (contractAddress) {
    const normalizedAddress = contractAddress.toLowerCase();
    const index = isListing ? contractAddressToListingClients : contractAddressToClients;
    const clients = index.get(normalizedAddress);

    if (clients) {
      clients.delete(clientId);
      if (clients.size === 0) {
        index.delete(normalizedAddress);
      }
    }
  }

  if (slug) {
    const index = isListing ? slugToListingClients : slugToClients;
    const clients = index.get(slug);

    if (clients) {
      clients.delete(clientId);
      if (clients.size === 0) {
        index.delete(slug);
      }
    }
  }
}

function parseAndCacheSubscription(subscriptionStr: string): { contractAddress?: string; slug?: string } | null {
  // Check cache first
  const cached = parsedSubscriptionCache.get(subscriptionStr);
  if (cached && (Date.now() - cached.parsedAt < CACHE_TTL)) { // Use same TTL as main cache
    return { contractAddress: cached.contractAddress, slug: cached.slug };
  }

  try {
    const parsed = JSON.parse(subscriptionStr);
    const result = {
      contractAddress: parsed.contractAddress,
      slug: parsed.slug
    };

    // Cache the parsed result
    parsedSubscriptionCache.set(subscriptionStr, {
      ...result,
      parsedAt: Date.now()
    });

    return result;
  } catch (e) {
    return null;
  }
}

function getInterestedClients(contractAddress?: string, slug?: string): {
  bidClients: Set<string>;
  listingClients: Set<string>;
} {
  const bidClients = new Set<string>();
  const listingClients = new Set<string>();

  if (contractAddress) {
    const normalizedAddress = contractAddress.toLowerCase();

    // Get bid subscribers
    const bidSubscribers = contractAddressToClients.get(normalizedAddress);
    if (bidSubscribers) {
      bidSubscribers.forEach(clientId => bidClients.add(clientId));
    }

    // Get listing subscribers
    const listingSubscribers = contractAddressToListingClients.get(normalizedAddress);
    if (listingSubscribers) {
      listingSubscribers.forEach(clientId => listingClients.add(clientId));
    }
  }

  if (slug) {
    // Get bid subscribers
    const bidSubscribers = slugToClients.get(slug);
    if (bidSubscribers) {
      bidSubscribers.forEach(clientId => bidClients.add(clientId));
    }

    // Get listing subscribers
    const listingSubscribers = slugToListingClients.get(slug);
    if (listingSubscribers) {
      listingSubscribers.forEach(clientId => listingClients.add(clientId));
    }
  }

  return { bidClients, listingClients };
}

// Message deduplication function
function isDuplicateMessage(messageHash: string): boolean {
  const now = Date.now();
  const existing = messageDeduplicationCache.get(messageHash);

  if (existing && (now - existing) < DEDUP_CACHE_TTL) {
    return true;
  }

  // Add to cache
  messageDeduplicationCache.set(messageHash, now);

  // Cleanup old entries if cache is too large
  if (messageDeduplicationCache.size > DEDUP_CACHE_MAX_SIZE) {
    const cutoff = now - DEDUP_CACHE_TTL;
    for (const [hash, timestamp] of messageDeduplicationCache) {
      if (timestamp < cutoff) {
        messageDeduplicationCache.delete(hash);
      }
    }
  }

  return false;
}

// Update subscription indexes
function updateSubscriptionIndex(clientId: string, contract: string | undefined, slug: string | undefined, add: boolean) {
  if (contract) {
    const contractSet = contractSubscriptionIndex.get(contract.toLowerCase()) || new Set();
    if (add) {
      contractSet.add(clientId);
    } else {
      contractSet.delete(clientId);
    }
    contractSubscriptionIndex.set(contract.toLowerCase(), contractSet);
  }

  if (slug) {
    const slugSet = slugSubscriptionIndex.get(slug) || new Set();
    if (add) {
      slugSet.add(clientId);
    } else {
      slugSet.delete(clientId);
    }
    slugSubscriptionIndex.set(slug, slugSet);
  }
}

// Queue message for slow client
async function queueMessageForClient(clientId: string, message: any): Promise<void> {
  try {
    if (!clientMessageQueues.has(clientId)) {
      clientMessageQueues.set(clientId, []);
    }
    const queue = clientMessageQueues.get(clientId)!;

    // Limit queue size to prevent memory issues
    const MAX_QUEUE_SIZE = 1000;
    if (queue.length >= MAX_QUEUE_SIZE) {
      // Remove oldest messages and track as dropped
      const droppedCount = 10;
      queue.splice(0, droppedCount);

      // Track dropped messages (in-memory for low latency)
      incrementClientStats(clientId, 'droppedMessages', droppedCount);

      logger.debug(`Queue full for client ${clientId}, dropped ${droppedCount} messages`);
    }

    queue.push(message);
    logger.debug(`Queued message for client ${clientId}, queue size: ${queue.length}`);
  } catch (error) {
    logger.error(`Failed to queue message for client ${clientId}: ${error}`);
    // Track this as a dropped message since queuing failed (in-memory for low latency)
    incrementClientStats(clientId, 'droppedMessages');
  }
}

// Update connection health
function updateConnectionHealth(ws: WebSocket) {
  const health = connectionHealthMap.get(ws) || {
    messagesPerSecond: 0,
    lastReset: Date.now(),
    messageCount: 0,
    isHealthy: true
  };

  health.messageCount++;

  const elapsed = Date.now() - health.lastReset;
  if (elapsed >= 1000) {
    health.messagesPerSecond = health.messageCount / (elapsed / 1000);
    health.messageCount = 0;
    health.lastReset = Date.now();
    health.isHealthy = health.messagesPerSecond > 0.1; // At least 0.1 msg/sec
  }

  connectionHealthMap.set(ws, health);
}


async function registerClient(ws: WebSocketWithClient, clientId: string, lastPing: string = Date.now().toString()) {
  // Check if this is actually a new connection
  const existingWs = clientWebSocketMap.get(clientId);
  const isReconnection = !existingWs || existingWs !== ws;

  // Store client ID on WebSocket instance
  ws.clientId = clientId;

  // Add to client WebSocket map for fast lookups
  clientWebSocketMap.set(clientId, ws);

  // Only track reconnections for actual new connections
  let reconnectCount = 1;
  if (isReconnection) {
    const existing = clientData.get(clientId);
    reconnectCount = (existing?.reconnectCount || 0) + 1;

    // Store in memory
    clientData.set(clientId, {
      lastPing: parseInt(lastPing),
      connectedAt: Date.now(),
      reconnectCount,
      disconnectCount: existing?.disconnectCount || 0
    });

    if (reconnectCount > 1) {
      logger.info(`Client ${clientId} reconnected (${reconnectCount} total connections)`);
    }
  } else {
    // Just update last ping time
    const existing = clientData.get(clientId);
    if (existing) {
      existing.lastPing = parseInt(lastPing);
    } else {
      clientData.set(clientId, {
        lastPing: parseInt(lastPing),
        connectedAt: Date.now(),
        reconnectCount: 1,
        disconnectCount: 0
      });
    }
  }

  // Client data is now stored in memory with automatic cleanup

  return clientId;
}

function saveClientSubscriptions(clientId: string, subscriptions: Set<string>) {
  // Remove old subscriptions from reverse indexes
  const oldSubscriptions = clientSubscriptions.get(clientId);
  if (oldSubscriptions) {
    for (const sub of oldSubscriptions) {
      const parsed = parseAndCacheSubscription(sub);
      if (parsed) {
        removeFromReverseIndex(clientId, parsed.contractAddress, parsed.slug, false);
      }
    }
  }

  // Store in memory
  clientSubscriptions.set(clientId, new Set(subscriptions));

  // Invalidate cache
  subscriptionCache.delete(clientId);

  // Add new subscriptions to reverse indexes
  for (const sub of subscriptions) {
    const parsed = parseAndCacheSubscription(sub);
    if (parsed) {
      addToReverseIndex(clientId, parsed.contractAddress, parsed.slug, false);
    }
  }
}

function getClientSubscriptions(clientId: string): Set<string> {
  return clientSubscriptions.get(clientId) || new Set();
}

function saveClientListingSubscriptions(clientId: string, subscriptions: Set<string>) {
  // Remove old listing subscriptions from reverse indexes
  const oldSubscriptions = clientListingSubscriptions.get(clientId);
  if (oldSubscriptions) {
    for (const sub of oldSubscriptions) {
      const parsed = parseAndCacheSubscription(sub);
      if (parsed) {
        removeFromReverseIndex(clientId, parsed.contractAddress, parsed.slug, true);
      }
    }
  }

  // Store in memory
  clientListingSubscriptions.set(clientId, new Set(subscriptions));

  // Invalidate cache
  subscriptionCache.delete(clientId);

  // Add new listing subscriptions to reverse indexes
  for (const sub of subscriptions) {
    const parsed = parseAndCacheSubscription(sub);
    if (parsed) {
      addToReverseIndex(clientId, parsed.contractAddress, parsed.slug, true);
    }
  }
}

function getClientListingSubscriptions(clientId: string): Set<string> {
  return clientListingSubscriptions.get(clientId) || new Set();
}

// Get cached subscriptions or fetch from memory
function getCachedSubscriptions(clientId: string): SubscriptionCacheEntry {
  const cached = subscriptionCache.get(clientId);
  const now = Date.now();

  if (cached && (now - cached.lastUpdated) < CACHE_TTL) {
    return cached;
  }

  // Fetch from memory
  const bids = getClientSubscriptions(clientId);
  const listings = getClientListingSubscriptions(clientId);

  // Parse subscriptions once and cache the results
  const parsedBids = new Map<string, { contractAddress?: string; slug?: string }>();
  const parsedListings = new Map<string, { contractAddress?: string; slug?: string }>();

  for (const sub of bids) {
    try {
      const parsed = JSON.parse(sub);
      parsedBids.set(sub, parsed);
    } catch (e) {
      // Skip invalid subscriptions
    }
  }

  for (const sub of listings) {
    try {
      const parsed = JSON.parse(sub);
      parsedListings.set(sub, parsed);
    } catch (e) {
      // Skip invalid subscriptions
    }
  }

  const cacheEntry: SubscriptionCacheEntry = {
    bids,
    listings,
    parsedBids,
    parsedListings,
    lastUpdated: now
  };

  subscriptionCache.set(clientId, cacheEntry);

  return cacheEntry;
}

// Safe WebSocket send with buffer management
async function safeSend(ws: WebSocket, message: string, clientId: string): Promise<boolean> {
  // Check if WebSocket is open
  if (ws.readyState !== WebSocket.OPEN) {
    logger.debug(`WebSocket not open for client ${clientId}, state: ${ws.readyState}`);
    // Track as dropped message when WebSocket is not open (in-memory for low latency)
    incrementClientStats(clientId, 'droppedMessages');
    return false;
  }

  // Check buffer BEFORE attempting to send to prevent overflow
  // Queue immediately if buffer is getting full (>50% capacity)
  if (ws.bufferedAmount > MAX_BUFFER_SIZE / 2) {
    logger.debug(`Client ${clientId} buffer at ${ws.bufferedAmount} bytes (${Math.round(ws.bufferedAmount / MAX_BUFFER_SIZE * 100)}% full), queueing message`);

    try {
      const parsedMessage = JSON.parse(message);
      await queueMessageForClient(clientId, parsedMessage);
      return true; // Successfully queued
    } catch (queueError) {
      logger.error(`Failed to queue message for client ${clientId}: ${queueError}`);
      // Already tracked as dropped in queueMessageForClient
      return false;
    }
  }

  // Buffer has space, try to send directly
  try {
    ws.send(message);
    return true;
  } catch (error) {
    // Send failed despite buffer check - queue for retry
    logger.error(`Failed to send message to client ${clientId}: ${error}`);

    try {
      const parsedMessage = JSON.parse(message);
      await queueMessageForClient(clientId, parsedMessage);
      return true; // Successfully queued
    } catch (queueError) {
      logger.error(`Failed to queue message after send failure: ${queueError}`);
      // Already tracked as dropped in queueMessageForClient
      return false;
    }
  }
}


// Get active subscription count from memory
function getActiveSubscriptionCount(): number {
  let totalCount = 0;
  for (const subscriptions of clientSubscriptions.values()) {
    totalCount += subscriptions.size;
  }
  for (const subscriptions of clientListingSubscriptions.values()) {
    totalCount += subscriptions.size;
  }
  return totalCount;
}


// Broadcast message to all connected clients
async function broadcastToClients(message: any) {
  const connectedClients = Array.from(wss.clients).filter(
    (client: WebSocket) => client.readyState === WebSocket.OPEN
  );

  for (const client of connectedClients) {
    try {
      client.send(JSON.stringify(message));
    } catch (error) {
      logger.error(`Failed to send broadcast to client: ${error}`);
    }
  }
}

// Get marketplace subscription count from memory
function getMarketplaceSubscriptionCount(_marketplaceName: string): number {
  let count = 0;

  for (const subscriptions of clientSubscriptions.values()) {
    for (const sub of subscriptions) {
      try {
        JSON.parse(sub); // Validate it's valid JSON
        // Count if subscription is for this marketplace (based on available data)
        count++;
      } catch (e) {
        // Skip invalid subscriptions
      }
    }
  }

  // Divide by 3 since each subscription can be for up to 3 marketplaces
  return Math.ceil(count / 3);
}

// Dynamic connection scaling (simplified - Redis removed)
function adjustMarketplaceConnections() {
  // Simplified implementation without Redis dependency
  for (const marketplace of marketplaces) {
    const subscriptionCount = getMarketplaceSubscriptionCount(marketplace.name);
    const currentConnections = marketplace.connections.filter(c => c && c.readyState === WebSocket.OPEN).length;

    logger.debug(`${marketplace.name}: ${subscriptionCount} subscriptions, ${currentConnections} connections`);

    // Maintain CONNECTIONS_PER_MARKETPLACE connections
    while (marketplace.connections.length > CONNECTIONS_PER_MARKETPLACE) {
      const conn = marketplace.connections.pop();
      if (conn && conn.readyState === WebSocket.OPEN) {
        conn.close();
      }
    }
  }
}

// Verify and restore subscriptions when marketplace reconnects (memory-based)
function verifyAndRestoreSubscriptions(marketplaceName: string) {
  const marketplace = marketplaces.find(m => m.name === marketplaceName);
  if (!marketplace) return;

  const allBidSubscriptions = new Set<string>();
  const allListingSubscriptions = new Set<string>();

  // Collect all unique subscriptions from memory
  for (const subscriptions of clientSubscriptions.values()) {
    subscriptions.forEach(sub => allBidSubscriptions.add(sub));
  }

  for (const subscriptions of clientListingSubscriptions.values()) {
    subscriptions.forEach(sub => allListingSubscriptions.add(sub));
  }

  logger.info(`Restoring ${allBidSubscriptions.size} bid subscriptions and ${allListingSubscriptions.size} listing subscriptions for ${marketplaceName}`);

  // Re-subscribe to each bid collection
  for (const subStr of allBidSubscriptions) {
    try {
      const sub = JSON.parse(subStr);

      marketplace.connections.forEach(ws => {
        if (!ws || ws.readyState !== WebSocket.OPEN) return;

        if (marketplaceName === OPENSEA) {
          const openseaMessage = {
            topic: `collection:${sub.slug}`,
            event: "phx_join",
            payload: {},
            ref: 0,
          };
          ws.send(JSON.stringify(openseaMessage));
        }
        else if (marketplaceName === MAGICEDEN) {
          const magicEdenMessage = {
            type: "subscribeCollection",
            constraint: {
              chain: "ethereum",
              collectionSymbol: sub.contractAddress?.toLowerCase(),
            },
          };
          ws.send(JSON.stringify(magicEdenMessage));
        }
        else if (marketplaceName === BLUR) {
          const bidStats = `4224["subscribe",["${sub.contractAddress}.denormalizer.bidStats"]]`;
          const bidLevels = `4215["subscribe",["${sub.contractAddress}.COLLECTION.{}.denormalizer.bidLevels"]]`;
          const eventsCreated = `4218["subscribe",["${sub.contractAddress}.feeds.activity.eventsCreated"]]`;

          ws.send(bidStats);
          ws.send(bidLevels);
          ws.send(eventsCreated);
        }
      });
    } catch (error) {
      logger.error(`Error restoring bid subscription: ${error}`);
    }
  }

  // Re-subscribe to each listing collection
  for (const subStr of allListingSubscriptions) {
    try {
      const sub = JSON.parse(subStr);

      marketplace.connections.forEach(ws => {
        if (!ws || ws.readyState !== WebSocket.OPEN) return;

        if (marketplaceName === OPENSEA) {
          const openseaListingMessage = {
            topic: `collection:${sub.slug}:listings`,
            event: "phx_join",
            payload: {},
            ref: 0,
          };
          ws.send(JSON.stringify(openseaListingMessage));
        }
        else if (marketplaceName === MAGICEDEN) {
          const magicEdenListingMessage = {
            type: "subscribeListings",
            constraint: {
              chain: "ethereum",
              collectionSymbol: sub.contractAddress?.toLowerCase(),
            },
          };
          ws.send(JSON.stringify(magicEdenListingMessage));
        }
        else if (marketplaceName === BLUR) {
          const askStats = `4225["subscribe",["${sub.contractAddress}.denormalizer.askStats"]]`;
          const askLevels = `4216["subscribe",["${sub.contractAddress}.COLLECTION.{}.denormalizer.askLevels"]]`;
          const listingEvents = `4219["subscribe",["${sub.contractAddress}.feeds.activity.listingsCreated"]]`;

          ws.send(askLevels);
          ws.send(askStats);
          ws.send(listingEvents);
        }
      });
    } catch (error) {
      logger.error(`Error restoring listing subscription: ${error}`);
    }
  }
}

const heartbeatStrategy: Record<MarketplaceName, (ws: WebSocket, i: number) => void> = {
  OPENSEA: (ws, i) => {
    const message = {
      topic: "phoenix",
      event: "heartbeat",
      payload: {},
      ref: 0,
    };
    ws.send(JSON.stringify(message));
    logger.info(`${BLUE}Heartbeat sent to OPENSEA [${i}]${RESET}`);
  },
  MAGICEDEN: (ws, i) => {
    ws.send("ping");
    logger.info(`${MAGENTA}Heartbeat sent to MAGICEDEN [${i}]${RESET}`);
  },
  BLUR: (ws, i) => {
    ws.send("3");
    logger.info(`${YELLOW}Heartbeat sent to BLUR [${i}]${RESET}`);
  },
};

function sendHeartbeats() {
  for (const { name, connections, url } of marketplaces) {
    logger.debug(JSON.stringify({
      name,
      connections: connections.map((conn) => ({
        exists: !!conn,
        status: conn?.readyState,
        open: conn?.readyState === WebSocket.OPEN
      }))
    }));

    const sendHeartbeat = heartbeatStrategy[name];

    // Ensure we always have the target number of connections
    for (let i = 0; i < CONNECTIONS_PER_MARKETPLACE; i++) {
      const ws = connections[i];

      if (!ws || ws.readyState !== WebSocket.OPEN) {
        // Connection is missing or not open, create/recreate it
        logger.info(`Connection ${i + 1} for ${name} is ${!ws ? 'missing' : 'closed'}, reconnecting...`);
        connectToMarketplace({ name, connections, url }, i);
      } else {
        // Connection is open, send heartbeat
        try {
          sendHeartbeat(ws, i);
        } catch (error) {
          logger.error(`Error sending heartbeat to ${name} [${i}]: ${error}`);
          // Reconnect on heartbeat error
          connectToMarketplace({ name, connections, url }, i);
        }
      }
    }
  }
}


setInterval(sendHeartbeats, 20000);


async function connectToMarketplace(marketplace: MarketplaceConnection, index: number, attempt: number = 1) {
  const { name, url } = marketplace;

  const wsOptions = {
    headers: {
      "accept-language": "en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7",
      "cache-control": "no-cache",
      "pragma": "no-cache",
      "sec-websocket-extensions": "permessage-deflate; client_max_window_bits",
      "sec-websocket-key": "BCPMf2FACdb85ryHEhhNoA==",
      "sec-websocket-version": "13"
    },
    handshakeTimeout: 10000 // 10 second handshake timeout
  };

  const ws = new WebSocket(url, wsOptions);

  ws.on("open", async () => {
    // Track connection time
    (ws as any).connectedAt = Date.now();

    logger.info(`CONNECTED TO ${name} (Connection ${index + 1}):
  - Attempt: ${attempt}
  - URL: ${url}
  - Time: ${new Date().toISOString()}`);

    marketplace.connections[index] = ws;

    // Re-subscribe to all client collections for this marketplace
    await verifyAndRestoreSubscriptions(name);
  });

  ws.on("message", (data: string) => {
    try {
      // Update connection quality metrics
      if (connectionQuality[name]) {
        connectionQuality[name].messages++;
        connectionQuality[name].lastMessage = Date.now();
      }

      // Handle MagicEden pong responses
      if (name === MAGICEDEN && data === "pong") {
        logger.debug(`${MAGENTA}Received pong from MAGICEDEN [${index}]${RESET}`);
        return;
      }

      handleMarketplaceMessages(name, data, ws);
    } catch (error) {
      if (connectionQuality[name]) {
        connectionQuality[name].errors++;
      }
      console.error(`Error handling message from ${name}:`, error);
    }
  });

  ws.on("error", (error: any) => {
    // Clear the connection reference on error
    if (marketplace.connections[index] === ws) {
      marketplace.connections[index] = null as any;
    }

    // Check for specific authentication errors
    if (error.message && error.message.includes('403')) {
      logger.error(`🚨 AUTHENTICATION ERROR FOR ${name}:
  - HTTP 403 Forbidden - Invalid or unauthorized API key
  - This indicates the API key doesn't have Stream API access enabled
  - For OpenSea: Contact OpenSea support to enable Stream API access
  - Current API Key: ${name === OPENSEA ? (OPENSEA_API_KEY || 'NOT_SET') : 'N/A'}
  - Time: ${new Date().toISOString()}`);
    } else {
      logger.error(`WEBSOCKET ERROR FOR ${name} (Connection ${index + 1}):
  - Error: ${error.message}
  - Code: ${error.code || 'No error code'}
  - Type: ${error.type || error.name || 'Unknown'}
  - Syscall: ${error.syscall || 'N/A'}
  - Address: ${error.address || 'N/A'}
  - Port: ${error.port || 'N/A'}
  - Time: ${new Date().toISOString()}`);
    }

    // Don't call handleReconnect here as it will be called by the close event
  });

  ws.on("close", (code: number, reason: Buffer) => {
    // Clear the connection reference to prevent using closed connections
    if (marketplace.connections[index] === ws) {
      marketplace.connections[index] = null as any;
    }

    // Determine close reason
    let closeType = 'Unknown';
    switch (code) {
      case 1000: closeType = 'Normal Closure'; break;
      case 1001: closeType = 'Going Away'; break;
      case 1002: closeType = 'Protocol Error'; break;
      case 1003: closeType = 'Unsupported Data'; break;
      case 1006: closeType = 'Abnormal Closure (No close frame)'; break;
      case 1007: closeType = 'Invalid Frame Payload'; break;
      case 1008: closeType = 'Policy Violation'; break;
      case 1009: closeType = 'Message Too Big'; break;
      case 1010: closeType = 'Mandatory Extension'; break;
      case 1011: closeType = 'Internal Server Error'; break;
      case 1015: closeType = 'TLS Handshake'; break;
    }

    logger.warn(`DISCONNECTED FROM ${name} (Connection ${index + 1}):
  - Close Code: ${code} (${closeType})
  - Close Reason: ${reason || 'No reason provided'}
  - Connection Duration: ${Date.now() - ((ws as any).connectedAt || Date.now())}ms
  - Reconnect Attempt: ${attempt}`);

    handleReconnect(marketplace, index, attempt);
  });
}

function handleReconnect(marketplace: MarketplaceConnection, index: number, attempt: number) {
  const maxAttempts = 5;
  const maxDelay = 300000; // Max 5 minutes

  if (attempt > maxAttempts) {
    logger.error(`Max reconnection attempts reached for ${marketplace.name}:
  - Connection: ${index + 1}
  - Total Attempts: ${attempt}
  - Scheduling final retry in: ${maxDelay / 1000} seconds
  - Time: ${new Date().toISOString()}`);

    // Schedule a final retry after a longer delay
    setTimeout(async () => {
      logger.info(`Final retry attempt for ${marketplace.name} (Connection ${index + 1})`);
      await connectToMarketplace(marketplace, index, 1); // Reset attempt counter
    }, maxDelay);
    return;
  }

  // Exponential backoff with jitter
  const baseDelay = Math.pow(2, attempt) * 1000;
  const jitter = Math.random() * 1000; // Add up to 1 second of jitter
  const delay = Math.min(baseDelay + jitter, maxDelay);

  logger.info(`Scheduling reconnection for ${marketplace.name}:
  - Connection: ${index + 1}
  - Attempt: ${attempt}/${maxAttempts}
  - Delay: ${Math.round(delay / 1000)} seconds
  - Base Delay: ${baseDelay / 1000}s, Jitter: ${Math.round(jitter)}ms
  - Next Attempt At: ${new Date(Date.now() + delay).toISOString()}`);

  setTimeout(async () => {
    logger.info(`Executing reconnection for ${marketplace.name} (Connection ${index + 1}, Attempt ${attempt + 1})`);
    await connectToMarketplace(marketplace, index, attempt + 1);
  }, delay);
}

// Validate API keys before starting connections
if (!OPENSEA_API_KEY || OPENSEA_API_KEY === 'your_opensea_api_key_here') {
  logger.error('🚨 OPENSEA_API_KEY is not set or using placeholder value!');
  logger.error('Please set a valid OpenSea API key in .env file');
  logger.error('Contact OpenSea support to enable Stream API access for your key');
} else {
  logger.info(`OpenSea API Key configured: ${OPENSEA_API_KEY.substring(0, 8)}...${OPENSEA_API_KEY.substring(-4)}`);
}

marketplaces.forEach(async marketplace => {
  for (let i = 0; i < CONNECTIONS_PER_MARKETPLACE; i++) {
    await connectToMarketplace(marketplace, i);
  }
});

// Send periodic status messages to all connected clients
setInterval(async () => {
  const status = {
    type: 'server_status',
    timestamp: Date.now(),
    marketplaces: marketplaces.map(m => ({
      name: m.name,
      connections: m.connections.filter(c => c?.readyState === WebSocket.OPEN).length
    })),
    subscriptions: await getActiveSubscriptionCount()
  };

  await broadcastToClients(status);
}, 30000); // Every 30 seconds


interface WebSocketWithClient extends WebSocket {
  clientId?: string;
}

wss.on("connection", async (ws: WebSocketWithClient, req: http.IncomingMessage) => {
  let pingTimeout: NodeJS.Timeout;

  // Log new client connection
  const clientIp = req.socket.remoteAddress || 'unknown';
  logger.info(`New client connection from ${clientIp} at ${new Date().toISOString()}`);

  // Add ping interval
  const pingInterval = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.ping();
    }
  }, 30000); // Send ping every 30 seconds

  // Handle pong responses
  ws.on('pong', () => {
    // Reset the connection timeout on pong received
    if (pingTimeout) clearTimeout(pingTimeout);
    pingTimeout = setTimeout(() => {
      logger.warn(`Client ${ws.clientId || 'unknown'} ping timeout, terminating connection`);
      ws.terminate();
    }, PING_TIMEOUT);
  });

  ws.on("message", async (message: string) => {
    try {
      // Rate limit logging for ping messages
      const isPing = message.includes('"ping"');
      if (!isPing || Math.random() < 0.01) { // Only log 1% of ping messages
        logger.info(`NEW MESSAGE RECEIVED FROM CLIENT: ${message}`);
      }
      const data: ClientMessage = JSON.parse(message);
      const clientId = data.clientId;

      if (!data || !data?.event) {
        logger.error(`Invalid message received from client: ${message}`);
        return;
      }

      if (data?.event?.toLowerCase() === "ping") {
        // Validate clientId exists
        if (!clientId || clientId === 'undefined') {
          logger.warn(`Ping received without valid clientId: ${clientId}`);
          ws.send(JSON.stringify({
            type: 'error',
            message: 'clientId is required for ping'
          }));
          return;
        }

        // Rate limit ping messages
        const now = Date.now();
        const lastPing = clientLastPing.get(clientId) || 0;
        if (now - lastPing < PING_RATE_LIMIT) {
          logger.debug(`Rate limiting ping from client ${clientId}`);
          return; // Ignore rapid pings
        }
        clientLastPing.set(clientId, now);

        // Only register if this is a new connection or reconnection
        const isNewConnection = !ws.clientId || ws.clientId !== clientId;
        if (isNewConnection) {
          await registerClient(ws, clientId, Date.now().toString());
          logger.info(`Client ${clientId} registered/reconnected from ${ws.clientId ? 'existing' : 'new'} connection`);
        } else {
          // Just update last ping time without incrementing reconnect count
          const existing = clientData.get(clientId);
          if (existing) {
            existing.lastPing = Date.now();
          }
        }

        clientLastResponse.set(clientId, Date.now());

        // send pong back to the client with timestamp
        ws.send(JSON.stringify({
          type: 'pong',
          timestamp: data.timestamp, // <-- echo client's timestamp
          message: 'pong 🏓 — you rang, mortal?'
        }));
      }
      if (data?.event?.toLowerCase() === "join_the_party") {
        await subscribeToCollection(data, ws);
        clientLastResponse.set(clientId, Date.now());
      }
      if (data?.event?.toLowerCase() === "leave_the_party") {
        await unsubscribeFromCollection(data);
        clientLastResponse.set(clientId, Date.now());
      }
      if (data?.event?.toLowerCase() === "join_listings") {
        await subscribeToListings(data, ws);
        clientLastResponse.set(clientId, Date.now());
      }
      if (data?.event?.toLowerCase() === "leave_listings") {
        await unsubscribeFromListings(data);
        clientLastResponse.set(clientId, Date.now());
      }
      if (data?.event?.toLowerCase() === "get_subscriptions") {
        // Return current subscriptions for this client
        const subscriptions = getClientSubscriptions(clientId);
        const listingSubscriptions = getClientListingSubscriptions(clientId);
        ws.send(JSON.stringify({
          type: 'subscription_sync',
          clientId,
          subscriptions: Array.from(subscriptions),
          listingSubscriptions: Array.from(listingSubscriptions)
        }));
      }
    } catch (error) {
      logger.error(`Failed to parse message: ${error}`);
    }
  });

  ws.on("error", (error: any) => {
    logger.error(`Client WebSocket error:
  - Client ID: ${ws.clientId || 'unknown'}
  - IP: ${clientIp}
  - Error: ${error.message}
  - Code: ${error.code || 'No error code'}
  - Type: ${error.type || error.name || 'Unknown'}`);
  });

  ws.on("close", async (code: number, reason: Buffer) => {
    const disconnectReason = reason.toString() || 'No reason provided';
    logger.info(`Client disconnected:
  - Client ID: ${ws.clientId || 'unknown'}
  - IP: ${clientIp}
  - Close Code: ${code}
  - Close Reason: ${disconnectReason}
  - Time: ${new Date().toISOString()}`);

    // Track disconnection in memory
    if (ws.clientId) {
      const existing = clientData.get(ws.clientId);
      if (existing) {
        existing.disconnectCount++;
      }
    }

    // Clean up intervals and timeouts
    clearInterval(pingInterval);
    if (pingTimeout) clearTimeout(pingTimeout);
    if (ws.clientId) {
      // Remove from client WebSocket map
      clientWebSocketMap.delete(ws.clientId);

      // Remove from rate limit map
      clientLastPing.delete(ws.clientId);

      // Clean up subscription indexes
      const subscriptions = getClientSubscriptions(ws.clientId);
      const listingSubscriptions = getClientListingSubscriptions(ws.clientId);

      for (const sub of subscriptions) {
        try {
          const parsed = JSON.parse(sub);
          updateSubscriptionIndex(ws.clientId, parsed.contractAddress, parsed.slug, false);
        } catch (e) { }
      }

      for (const sub of listingSubscriptions) {
        try {
          const parsed = JSON.parse(sub);
          updateSubscriptionIndex(ws.clientId, parsed.contractAddress, parsed.slug, false);
        } catch (e) { }
      }

      // Client data cleanup handled by automatic cleanup intervals
      clientLastResponse.delete(ws.clientId);
      // Clear subscription cache
      subscriptionCache.delete(ws.clientId);

      // Track dropped messages before clearing queue
      const queue = clientMessageQueues.get(ws.clientId);
      if (queue && queue.length > 0) {
        // Track dropped messages for disconnected client (in-memory for low latency)
        incrementClientStats(ws.clientId, 'droppedMessages', queue.length);
        logger.debug(`Client ${ws.clientId} disconnected with ${queue.length} queued messages - tracked as dropped`);
      }

      // Clear message queue
      clientMessageQueues.delete(ws.clientId);
    }
  });
});

async function subscribeToCollection(data: ClientMessage, ws: WebSocketWithClient) {
  const clientId = data.clientId;
  const subscription = { slug: data.slug, contractAddress: data.contractAddress };

  await registerClient(ws, clientId, Date.now().toString());

  const subscriptions = getClientSubscriptions(clientId);
  subscriptions.add(JSON.stringify(subscription));
  saveClientSubscriptions(clientId, subscriptions);

  marketplaces.forEach(marketplace => {
    marketplace.connections.forEach(ws => {
      if (!ws || ws.readyState !== WebSocket.OPEN) return;

      if (marketplace.name === OPENSEA) {
        const openseaMessage = {
          topic: `collection:${data.slug}`,
          event: "phx_join",
          "payload": {},
          ref: 0,
        };
        ws.send(JSON.stringify(openseaMessage));
      }
      if (marketplace.name === MAGICEDEN) {
        const magicEdenMessage = {
          type: "subscribeCollection",
          constraint: {
            chain: "ethereum",
            collectionSymbol: data.contractAddress.toLowerCase(),
          },
        };
        ws.send(JSON.stringify(magicEdenMessage));
      }
      if (marketplace.name === BLUR) {
        const bidStats = `4224["subscribe",["${data.contractAddress}.denormalizer.bidStats"]]`
        const eventsCreated = `4218["subscribe",["${data.contractAddress}.feeds.activity.eventsCreated"]]`

        ws.send(bidStats);
        ws.send(eventsCreated);
      }
    });
  });
}

async function subscribeToListings(data: ClientMessage, ws: WebSocketWithClient) {
  const clientId = data.clientId;
  const subscription = { slug: data.slug, contractAddress: data.contractAddress };

  await registerClient(ws, clientId, Date.now().toString());

  const listingSubscriptions = getClientListingSubscriptions(clientId);
  listingSubscriptions.add(JSON.stringify(subscription));
  saveClientListingSubscriptions(clientId, listingSubscriptions);

  marketplaces.forEach(marketplace => {
    marketplace.connections.forEach(ws => {
      if (!ws || ws.readyState !== WebSocket.OPEN) return;

      if (marketplace.name === OPENSEA) {
        // OpenSea listing subscription - different topic for listings
        const openseaListingMessage = {
          topic: `collection:${data.slug}`,
          event: "phx_join",
          payload: {},
          ref: 0,
        };
        ws.send(JSON.stringify(openseaListingMessage));
      }
      if (marketplace.name === MAGICEDEN) {
        // MagicEden listing subscription
        const magicEdenListingMessage = {
          type: "subscribeCollection",
          constraint: {
            chain: "ethereum",
            collectionSymbol: data.contractAddress.toLowerCase(),
          },
        };
        ws.send(JSON.stringify(magicEdenListingMessage));
      }
      if (marketplace.name === BLUR) {
        const eventsCreated = `4212["subscribe",["${data.contractAddress}.feeds.activity.eventsCreated"]]`
        ws.send(eventsCreated);
      }
    });
  });
}

async function unsubscribeFromCollection(data: ClientMessage) {
  const clientId = data.clientId;
  const subscription = { slug: data.slug, contractAddress: data.contractAddress };

  if (!data.slug && !data.contractAddress) return;

  const subscriptions = getClientSubscriptions(clientId);
  subscriptions.delete(JSON.stringify(subscription));

  let isAnyoneElseSubscribed = false;
  const clientKeys = await redis.keys('subscriptions:*');

  for (const key of clientKeys) {
    const otherClientId = key.split(':')[1];
    if (otherClientId !== clientId) {
      const otherSubscriptions = getClientSubscriptions(otherClientId);
      isAnyoneElseSubscribed = Array.from(otherSubscriptions).some(sub => {
        const parsedSub = JSON.parse(sub);
        return (parsedSub.contractAddress?.toLowerCase() === subscription.contractAddress?.toLowerCase()) ||
          (parsedSub.slug === subscription.slug);
      });
      if (isAnyoneElseSubscribed) break;
    }
  }

  if (!isAnyoneElseSubscribed) {
    marketplaces.forEach(marketplace => {
      marketplace.connections.forEach(ws => {
        if (!ws || ws.readyState !== WebSocket.OPEN) return;

        if (marketplace.name === OPENSEA) {
          ws.send(JSON.stringify({
            topic: `collection:${subscription.slug}`,
            event: "phx_leave",
            payload: {},
            ref: 0,
          }));
        }

        if (marketplace.name === MAGICEDEN) {
          const magicEdenUnsubscribeMessage = {
            type: "unsubscribeCollection",
            constraint: {
              chain: "ethereum",
              collectionSymbol: subscription?.contractAddress?.toLowerCase(),
            },
          };
          ws.send(JSON.stringify(magicEdenUnsubscribeMessage));
        }

        if (marketplace.name === BLUR) {
          const unsubscribeFromBlurStats = `4229["unsubscribe",["${data.contractAddress}.denormalizer.bidStats"]]`
          const unsubscribeFromBlurBidLevels = `4328["unsubscribed",{"rooms":[["${data.contractAddress}.COLLECTION.{}.denormalizer.bidLevels"]],"success":true}]`
          ws.send(unsubscribeFromBlurStats);
          ws.send(unsubscribeFromBlurBidLevels);

        }
      });
    });
  }

  if (subscriptions.size === 0) {
    await redis.del(`client:${clientId}`);
    await redis.del(`subscriptions:${clientId}`);
  } else {
    saveClientSubscriptions(clientId, subscriptions);
  }
}

async function unsubscribeFromListings(data: ClientMessage) {
  const clientId = data.clientId;
  const subscription = { slug: data.slug, contractAddress: data.contractAddress };

  if (!data.slug && !data.contractAddress) return;

  const listingSubscriptions = getClientListingSubscriptions(clientId);
  listingSubscriptions.delete(JSON.stringify(subscription));

  let isAnyoneElseSubscribed = false;
  const clientKeys = await redis.keys('listing_subscriptions:*');

  for (const key of clientKeys) {
    const otherClientId = key.split(':')[1];
    if (otherClientId !== clientId) {
      const otherListingSubscriptions = getClientListingSubscriptions(otherClientId);
      isAnyoneElseSubscribed = Array.from(otherListingSubscriptions).some(sub => {
        const parsedSub = JSON.parse(sub);
        return (parsedSub.contractAddress?.toLowerCase() === subscription.contractAddress?.toLowerCase()) ||
          (parsedSub.slug === subscription.slug);
      });
      if (isAnyoneElseSubscribed) break;
    }
  }

  if (!isAnyoneElseSubscribed) {
    marketplaces.forEach(marketplace => {
      marketplace.connections.forEach(ws => {
        if (!ws || ws.readyState !== WebSocket.OPEN) return;

        if (marketplace.name === OPENSEA) {
          ws.send(JSON.stringify({
            topic: `collection:${subscription.slug}:listings`,
            event: "phx_leave",
            payload: {},
            ref: 0,
          }));
        }

        if (marketplace.name === MAGICEDEN) {
          const magicEdenUnsubscribeMessage = {
            type: "unsubscribeListings",
            constraint: {
              chain: "ethereum",
              collectionSymbol: subscription?.contractAddress?.toLowerCase(),
            },
          };
          ws.send(JSON.stringify(magicEdenUnsubscribeMessage));
        }

        if (marketplace.name === BLUR) {
          const blurUnsubscribeAskLevels = `4229["unsubscribe",["${data.contractAddress}.COLLECTION.{}.denormalizer.askLevels"]]`
          const unsubscribeFromBlurAskStats = `4229["unsubscribe",["${data.contractAddress}.denormalizer.askStats"]]`
          const unsubscribeFromListingEvents = `4229["unsubscribe",["${data.contractAddress}.feeds.activity.listingsCreated"]]`
          ws.send(blurUnsubscribeAskLevels);
          ws.send(unsubscribeFromBlurAskStats);
          ws.send(unsubscribeFromListingEvents);
        }
      });
    });
  }

  if (listingSubscriptions.size === 0) {
    await redis.del(`listing_subscriptions:${clientId}`);
  } else {
    saveClientListingSubscriptions(clientId, listingSubscriptions);
  }
}

function handleMarketplaceMessages(name: string, data: string, ws: WebSocket) {
  const dataString = data.toString();
  let processedCount = 0;
  let errorCount = 0;

  let message: any;
  let contractAddress: string | undefined;
  let slug: string | undefined;
  let uniqueOrderId: string | undefined;

  // Update connection health
  updateConnectionHealth(ws);

  try {
    switch (name) {
      case BLUR:
        const blurMessage = handleBlurMessages(dataString, ws);

        message = blurMessage?.message;
        contractAddress = blurMessage?.contractAddress;
        break;

      case OPENSEA:
        const openSeaMessage = handleOpenSeaMessages(dataString);
        message = openSeaMessage?.message;
        contractAddress = openSeaMessage?.contractAddress;
        slug = openSeaMessage?.slug;
        uniqueOrderId = openSeaMessage?.uniqueOrderId;
        break;

      case MAGICEDEN:
        const magicEdenMessage = handleMagicEdenMessages(dataString);
        message = magicEdenMessage?.message;
        contractAddress = magicEdenMessage?.contractAddress;
        slug = magicEdenMessage?.slug;
        uniqueOrderId = magicEdenMessage?.uniqueOrderId;
        break;

      default:
        return;
    }


    if (message && (contractAddress || slug)) {
      processedCount++;
      // Track collection statistics (in-memory for low latency)
      const collectionKey = slug || contractAddress || 'unknown';
      incrementCollectionStats(`${name}:${collectionKey}`);
      forwardMessageToClients(message, name, contractAddress, slug);

      // Track successful message processing (in-memory for low latency)
      incrementWebsocketStats(name, 'processed');
    } else {
      // Log why message was not forwarded
      logger.debug(`Message from ${name} not forwarded - Message: ${!!message}, Contract: ${contractAddress}, Slug: ${slug}`);
      if (message && !contractAddress && !slug) {
        logger.warn(`Message has no contract address or slug from ${name}: ${JSON.stringify(message).substring(0, 200)}`);
      }
    }
  } catch (error) {
    errorCount++;

    const errorDetails = {
      marketplace: name,
      error: error instanceof Error ? error.message : String(error),
      errorStack: error instanceof Error ? error.stack : undefined,
      dataPreview: dataString.substring(0, 500),
      dataLength: dataString.length,
      timestamp: new Date().toISOString()
    };

    logger.error(`Error processing message from ${name}:`, errorDetails);

    // Error tracking now handled by in-memory stats

    // Track error metrics (in-memory for low latency)
    incrementWebsocketStats(name, 'errors');

    // Attempt recovery for common parsing issues
    if (dataString.includes('{') && dataString.includes('}')) {
      const jsonMatch = dataString.match(/\{[\s\S]*\}/);
      if (jsonMatch) {
        logger.info(`Attempting to recover JSON from malformed message`);
        try {
          return handleMarketplaceMessages(name, jsonMatch[0], ws);
        } catch (recoveryError) {
          logger.error(`Recovery attempt failed: ${recoveryError}`);
        }
      }
    }
  }
}

// Ultra-fast message forwarding with O(1) lookups and pre-serialized messages
function forwardMessageToClients(message: any, name: string, contractAddress?: string, slug?: string) {
  const startTime = Date.now();
  // Use high-performance reverse indexes for O(1) client lookup
  const { bidClients, listingClients } = getInterestedClients(contractAddress, slug);

  // Combine all interested clients
  const allInterestedClients = new Set<string>();
  bidClients.forEach(clientId => allInterestedClients.add(clientId));
  listingClients.forEach(clientId => allInterestedClients.add(clientId));

  if (allInterestedClients.size === 0) {
    logger.debug(`No subscribed clients for message from ${name}`);
    return;
  }

  logger.debug(`Forwarding message from ${name} to ${allInterestedClients.size} clients. Contract: ${contractAddress}, Slug: ${slug}`);

  let messagesDelivered = 0;

  // Pre-calculate base message data once
  const baseTimestamp = Date.now();

  // Pre-serialize message templates to avoid repeated JSON.stringify
  const bidMessageTemplate = Array.isArray(message) ?
    message.map(msg => JSON.stringify({ ...msg, marketplace: name, subscriptionType: 'bids', timestamp: baseTimestamp })) :
    JSON.stringify({ ...message, marketplace: name, subscriptionType: 'bids', timestamp: baseTimestamp });

  const listingMessageTemplate = Array.isArray(message) ?
    message.map(msg => JSON.stringify({ ...msg, marketplace: name, subscriptionType: 'listings', timestamp: baseTimestamp })) :
    JSON.stringify({ ...message, marketplace: name, subscriptionType: 'listings', timestamp: baseTimestamp });

  const bothMessageTemplate = Array.isArray(message) ?
    message.map(msg => JSON.stringify({ ...msg, marketplace: name, subscriptionType: 'both', timestamp: baseTimestamp })) :
    JSON.stringify({ ...message, marketplace: name, subscriptionType: 'both', timestamp: baseTimestamp });

  // Process all clients with fire-and-forget pattern for maximum speed
  allInterestedClients.forEach(clientId => {
    const ws = clientWebSocketMap.get(clientId);
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      return;
    }

    try {
      // Use high-performance reverse indexes - no need for cached subscriptions
      const isBidSubscribed = bidClients.has(clientId);
      const isListingSubscribed = listingClients.has(clientId);

      if (isBidSubscribed || isListingSubscribed) {
        // Select pre-serialized template based on subscription type
        const messageTemplate = isBidSubscribed && isListingSubscribed ? bothMessageTemplate :
          isBidSubscribed ? bidMessageTemplate : listingMessageTemplate;

        // Fire-and-forget sync send for maximum speed
        if (Array.isArray(messageTemplate)) {
          for (const serializedMessage of messageTemplate) {
            // Sync send - no await for maximum speed
            if (ws.readyState === WebSocket.OPEN && ws.bufferedAmount < MAX_BUFFER_SIZE / 2) {
              try {
                ws.send(serializedMessage);
                messagesDelivered++;
                incrementClientStats(clientId, 'messagesReceived');
              } catch (sendError) {
                // Queue on send failure (async operation in background)
                queueMessageForClient(clientId, JSON.parse(serializedMessage));
              }
            } else {
              // Queue if buffer full (async operation in background)
              queueMessageForClient(clientId, JSON.parse(serializedMessage));
            }
          }
        } else {
          // Sync send - no await for maximum speed
          if (ws.readyState === WebSocket.OPEN && ws.bufferedAmount < MAX_BUFFER_SIZE / 2) {
            try {
              ws.send(messageTemplate);
              messagesDelivered++;
              incrementClientStats(clientId, 'messagesReceived');
            } catch (sendError) {
              // Queue on send failure (async operation in background)
              queueMessageForClient(clientId, JSON.parse(messageTemplate));
            }
          } else {
            // Queue if buffer full (async operation in background)
            queueMessageForClient(clientId, JSON.parse(messageTemplate));
          }
        }
      }
    } catch (error) {
      logger.error(`Error processing client ${clientId}: ${error}`);
    }
  });

  const elapsed = Date.now() - startTime;
  if (elapsed > 50) {
    logger.warn(`Slow message forwarding from ${name}: ${elapsed}ms for ${allInterestedClients.size} clients`);
  }

  if (messagesDelivered > 0) {
    logger.debug(`Delivered ${messagesDelivered} messages from ${name} in ${elapsed}ms`);
  }
}

function handleOpenSeaMessages(dataString: string) {
  const jsonStartIndex = dataString.search(/[\[{]/);
  if (jsonStartIndex !== -1) {
    try {
      const message = JSON.parse(dataString);
      if (typeof message === "object") {
        if (message.event === "item_received_bid") {
          const slug = message.payload?.payload?.collection?.slug;
          const contractAddress = message.payload?.payload?.asset_contract_criteria?.address;
          const uniqueOrderId = message.payload?.payload?.order_hash;

          // Check for duplicate early
          if (uniqueOrderId && isDuplicateMessage(`OPENSEA:${uniqueOrderId}`)) {
            logger.debug(`Duplicate OpenSea bid detected, order ID: ${uniqueOrderId}, skipping`);
            return null;
          }

          const transformedMessage = { ...message, eventType: "BID_CREATED" }
          return { message: transformedMessage, contractAddress, slug, uniqueOrderId };

        } else if (message.event === "trait_offer") {
          const slug = message.payload?.payload?.collection?.slug;
          const contractAddress = message.payload?.payload?.asset_contract_criteria?.address;
          const uniqueOrderId = message.payload?.payload?.order_hash;

          // Check for duplicate early
          if (uniqueOrderId && isDuplicateMessage(`OPENSEA:${uniqueOrderId}`)) {
            logger.debug(`Duplicate OpenSea trait offer detected, order ID: ${uniqueOrderId}, skipping`);
            return null;
          }

          const transformedMessage = { ...message, eventType: "BID_CREATED" }
          return { message: transformedMessage, contractAddress, slug, uniqueOrderId };
        } else if (message.event === "collection_offer") {
          const slug = message.payload?.payload?.collection?.slug;
          const contractAddress = message.payload?.payload?.asset_contract_criteria?.address;
          const uniqueOrderId = message.payload?.payload?.order_hash;

          // Check for duplicate early
          if (uniqueOrderId && isDuplicateMessage(`OPENSEA:${uniqueOrderId}`)) {
            logger.debug(`Duplicate OpenSea collection offer detected, order ID: ${uniqueOrderId}, skipping`);
            return null;
          }

          const transformedMessage = { ...message, eventType: "BID_CREATED" }
          return { message: transformedMessage, contractAddress, slug, uniqueOrderId };
        } else if (message.event === "item_sold") {
          const slug = message?.payload?.payload?.collection?.slug;
          const contractAddress = message?.payload?.payload?.item?.nft_id?.split('/')[1];
          const uniqueOrderId = message.payload?.payload?.order_hash;

          // Check for duplicate early
          if (uniqueOrderId && isDuplicateMessage(`OPENSEA:${uniqueOrderId}`)) {
            logger.debug(`Duplicate OpenSea item sold detected, order ID: ${uniqueOrderId}, skipping`);
            return null;
          }

          // Transform to normalized format similar to Blur
          const transformedMessage = {
            id: message.payload?.payload?.order_hash,
            tokenId: message.payload?.payload?.item?.nft_id?.split('/').pop(),
            eventType: "ITEM_SOLD",
            marketplace: "OPENSEA",
            price: message.payload?.payload?.sale_price ?
              (parseFloat(message.payload.payload.sale_price) / Math.pow(10, 18)).toString() : "0",
            priceUnit: message.payload?.payload?.payment_token?.symbol || "ETH",
            fromAddress: message.payload?.payload?.maker?.address,
            toAddress: message.payload?.payload?.taker?.address,
            createdAt: message.payload?.payload?.event_timestamp || new Date().toISOString(),
            transactionHash: message.payload?.payload?.transaction?.hash,
            imageUrl: message.payload?.payload?.item?.metadata?.image_url,
            contractAddress: contractAddress
          };

          return { message: transformedMessage, contractAddress, slug, uniqueOrderId }
        } else if (message.event === "item_listed") {
          const slug = message?.payload?.payload?.collection?.slug;
          const contractAddress = message.payload?.payload?.asset_contract_criteria?.address;
          const uniqueOrderId = message.payload?.payload?.order_hash;

          // Check for duplicate early
          if (uniqueOrderId && isDuplicateMessage(`OPENSEA:${uniqueOrderId}`)) {
            logger.debug(`Duplicate OpenSea item listed detected, order ID: ${uniqueOrderId}, skipping`);
            return null;
          }

          // Transform to normalized format
          const transformedMessage = {
            id: message.payload?.payload?.order_hash,
            marketplace: "OPENSEA",
            eventType: "LISTING_CREATED",
            fromAddress: message.payload?.payload?.maker?.address,
            priceUnit: message.payload?.payload?.payment_token?.symbol || "ETH",
            createdAt: message.payload?.payload?.listing_date || new Date().toISOString(),
            transactionHash: null,
            tokenId: message.payload?.payload?.item?.nft_id?.split('/').pop(),
            imageUrl: message.payload?.payload?.item?.metadata?.image_url,
            price: message.payload?.payload?.base_price ?
              (parseFloat(message.payload.payload.base_price) / Math.pow(10, message.payload?.payload?.payment_token?.decimals || 18)).toString() :
              "0",
            contractAddress: message.payload?.payload?.item?.nft_id?.split('/')[1]
          };

          return { message: transformedMessage, contractAddress, slug, uniqueOrderId }
        }
      }
    } catch (err) {
      console.error("Error parsing OpenSea message:", err);
    }
  }
  return null;
}

function handleMagicEdenMessages(dataString: string) {
  const jsonStartIndex = dataString.search(/[\[{]/);
  if (jsonStartIndex !== -1) {
    try {
      const message = JSON.parse(dataString);

      // Early return if domain is not from MagicEden
      const domain = message.data?.source?.domain;
      if (domain && !domain.startsWith("magiceden.")) {
        return null;
      }

      if (
        typeof message === "object" && message.event === "bid.created"
      ) {
        const slug = message.payload?.payload?.collection?.slug;
        const contractAddress = message.tags?.contract || message.payload?.payload?.asset_contract_criteria?.address;
        const uniqueOrderId = message.data?.id;

        // Check for duplicate early
        if (uniqueOrderId && isDuplicateMessage(`MAGICEDEN:${uniqueOrderId}`)) {
          logger.debug(`Duplicate MagicEden bid detected, order ID: ${uniqueOrderId}, skipping`);
          return null;
        }

        const transformedMessage = { ...message, eventType: "BID_CREATED" }

        return { message: transformedMessage, contractAddress, slug, uniqueOrderId }
      }
      else if (
        typeof message === "object" && message.event === "ask.created"
      ) {
        const slug = message.payload?.payload?.collection?.slug;
        const contractAddress = message.tags?.contract || message.data?.contract;
        const uniqueOrderId = message.data?.id;

        // Check for duplicate early
        if (uniqueOrderId && isDuplicateMessage(`MAGICEDEN:${uniqueOrderId}`)) {
          logger.debug(`Duplicate MagicEden ask detected, order ID: ${uniqueOrderId}, skipping`);
          return null;
        }

        // Transform to normalized format
        const transformedMessage = {
          id: message.data?.id,
          marketplace: "MAGICEDEN",
          eventType: "LISTING_CREATED",
          fromAddress: message.data?.maker || message.tags?.maker,
          priceUnit: message.data?.price?.currency?.symbol || "ETH",
          createdAt: message.data?.createdAt || message.published_at ? new Date(message.published_at).toISOString() : new Date().toISOString(),
          transactionHash: null,
          tokenId: message.data?.criteria?.data?.token?.tokenId,
          imageUrl: message.data?.criteria?.data?.token?.image,
          price: message.data?.price?.amount?.decimal?.toString() || "0",
          contractAddress: contractAddress
        };

        return { message: transformedMessage, contractAddress, slug, uniqueOrderId }
      }
    } catch (error) {
      console.error("Error parsing MagicEden message:", error);
    }
  }
  return null;
}

function handleBlurMessages(dataString: string, ws: WebSocket) {

  let message: any;
  let contractAddress: string | undefined;

  if (dataString === "2") {
    ws.send('3');
    return null;
  }
  else if (dataString.startsWith('42')) {
    try {
      const jsonString: any = dataString.replace('42', '').trim();
      message = JSON.parse(jsonString);
      const parsedMessage = JSON.parse(jsonString);

      const salesEvent: BlurSaleEvent[] = message[1]?.items?.filter((event: any) => event.eventType === "SALE")

      if (salesEvent?.length > 0) {
        const contractAddress = message[1].contractAddress
        // Return sale events as individual messages
        return {
          message: salesEvent.map((event: BlurSaleEvent) => ({
            ...event,
            eventType: "ITEM_SOLD",
            contractAddress: contractAddress
          })),
          contractAddress
        };
      }

      // Handle events from feeds.activity.eventsCreated (for listings)
      if (parsedMessage[0] && parsedMessage[0].includes('.feeds.activity.eventsCreated')) {
        contractAddress = parsedMessage[0].split('.')[0];
        const eventsData = parsedMessage[1];
        if (eventsData && eventsData.items && Array.isArray(eventsData.items)) {
          const listingEvents = eventsData.items.filter((item: any) => item.eventType !== "SALE");

          // Transform each listing event to normalized format
          const transformedItems = listingEvents.map((item: any) => ({
            id: item.id,
            marketplace: item.marketplace || "BLUR",
            eventType: "LISTING_CREATED",
            fromAddress: item.fromAddress,
            priceUnit: item.priceUnit,
            createdAt: item.createdAt,
            transactionHash: item.transactionHash,
            tokenId: item.tokenId,
            imageUrl: item.imageUrl,
            price: item.price,
            contractAddress: eventsData.contractAddress || contractAddress
          }));

          // Return with individual transformed messages
          return {
            message: transformedItems,
            contractAddress
          };
        }
      }
      contractAddress = parsedMessage[1]?.contractAddress;
      const transformedMessage = { ...message, eventType: "BID_CREATED" }
      return { message: transformedMessage, contractAddress };
    } catch (error) {
      console.error(`Failed to parse Blur message:`, error);
    }
  }
  else {
    const jsonStartIndex = dataString.search(/[\[{]/);
    if (jsonStartIndex !== -1) {
      const cleanMessage = dataString.substring(jsonStartIndex);
      try {
        message = JSON.parse(cleanMessage);
        if (message.sid && message.upgrades) {
          ws.send("40");
        }
      } catch (error) {
        console.error("Failed to parse Blur message:", error);
      }
    }
  }
  return null;
}

interface ClientMessage {
  slug: string;
  event: string;
  topic: string;
  clientId: string;
  contractAddress: string;
  timestamp: number;
  messageId?: string;
  batchId?: string;
  messageCount?: number;
  stats?: {
    messagesReceived: number;
    lastMessageId: string;
    lastMessageTime: number;
  };
}


// Duplicate cleanup function removed - using in-memory version above

// Clean up stale subscription cache entries
setInterval(() => {
  const now = Date.now();
  for (const [clientId, cache] of subscriptionCache) {
    if (now - cache.lastUpdated > CACHE_TTL * 2) {
      subscriptionCache.delete(clientId);
    }
  }
}, 30000); // Run every 30 seconds

// Flush queued messages for slow clients
setInterval(async () => {
  for (const [clientId, queue] of clientMessageQueues) {
    const ws = clientWebSocketMap.get(clientId);
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      // Client disconnected, track dropped messages and clear their queue
      if (queue.length > 0) {
        // Track dropped messages for disconnected client (in-memory for low latency)
        incrementClientStats(clientId, 'droppedMessages', queue.length);
        logger.debug(`Client ${clientId} disconnected, dropped ${queue.length} queued messages`);
      }
      clientMessageQueues.delete(clientId);
      continue;
    }

    if (queue.length === 0) continue;

    // Check if buffer has space
    if (ws.bufferedAmount < MAX_BUFFER_SIZE / 2) {
      // Process up to 10 messages at a time
      const messagesToSend = queue.splice(0, 10);

      for (const msg of messagesToSend) {
        try {
          ws.send(JSON.stringify(msg));
        } catch (error) {
          // Put message back at front of queue
          queue.unshift(msg);
          logger.debug(`Failed to send queued message to ${clientId}, re-queuing`);
          break; // Stop processing this client
        }
      }

      if (queue.length > 0) {
        logger.debug(`Client ${clientId} still has ${queue.length} queued messages`);
      }
    }
  }
}, 100); // Check every 100ms

// Set up periodic connection scaling  
setInterval(adjustMarketplaceConnections, 60000); // Run every 60 seconds

// Add periodic subscription health check
setInterval(async () => {
  const activeClients = await redis.keys('client:*');
  const bidSubscriptions = await redis.keys('subscriptions:*');
  const listingSubscriptions = await redis.keys('listing_subscriptions:*');

  // Get detailed subscription counts
  let totalBidSubs = 0;
  let totalListingSubs = 0;
  let orphanedSubscriptions = 0;

  for (const key of bidSubscriptions) {
    const count = await redis.scard(key);
    totalBidSubs += count;

    // Check for orphaned subscriptions
    const clientId = key.split(':')[1];
    const clientExists = await redis.exists(`client:${clientId}`);
    if (!clientExists) {
      orphanedSubscriptions++;
      logger.warn(`Found orphaned bid subscription for disconnected client: ${clientId}`);
      // Clean up orphaned subscriptions
      await redis.del(key);
      await redis.del(`listing_subscriptions:${clientId}`);
    }
  }

  for (const key of listingSubscriptions) {
    const count = await redis.scard(key);
    totalListingSubs += count;

    // Check for orphaned subscriptions (if not already checked)
    const clientId = key.split(':')[1];
    const clientExists = await redis.exists(`client:${clientId}`);
    if (!clientExists && !bidSubscriptions.includes(`subscriptions:${clientId}`)) {
      orphanedSubscriptions++;
      logger.warn(`Found orphaned listing subscription for disconnected client: ${clientId}`);
      // Clean up orphaned subscriptions
      await redis.del(key);
    }
  }

  logger.info('Subscription health check', {
    activeClients: activeClients.length,
    bidSubscriptionClients: bidSubscriptions.length,
    listingSubscriptionClients: listingSubscriptions.length,
    totalBidSubscriptions: totalBidSubs,
    totalListingSubscriptions: totalListingSubs,
    orphanedSubscriptionsRemoved: orphanedSubscriptions,
    timestamp: new Date().toISOString()
  });
}, 60000); // Every minute

// Add quality reporting
setInterval(() => {
  Object.entries(connectionQuality).forEach(([marketplace, stats]) => {
    const timeSinceLastMessage = stats.lastMessage ? Date.now() - stats.lastMessage : null;
    const errorRate = stats.messages > 0 ? (stats.errors / stats.messages) * 100 : 0;

    logger.info(`${marketplace} connection quality`, {
      messagesReceived: stats.messages,
      errors: stats.errors,
      errorRate: `${errorRate.toFixed(2)}%`,
      timeSinceLastMessage,
      status: timeSinceLastMessage && timeSinceLastMessage > 300000 ? 'STALE' : 'HEALTHY'
    });
  });
}, 300000); // Every 5 minutes

// Comprehensive memory cleanup
async function performMemoryCleanup() {
  const now = Date.now();

  // 1. Clean message deduplication cache
  const dedupCutoff = now - DEDUP_CACHE_TTL;
  for (const [hash, timestamp] of messageDeduplicationCache) {
    if (timestamp < dedupCutoff) {
      messageDeduplicationCache.delete(hash);
    }
  }

  // 2. Clean subscription cache
  for (const [clientId, cache] of subscriptionCache) {
    if (now - cache.lastUpdated > CACHE_TTL * 10) {
      subscriptionCache.delete(clientId);
    }
  }

  // 3. Clean inactive message queues
  for (const [clientId] of clientMessageQueues) {
    if (!clientWebSocketMap.has(clientId)) {
      clientMessageQueues.delete(clientId);
      logger.debug(`Cleaned up queue for disconnected client ${clientId}`);
    }
  }

  // 4. Clean connection health map
  for (const [ws] of connectionHealthMap) {
    if (ws.readyState !== WebSocket.OPEN) {
      connectionHealthMap.delete(ws);
    }
  }

  // 5. Clean orphaned subscription indexes
  for (const [contract, clientIds] of contractSubscriptionIndex) {
    const validClients = new Set<string>();
    for (const clientId of clientIds) {
      if (clientWebSocketMap.has(clientId)) {
        validClients.add(clientId);
      }
    }
    if (validClients.size === 0) {
      contractSubscriptionIndex.delete(contract);
    } else {
      contractSubscriptionIndex.set(contract, validClients);
    }
  }

  for (const [slug, clientIds] of slugSubscriptionIndex) {
    const validClients = new Set<string>();
    for (const clientId of clientIds) {
      if (clientWebSocketMap.has(clientId)) {
        validClients.add(clientId);
      }
    }
    if (validClients.size === 0) {
      slugSubscriptionIndex.delete(slug);
    } else {
      slugSubscriptionIndex.set(slug, validClients);
    }
  }

  // 6. Log memory usage
  const memUsage = process.memoryUsage();
  logger.info('Memory cleanup complete', {
    heapUsed: Math.round(memUsage.heapUsed / 1024 / 1024) + 'MB',
    external: Math.round(memUsage.external / 1024 / 1024) + 'MB',
    deduplicationCacheSize: messageDeduplicationCache.size,
    activeClients: clientWebSocketMap.size,
    messageQueues: clientMessageQueues.size,
    contractIndexes: contractSubscriptionIndex.size,
    slugIndexes: slugSubscriptionIndex.size
  });
}

// Run cleanup every 30 minutes (1800 seconds)
setInterval(performMemoryCleanup, 1800000);


// Graceful shutdown handling
process.on('SIGTERM', async () => {
  logger.info('Shutting down gracefully...');

  // Notify clients
  await broadcastToClients({ type: 'server_shutdown', reconnectIn: 5000 });

  // Close marketplace connections
  for (const marketplace of marketplaces) {
    marketplace.connections.forEach(conn => conn?.close());
  }

  // Clear message queues
  clientMessageQueues.clear();

  // Wait for pending messages
  await new Promise(resolve => setTimeout(resolve, 2000));

  // Close server
  wss.close();
  server.close();

  // Redis connection removed - no longer needed

  process.exit(0);
});

process.on('SIGINT', async () => {
  logger.info('Received SIGINT, shutting down gracefully...');
  process.emit('SIGTERM' as any);
});

interface BlurSaleEvent {
  id: string;
  tokenId: string;
  imageUrl: string;
  fromAddress: string;
  toAddress: string;
  createdAt: string; // ISO date string
  transactionHash: string;
  blockHeight: number;
  blockHash: string;
  logIndex: number;
  eventType: 'SALE'; // If it could be more, use string instead
  price: string; // Use `number` if you convert it elsewhere
  priceUnit: string; // e.g. 'ETH'
  marketplace: 'BLUR'; // Use string if not always BLUR
  makerSide: 'BID' | 'ASK'; // If only two possible values
}