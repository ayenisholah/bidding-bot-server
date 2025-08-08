import { config } from 'dotenv';
import Redis, { Cluster } from 'ioredis';
import { logger, DatabaseOperation } from './logger';

config()

const REDIS_NODES = process.env.REDIS_NODES as string

const getRedisNodes = async () => {
  if (!REDIS_NODES) {
    await logger.logDatabaseConnectionError('REDIS_NODES is not defined in the environment variables', {
      operation: 'redis_initialization',
      errorCode: 'MISSING_CONFIG'
    });
    throw new Error('REDIS_NODES is not defined in the environment variables');
  }
  return JSON.parse(REDIS_NODES)
};

const REDIS_NET_MAP = process.env.REDIS_NET_MAP ? JSON.parse(process.env.REDIS_NET_MAP) : null

const RETRY_MAX_ATTEMPTS = 30;
const INITIAL_RETRY_DELAY = 2000;

const defaultConfig = {
  maxRetriesPerRequest: null,
  retryStrategy: (times: number) => {
    const delay = Math.min(times * INITIAL_RETRY_DELAY, 20000);
    return delay;
  },
  clusterRetryStrategy: (times: number) => {
    if (times > RETRY_MAX_ATTEMPTS) {
      return null;
    }
    const delay = Math.min(times * INITIAL_RETRY_DELAY, 20000);
    return delay;
  },
  redisOptions: {
    enableReadyCheck: true,
    maxRetriesPerRequest: null,
    connectTimeout: 30000,
    retryStrategy: (times: number) => {
      const delay = Math.min(times * INITIAL_RETRY_DELAY, 20000);
      return delay;
    }
  },
  // Add memory optimization settings
  dropBufferSupport: true,
  slotsRefreshTimeout: 30000,
  showFriendlyErrorStack: true,
  enableOfflineQueue: true,
  reconnectOnError: (err: Error) => {
    const targetErrors = ['READONLY', 'ECONNRESET', 'ETIMEDOUT', 'ECONNREFUSED', 'CLUSTERDOWN'];
    return targetErrors.some(errType => err.message.includes(errType));
  }
};

class RedisClient {
  private static instance: RedisClient;
  private client: Cluster | null = null;
  private isConnected: boolean = false;
  private connectionAttempts: number = 0;
  private readonly maxConnectionAttempts = 10;
  private connecting: boolean = false;

  private constructor() {
    this.connect();
  }

  public static getInstance(): RedisClient {
    if (!RedisClient.instance) {
      RedisClient.instance = new RedisClient();
    }
    return RedisClient.instance;
  }

  private async connect() {
    if (this.connecting || this.isConnected) {
      return;
    }

    this.connecting = true;

    if (!this.client) {
      try {
        const nodes = await getRedisNodes();
        this.client = new Redis.Cluster(nodes, {
          ...defaultConfig,
          scaleReads: 'master',
          redisOptions: {
            ...defaultConfig.redisOptions,
            lazyConnect: true,
            // Disable auto-discovery of nodes
            enableAutoPipelining: false,
          },
          enableOfflineQueue: true,
          slotsRefreshTimeout: 30000,
          dnsLookup: (address, callback) => callback(null, address),
          natMap: REDIS_NET_MAP,
          // Only use the nodes we explicitly provide
          slotsRefreshInterval: -1,
          // Prevent connection to default Redis
          lazyConnect: true,
        });

        this.setupEventHandlers();

        // Explicitly connect after setup
        await this.client.connect();
      } catch (error) {
        this.isConnected = false;
        this.client = null;

        console.error('Failed to connect to Redis cluster:', error);

        // Always log Redis connection errors
        try {
          await logger.logDatabaseConnectionError('Failed to connect to Redis cluster', {
            operation: 'redis_cluster_connection',
            connectionAttempts: this.connectionAttempts,
            maxAttempts: this.maxConnectionAttempts,
            error: error as Error,
            errorMessage: error instanceof Error ? error.message : String(error)
          });
        } catch (logError) {
          // Fallback if logger fails
          console.error('Failed to log Redis error:', logError);
        }

        if (this.connectionAttempts < this.maxConnectionAttempts) {
          this.connectionAttempts++;
          setTimeout(() => {
            this.connecting = false;
            this.connect();
          }, 5000);
        } else {
          this.connecting = false;
          throw error;
        }
      }
    }
  }

  private setupEventHandlers() {
    if (!this.client) return;

    // Add connection pool monitoring
    this.client.on('ready', () => {
      this.isConnected = true;
      this.connecting = false;
      this.connectionAttempts = 0;
      // Redis cluster connection established
    });

    this.client.on('error', async (err) => {
      console.error('Redis cluster error:', err);

      // Only log to Better Stack if it's not a connection refused error to localhost:6379
      // This prevents recursive logging attempts
      if (!err.message.includes('ECONNREFUSED') || !err.message.includes('127.0.0.1:6379')) {
        try {
          await logger.logDatabaseError('Redis cluster error', {
            databaseOperation: DatabaseOperation.CONNECTION,
            collection_name: 'redis_cluster',
            error: err,
            errorMessage: err.message,
            connectionAttempts: this.connectionAttempts
          });
        } catch (logError) {
          console.error('Failed to log Redis error:', logError);
        }
      }
      this.isConnected = false;
      if (!this.connecting && this.connectionAttempts < this.maxConnectionAttempts) {
        this.connectionAttempts++;
        setTimeout(() => this.connect(), 5000);
      }
    });

    // Add periodic health check
    setInterval(() => {
      if (this.isConnected && this.client) {
        this.client.ping().catch(async err => {
          await logger.logDatabaseError('Redis health check failed', {
            databaseOperation: DatabaseOperation.CONNECTION,
            collection_name: 'redis_cluster',
            operation: 'health_check_ping',
            error: err,
            errorMessage: err.message
          });
          console.warn('Redis ping failed:', err);
          this.isConnected = false;
          this.connect();
        });
      }
    }, 30000);
  }

  public getClient(): Cluster {
    if (!this.client) {
      this.connect();
    }
    return this.client!;
  }

  public isReady(): boolean {
    return this.isConnected;
  }
}

const redisClient = RedisClient.getInstance();

// Export both the client instance and a getter for the actual Redis connection
export default redisClient;
export const getRedisConnection = () => redisClient.getClient();

// Connection tracking for BullMQ
const bullMQConnections = new Map<string, { cluster: Cluster; createdAt: number; lastHealthCheck: number }>();
let connectionIdCounter = 0;

// Create a new Redis Cluster connection for BullMQ with enhanced configuration
export const createBullMQConnection = async (identifier?: string): Promise<Cluster> => {
  const connectionId = identifier || `bullmq-${++connectionIdCounter}`;
  console.log(`[Redis] Creating BullMQ connection: ${connectionId}`);

  // Check if this is a queue events or worker connection that needs special handling
  // Both queue events and workers use blocking commands (BLPOP/BRPOP) which require special timeout configuration
  const isQueueEvents = identifier?.includes('events');
  const isWorker = identifier?.includes('worker');
  const needsBlockingConfig = isQueueEvents || isWorker;

  if (needsBlockingConfig) {
    console.log(`[Redis] ${connectionId} is a ${isQueueEvents ? 'queue events' : 'worker'} connection - using blocking command configuration`);
  }

  try {
    const nodes = await getRedisNodes();

    const cluster = new Redis.Cluster(nodes, {
      // Cluster-level retry strategy
      clusterRetryStrategy: (times: number, reason?: Error) => {
        // Log the retry reason
        if (reason) {
          console.log(`[Redis] BullMQ connection ${connectionId} retry ${times} due to: ${reason.message}`);
        }

        if (times > RETRY_MAX_ATTEMPTS) {
          console.error(`[Redis] BullMQ connection ${connectionId} exceeded max retry attempts`);
          return null;
        }

        // Use shorter delays for initial retries
        const delay = times <= 3 ? 1000 : Math.min(times * INITIAL_RETRY_DELAY, 20000);
        console.log(`[Redis] BullMQ connection ${connectionId} retry attempt ${times}, delay: ${delay}ms`);
        return delay;
      },

      // Critical: disable offline queue to fail fast (except for blocking connections)
      enableOfflineQueue: needsBlockingConfig ? true : false,

      // Connection pool configuration
      scaleReads: 'master',

      // Node-level Redis options
      redisOptions: {
        // Connection settings
        // Blocking connections don't need ready check as they use blocking operations
        enableReadyCheck: !needsBlockingConfig,
        connectTimeout: 10000, // 10 second connection timeout
        // Don't set commandTimeout for blocking connections - let ioredis handle blocking commands automatically
        ...(needsBlockingConfig ? {} : { commandTimeout: 5000 }), // Only set timeout for non-blocking connections

        // Retry configuration - BullMQ requires null for blocking operations
        maxRetriesPerRequest: null,


        // Performance settings
        enableAutoPipelining: false,

        // Keep-alive settings
        keepAlive: 30000,
        noDelay: true,

        // Don't set socket timeout - let ioredis handle it based on operation type
      },

      // NAT mapping for Docker environments
      natMap: REDIS_NET_MAP,

      // Cluster refresh settings
      slotsRefreshTimeout: 10000, // 10 seconds for all connections
      slotsRefreshInterval: 30000, // Refresh every 30 seconds

      // DNS resolution
      dnsLookup: (address, callback) => callback(null, address),


      // Show friendly error messages
      showFriendlyErrorStack: true,
    });

    // Set up event handlers before connecting
    cluster.on('error', (err) => {
      // Check if it's a timeout error
      const isTimeoutError = err.message.includes('Command timed out') || err.message.includes('ETIMEDOUT');

      if (isTimeoutError && needsBlockingConfig) {
        console.warn(`[Redis] BullMQ connection ${connectionId} timeout (expected for ${isQueueEvents ? 'queue events' : 'worker'}):`, err.message);
      } else {
        console.error(`[Redis] BullMQ connection ${connectionId} error:`, err.message);
        logger.logDatabaseError(`BullMQ Redis connection error: ${connectionId}`, {
          databaseOperation: DatabaseOperation.CONNECTION,
          collection_name: 'bullmq_redis',
          connectionId,
          error: err,
          errorMessage: err.message,
          isTimeoutError,
          isQueueEvents,
          isWorker,
          needsBlockingConfig
        });
      }
    });

    cluster.on('connect', () => {
      console.log(`[Redis] BullMQ connection ${connectionId} connected`);
    });

    cluster.on('ready', () => {
      console.log(`[Redis] BullMQ connection ${connectionId} ready`);
    });

    cluster.on('close', () => {
      console.log(`[Redis] BullMQ connection ${connectionId} closed`);
      bullMQConnections.delete(connectionId);
    });

    cluster.on('reconnecting', () => {
      console.log(`[Redis] BullMQ connection ${connectionId} reconnecting...`);
    });

    cluster.on('end', () => {
      console.log(`[Redis] BullMQ connection ${connectionId} ended`);
      bullMQConnections.delete(connectionId);
    });

    // Check if already connected/connecting before calling connect
    if (cluster.status === 'ready') {
      console.log(`[Redis] BullMQ connection ${connectionId} already ready`);
    } else if (cluster.status === 'connecting' || cluster.status === 'connect') {
      console.log(`[Redis] BullMQ connection ${connectionId} already connecting, waiting...`);
      // Wait for connection to complete
      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error(`Connection timeout waiting for ${connectionId}`));
        }, 15000);

        cluster.once('ready', () => {
          clearTimeout(timeout);
          resolve();
        });

        cluster.once('error', (err) => {
          clearTimeout(timeout);
          reject(err);
        });
      });
    } else {
      // Only connect if not already connected/connecting
      await cluster.connect();
    }

    // Ensure cluster is ready
    if (cluster.status !== 'ready') {
      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error(`BullMQ connection ${connectionId} timed out waiting for ready state`));
        }, 15000);

        cluster.once('ready', () => {
          clearTimeout(timeout);
          resolve();
        });

        cluster.once('error', (err) => {
          clearTimeout(timeout);
          reject(err);
        });
      });
    }

    // Verify connection with a ping
    try {
      await cluster.ping();
      console.log(`[Redis] BullMQ connection ${connectionId} verified with ping`);
    } catch (pingError) {
      console.error(`[Redis] BullMQ connection ${connectionId} ping failed:`, pingError);
      throw pingError;
    }

    // Track connection
    bullMQConnections.set(connectionId, {
      cluster,
      createdAt: Date.now(),
      lastHealthCheck: Date.now()
    });

    console.log(`[Redis] BullMQ connection ${connectionId} successfully created. Total connections: ${bullMQConnections.size}`);
    return cluster;

  } catch (error) {
    console.error(`[Redis] Failed to create BullMQ connection ${connectionId}:`, error);
    await logger.logDatabaseConnectionError(`Failed to create BullMQ connection: ${connectionId}`, {
      operation: 'bullmq_connection_creation',
      connectionId,
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error)
    });
    throw error;
  }
};

// Get all active BullMQ connections
export const getBullMQConnections = () => {
  return Array.from(bullMQConnections.entries()).map(([id, info]) => ({
    id,
    createdAt: info.createdAt,
    lastHealthCheck: info.lastHealthCheck,
    age: Date.now() - info.createdAt,
    status: info.cluster.status || 'unknown'
  }));
};

// Health check for all BullMQ connections
export const checkBullMQConnectionsHealth = async () => {
  const results: { [key: string]: { healthy: boolean; error?: string } } = {};

  for (const [id, info] of bullMQConnections.entries()) {
    try {
      await info.cluster.ping();
      info.lastHealthCheck = Date.now();
      results[id] = { healthy: true };
    } catch (error) {
      results[id] = {
        healthy: false,
        error: error instanceof Error ? error.message : 'Unknown error'
      };
    }
  }

  return results;
};

// Close all BullMQ connections gracefully
export const closeAllBullMQConnections = async () => {
  console.log(`[Redis] Closing ${bullMQConnections.size} BullMQ connections...`);

  const promises: Promise<void>[] = [];

  for (const [id, info] of bullMQConnections.entries()) {
    promises.push(
      info.cluster.quit()
        .then(() => console.log(`[Redis] Closed BullMQ connection: ${id}`))
        .catch(err => console.error(`[Redis] Error closing BullMQ connection ${id}:`, err))
    );
  }

  await Promise.all(promises);
  bullMQConnections.clear();
  console.log('[Redis] All BullMQ connections closed');
};
