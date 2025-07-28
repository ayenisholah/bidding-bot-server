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
    const targetError = 'READONLY';
    if (err.message.includes(targetError)) {
      return true;
    }
    return false;
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
          scaleReads: 'all',
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

// Create a new Redis Cluster connection for BullMQ
export const createBullMQConnection = async () => {
  const nodes = await getRedisNodes();
  const cluster = new Redis.Cluster(nodes, {
    clusterRetryStrategy: (times: number) => {
      const delay = Math.min(times * 2000, 20000);
      return delay;
    },
    enableOfflineQueue: true,
    redisOptions: {
      enableReadyCheck: true,
      maxRetriesPerRequest: null,
    },
    natMap: REDIS_NET_MAP,
    slotsRefreshTimeout: 30000,
  });
  
  // Wait for the cluster to be ready
  await new Promise<void>((resolve, reject) => {
    cluster.once('ready', () => resolve());
    cluster.once('error', (err) => reject(err));
  });
  
  return cluster;
};
