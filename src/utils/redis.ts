import { config } from 'dotenv';
import Redis from 'ioredis';
import { errorLog, warnLog } from './debug-file-logger';

config()

// Track if Redis config has been logged to avoid duplicate logs
let redisConfigLogged = false;

// Read Redis configuration at runtime to ensure Docker env vars are available
export const getRedisConfig = () => {
  const host = process.env.REDIS_HOST || 'localhost';
  const port = parseInt(process.env.REDIS_PORT || '6379', 10);

  // In production Docker environment, ensure we use the service name
  if (process.env.NODE_ENV === 'production' && host === 'localhost') {
    warnLog('Redis', 'Production environment detected but REDIS_HOST not set, using "redis" service name');
    return { host: 'redis', port };
  }

  // Only log config once to avoid duplicate log entries
  if (!redisConfigLogged) {
    console.log(`[Redis] Configuration: host=${host}, port=${port}, env=${process.env.NODE_ENV}`);
    redisConfigLogged = true;
  }
  return { host, port };
};

const RETRY_MAX_ATTEMPTS = 30;
const INITIAL_RETRY_DELAY = 2000;

const getDefaultConfig = () => {
  const { host, port } = getRedisConfig();
  return {
    host,
    port,
    maxRetriesPerRequest: null,
    retryStrategy: (times: number) => {
      if (times > RETRY_MAX_ATTEMPTS) {
        return null;
      }
      const delay = Math.min(times * INITIAL_RETRY_DELAY, 20000);
      return delay;
    },
    enableReadyCheck: true,
    connectTimeout: 60000,
    // Add memory optimization settings
    dropBufferSupport: true,
    showFriendlyErrorStack: true,
    enableOfflineQueue: false,
    reconnectOnError: (err: Error) => {
      const targetErrors = ['READONLY', 'ECONNRESET', 'ETIMEDOUT', 'ECONNREFUSED'];
      return targetErrors.some(errType => err.message.includes(errType));
    }
  };
};

class RedisClient {
  private static _instance: RedisClient;
  private client: Redis | null = null;
  private isConnected: boolean = false;
  private connectionAttempts: number = 0;
  private readonly maxConnectionAttempts = 10;
  private connecting: boolean = false;

  // Getter for instance
  static get instance(): RedisClient | undefined {
    return RedisClient._instance;
  }

  private constructor() {
    this.connect();
  }

  public static getInstance(): RedisClient {
    if (!RedisClient._instance) {
      RedisClient._instance = new RedisClient();
    }
    return RedisClient._instance;
  }

  private async connect() {
    if (this.connecting || this.isConnected) {
      return;
    }

    this.connecting = true;

    if (!this.client) {
      try {
        const config = getDefaultConfig();
        console.log(`[Redis] Connecting to ${config.host}:${config.port}...`);
        
        this.client = new Redis({
          ...config,
          lazyConnect: true,
          enableAutoPipelining: false,
        });

        this.setupEventHandlers();

        // Explicitly connect after setup
        await this.client.connect();
      } catch (error) {
        this.isConnected = false;
        this.client = null;

        errorLog('Database', `[Redis] Failed to connect | attempts=${this.connectionAttempts}/${this.maxConnectionAttempts}`, error);

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
      // console.log('[Redis] Connection established');
    });

    this.client.on('error', async (err) => {
      errorLog('Database', `[Redis] Connection error | attempts=${this.connectionAttempts}`, err);
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
          warnLog('Database', `[Redis] Health check ping failed | ${err.message}`);
          this.isConnected = false;
          this.connect();
        });
      }
    }, 30000);
  }

  public getClient(): Redis {
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
const bullMQConnections = new Map<string, { redis: Redis; createdAt: number; lastHealthCheck: number }>();
let connectionIdCounter = 0;

// Create a new Redis connection for BullMQ with enhanced configuration
export const createBullMQConnection = async (identifier?: string): Promise<Redis> => {
  const connectionId = identifier || `bullmq-${++connectionIdCounter}`;
  // console.log(`[Redis] Creating BullMQ connection: ${connectionId}`);

  // Check if this is a queue events or worker connection that needs special handling
  // Both queue events and workers use blocking commands (BLPOP/BRPOP) which require special timeout configuration
  const isQueueEvents = identifier?.includes('events');
  const isWorker = identifier?.includes('worker');
  const needsBlockingConfig = isQueueEvents || isWorker;

  if (needsBlockingConfig) {
    // console.log(`[Redis] ${connectionId} is a ${isQueueEvents ? 'queue events' : 'worker'} connection - using blocking command configuration`);
  }
  

  try {
    const { host, port } = getRedisConfig();
    const redis = new Redis({
      host,
      port,
      
      // Retry strategy
      retryStrategy: (times: number) => {
        if (times > RETRY_MAX_ATTEMPTS) {
          errorLog('Redis', `BullMQ connection ${connectionId} exceeded max retry attempts`);
          return null;
        }

        // Use shorter delays for initial retries
        const delay = times <= 3 ? 1000 : Math.min(times * INITIAL_RETRY_DELAY, 20000);
        // console.log(`[Redis] BullMQ connection ${connectionId} retry attempt ${times}, delay: ${delay}ms`);
        return delay;
      },

      // Critical: disable offline queue to fail fast (except for blocking connections)
      enableOfflineQueue: needsBlockingConfig ? true : false,

      // Connection settings
      // Blocking connections don't need ready check as they use blocking operations
      enableReadyCheck: !needsBlockingConfig,
      connectTimeout: 10000,
      ...(needsBlockingConfig ? {} : { commandTimeout: 60000 }), // 60 second timeout for non-blocking connections

      // Retry configuration - BullMQ requires null for blocking operations
      maxRetriesPerRequest: null,

      // Performance settings
      enableAutoPipelining: false,

      // Keep-alive settings
      keepAlive: 30000,
      noDelay: true,

      // Show friendly error messages
      showFriendlyErrorStack: true,
    });

    // Set up event handlers before connecting
    redis.on('error', async (err) => {
      // Check if it's a timeout error
      const isTimeoutError = err.message.includes('Command timed out') || err.message.includes('ETIMEDOUT');

      if (isTimeoutError && needsBlockingConfig) {
        warnLog('Database', `[Redis] BullMQ connection ${connectionId} timeout (expected for ${isQueueEvents ? 'queue events' : 'worker'}) | ${err.message}`);
      } else {
        errorLog('Database', `[Redis] BullMQ connection ${connectionId} error | timeout=${isTimeoutError}`, err);
      }
    });

    redis.on('connect', () => {
      // console.log(`[Redis] BullMQ connection ${connectionId} connected`);
    });

    redis.on('ready', () => {
      // console.log(`[Redis] BullMQ connection ${connectionId} ready`);
    });

    redis.on('close', () => {
      // console.log(`[Redis] BullMQ connection ${connectionId} closed`);
      bullMQConnections.delete(connectionId);
    });

    redis.on('reconnecting', () => {
      // console.log(`[Redis] BullMQ connection ${connectionId} reconnecting...`);
    });

    redis.on('end', () => {
      // console.log(`[Redis] BullMQ connection ${connectionId} ended`);
      bullMQConnections.delete(connectionId);
    });

    // Check if already connected/connecting before calling connect
    if (redis.status === 'ready') {
      // console.log(`[Redis] BullMQ connection ${connectionId} already ready`);
    } else if (redis.status === 'connecting' || redis.status === 'connect') {
      // console.log(`[Redis] BullMQ connection ${connectionId} already connecting, waiting...`);
      // Wait for connection to complete
      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error(`Connection timeout waiting for ${connectionId}`));
        }, 15000);

        redis.once('ready', () => {
          clearTimeout(timeout);
          resolve();
        });

        redis.once('error', (err) => {
          clearTimeout(timeout);
          reject(err);
        });
      });
    } else {
      // Only connect if not already connected/connecting
      await redis.connect();
    }

    // Ensure redis is ready
    if (redis.status !== 'ready') {
      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error(`BullMQ connection ${connectionId} timed out waiting for ready state`));
        }, 15000);

        redis.once('ready', () => {
          clearTimeout(timeout);
          resolve();
        });

        redis.once('error', (err) => {
          clearTimeout(timeout);
          reject(err);
        });
      });
    }

    // Verify connection with a ping
    try {
      await redis.ping();
      // console.log(`[Redis] BullMQ connection ${connectionId} verified with ping`);
    } catch (pingError) {
      errorLog('Database', `[Redis] BullMQ connection ${connectionId} ping failed`, pingError);
      throw pingError;
    }

    // Track connection
    bullMQConnections.set(connectionId, {
      redis,
      createdAt: Date.now(),
      lastHealthCheck: Date.now()
    });

    // console.log(`[Redis] BullMQ connection ${connectionId} successfully created. Total connections: ${bullMQConnections.size}`);
    return redis;

  } catch (error) {
    errorLog('Database', `[Redis] Failed to create BullMQ connection ${connectionId}`, error);
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
    status: info.redis.status || 'unknown'
  }));
};

// Health check for all BullMQ connections
export const checkBullMQConnectionsHealth = async () => {
  const results: { [key: string]: { healthy: boolean; error?: string } } = {};

  for (const [id, info] of bullMQConnections.entries()) {
    try {
      await info.redis.ping();
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
  // console.log(`[Redis] Closing ${bullMQConnections.size} BullMQ connections...`);

  const promises: Promise<void>[] = [];

  for (const [id, info] of bullMQConnections.entries()) {
    promises.push(
      info.redis.quit()
        .then(() => console.log(`[Redis] Closed BullMQ connection: ${id}`))
        .catch(err => errorLog('Redis', `Error closing BullMQ connection ${id}`, err))
    );
  }

  await Promise.all(promises);
  bullMQConnections.clear();
  // console.log('[Redis] All BullMQ connections closed');
};

// Helper function to check if an error is a Redis error
export const isRedisError = (error: unknown): boolean => {
  if (!(error instanceof Error)) return false;

  const redisErrorPatterns = [
    'READONLY',
    'ECONNRESET',
    'ETIMEDOUT',
    'ECONNREFUSED',
    'Connection is closed',
    'Connection lost',
  ];

  return redisErrorPatterns.some(pattern => error.message.includes(pattern));
};

// Helper function to handle Redis errors with automatic recovery
export const handleRedisError = async (error: unknown, context: string): Promise<void> => {
  if (!isRedisError(error)) return;

  errorLog('Redis', `Error in ${context}`, error);
};

// Retry wrapper for Redis operations with exponential backoff
export const withRedisRetry = async <T>(
  operation: () => Promise<T>,
  context: string,
  maxRetries: number = 3
): Promise<T | null> => {
  let lastError: Error | null = null;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error: any) {
      lastError = error;
      
      // Check if it's a redis error
      if (isRedisError(error)) {
        warnLog('Redis', `Error in ${context} (attempt ${attempt}/${maxRetries}): ${error.message}`);

        // Don't retry if we've exhausted attempts
        if (attempt === maxRetries) {
          errorLog('Redis', `Max retries reached for ${context}`);
          return null;
        }
        
        // Exponential backoff
        const delay = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
        // console.log(`[Redis] Retrying ${context} after ${delay}ms...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      } else {
        // Non-redis errors, don't retry
        throw error;
      }
    }
  }

  // If we get here, all retries failed
  errorLog('Redis', `All retries exhausted for ${context}`, lastError);
  return null;
};