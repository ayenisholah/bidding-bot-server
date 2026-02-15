import { config } from 'dotenv';
import Redis, { Cluster, ClusterNode } from 'ioredis';
import { errorLog, warnLog } from './debug-file-logger';

config()

// Track if Redis config has been logged to avoid duplicate logs
let redisConfigLogged = false;

// Read Redis configuration at runtime to ensure Docker env vars are available
export const getRedisConfig = () => {
  // Check for cluster nodes (JSON array) - takes precedence
  const redisNodesEnv = process.env.REDIS_NODES;

  if (redisNodesEnv) {
    try {
      const nodes = JSON.parse(redisNodesEnv);
      if (Array.isArray(nodes) && nodes.length >= 3) {
        // Only log config once to avoid duplicate log entries
        if (!redisConfigLogged) {
          console.log(`[Redis] Cluster Configuration: ${nodes.length} nodes | env=${process.env.NODE_ENV}`);
          redisConfigLogged = true;
        }
        return { cluster: true, nodes };
      } else {
        warnLog('Redis', `REDIS_NODES must be an array with at least 3 nodes, got ${nodes?.length || 0}. Falling back to single instance.`);
      }
    } catch (error) {
      warnLog('Redis', `Failed to parse REDIS_NODES: ${error instanceof Error ? error.message : String(error)}. Falling back to single instance.`);
    }
  }

  // Fallback to single instance (current behavior)
  const host = process.env.REDIS_HOST || 'localhost';
  const port = parseInt(process.env.REDIS_PORT || '6379', 10);

  // In production Docker environment, ensure we use the service name
  if (process.env.NODE_ENV === 'production' && host === 'localhost') {
    warnLog('Redis', 'Production environment detected but REDIS_HOST not set, using "redis" service name');
    return { cluster: false, host: 'redis', port };
  }

  // Only log config once to avoid duplicate log entries
  if (!redisConfigLogged) {
    console.log(`[Redis] Single Instance Configuration: host=${host}, port=${port}, env=${process.env.NODE_ENV}`);
    redisConfigLogged = true;
  }
  return { cluster: false, host, port };
};

const RETRY_MAX_ATTEMPTS = 30;
const INITIAL_RETRY_DELAY = 2000;


/**
 * DEPRECATED: RedisClient class removed
 *
 * Redis is now BullMQ-only. All non-BullMQ Redis usage has been migrated to in-memory LRU caches.
 * Use createBullMQConnection() for BullMQ queue connections.
 *
 * Migration completed: 2026-02-06
 * - Distributed locks: Migrated to in-memory LRU cache (lock.ts)
 * - Health monitoring: Use checkBullMQConnectionsHealth() instead
 * - General Redis access: No longer needed
 */

// DEPRECATED: Kept for backwards compatibility during migration
// These exports will throw errors if used, forcing code to be updated
class RedisClient {
  public static getInstance(): never {
    throw new Error('RedisClient.getInstance() is deprecated - Redis is BullMQ-only. Use createBullMQConnection() for BullMQ connections.');
  }

  public getClient(): never {
    throw new Error('redisClient.getClient() is deprecated - Redis is BullMQ-only. Use createBullMQConnection() for BullMQ connections.');
  }

  public isReady(): never {
    throw new Error('redisClient.isReady() is deprecated - use checkBullMQConnectionsHealth() instead.');
  }
}

const redisClient = new RedisClient();

// DEPRECATED: These exports will throw errors if used
export default redisClient;
export const getRedisConnection = (): never => {
  throw new Error('getRedisConnection() is deprecated - Redis is BullMQ-only. Use createBullMQConnection() for BullMQ connections.');
};

// Connection tracking for BullMQ
const bullMQConnections = new Map<string, { redis: Redis | Cluster; createdAt: number; lastHealthCheck: number }>();
let connectionIdCounter = 0;

// Create a new Redis connection for BullMQ with enhanced configuration
export const createBullMQConnection = async (identifier?: string): Promise<Redis | Cluster> => {
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
    const config = getRedisConfig();
    let redis: Redis | Cluster;

    // Base Redis options (common to both single instance and cluster)
    const baseRedisOptions = {
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
    };

    if ('cluster' in config && config.cluster) {
      // Redis Cluster mode
      redis = new Redis.Cluster(config.nodes as ClusterNode[], {
        redisOptions: baseRedisOptions,
        // Cluster-specific options
        scaleReads: 'slave',  // Read from replicas when possible
        maxRedirections: 16,
        retryDelayOnFailover: 100,
        retryDelayOnClusterDown: 300,
        slotsRefreshTimeout: 2000,
      });
      // console.log(`[Redis] Created cluster connection ${connectionId} with ${config.nodes.length} nodes`);
    } else {
      // Single instance mode (existing code)
      redis = new Redis({
        host: config.host,
        port: config.port,
        ...baseRedisOptions,
      });
      // console.log(`[Redis] Created single instance connection ${connectionId} to ${config.host}:${config.port}`);
    }

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
  const results: { [key: string]: { healthy: boolean; error?: string; mode?: string } } = {};

  for (const [id, info] of bullMQConnections.entries()) {
    try {
      // Check if this is a cluster connection
      const isCluster = 'isCluster' in info.redis && info.redis.isCluster;

      if (isCluster) {
        // For cluster, check cluster info command
        const clusterInfo = await info.redis.call('CLUSTER', 'INFO') as string;
        const isHealthy = clusterInfo.includes('cluster_state:ok');

        if (!isHealthy) {
          warnLog('Redis', `Cluster ${id} is not healthy: ${clusterInfo.split('\n')[0]}`);
        }

        results[id] = {
          healthy: isHealthy,
          mode: 'cluster',
          ...(isHealthy ? {} : { error: 'Cluster state not OK' })
        };
      } else {
        // For single instance, use ping
        await info.redis.ping();
        results[id] = { healthy: true, mode: 'single' };
      }

      info.lastHealthCheck = Date.now();
    } catch (error) {
      results[id] = {
        healthy: false,
        mode: 'isCluster' in info.redis && info.redis.isCluster ? 'cluster' : 'single',
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