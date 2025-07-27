import { BoundedCache } from './bounded-cache';
import redisClient, { getRedisConnection } from './redis';
import { logger, DatabaseOperation } from './logger';

interface CollectionData {
  floor?: number;
  details?: any;
  stats?: any;
  timestamp: number;
}

interface FloorPriceData {
  price: number;
  marketplace: string;
  timestamp: number;
}

class CollectionCache {
  // Floor prices by marketplace and collection
  private floorCache: BoundedCache<string, FloorPriceData>;
  
  // Collection details cache
  private detailsCache: BoundedCache<string, CollectionData>;
  
  // Batch request tracking to prevent duplicate API calls
  private pendingRequests: Map<string, Promise<any>> = new Map();
  
  constructor() {
    // 1000 items, 5 minute TTL for floor prices (they change frequently)
    this.floorCache = new BoundedCache<string, FloorPriceData>(1000, 5 * 60 * 1000);
    
    // 500 items, 30 minute TTL for collection details (they change less frequently)
    this.detailsCache = new BoundedCache<string, CollectionData>(500, 30 * 60 * 1000);
  }

  // Get floor price for a collection on a specific marketplace
  async getFloorPrice(collection: string, marketplace: string): Promise<number | null> {
    const memoryKey = `${marketplace}:${collection}`.toLowerCase();
    
    // L1: Check memory cache first
    const memoryCached = this.floorCache.get(memoryKey);
    if (memoryCached) {
      return memoryCached.price;
    }
    
    // L2: Check Redis cache
    const redisKey = `{floor:${marketplace}}:${collection}`.toLowerCase();
    try {
      const redis = getRedisConnection();
      if (redis && redisClient.isReady()) {
        const cached = await redis.get(redisKey);
        if (cached) {
          const data = JSON.parse(cached) as FloorPriceData;
          // Update memory cache with Redis data
          this.floorCache.set(memoryKey, data);
          return data.price;
        }
      }
    } catch (error) {
      // Log error but don't fail the operation
      await logger.logDatabaseError('Failed to get floor price from Redis', {
        databaseOperation: DatabaseOperation.READ,
        collection_name: 'redis_floor_cache',
        key: redisKey,
        marketplace,
        collection,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
    }
    
    return null;
  }

  // Set floor price for a collection on a specific marketplace
  async setFloorPrice(collection: string, marketplace: string, price: number): Promise<void> {
    const memoryKey = `${marketplace}:${collection}`.toLowerCase();
    const data: FloorPriceData = {
      price,
      marketplace,
      timestamp: Date.now()
    };
    
    // L1: Update memory cache
    this.floorCache.set(memoryKey, data);
    
    // L2: Update Redis cache
    const redisKey = `{floor:${marketplace}}:${collection}`.toLowerCase();
    try {
      const redis = getRedisConnection();
      if (redis && redisClient.isReady()) {
        // Set with 5 minute TTL
        await redis.setex(redisKey, 5 * 60, JSON.stringify(data));
      }
    } catch (error) {
      // Log error but don't fail the operation
      await logger.logDatabaseError('Failed to set floor price in Redis', {
        databaseOperation: DatabaseOperation.UPDATE,
        collection_name: 'redis_floor_cache',
        key: redisKey,
        marketplace,
        collection,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
    }
  }

  // Get collection details
  async getCollectionDetails(collection: string): Promise<any | null> {
    const memoryKey = collection.toLowerCase();
    
    // L1: Check memory cache first
    const memoryCached = this.detailsCache.get(memoryKey);
    if (memoryCached?.details) {
      return memoryCached.details;
    }
    
    // L2: Check Redis cache
    const redisKey = `{collection:details}:${collection}`.toLowerCase();
    try {
      const redis = getRedisConnection();
      if (redis && redisClient.isReady()) {
        const cached = await redis.get(redisKey);
        if (cached) {
          const data = JSON.parse(cached) as CollectionData;
          // Update memory cache with Redis data
          this.detailsCache.set(memoryKey, data);
          return data.details;
        }
      }
    } catch (error) {
      // Log error but don't fail the operation
      await logger.logDatabaseError('Failed to get collection details from Redis', {
        databaseOperation: DatabaseOperation.READ,
        collection_name: 'redis_collection_cache',
        key: redisKey,
        collection,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
    }
    
    return null;
  }

  // Set collection details
  async setCollectionDetails(collection: string, details: any): Promise<void> {
    const memoryKey = collection.toLowerCase();
    const existing = this.detailsCache.get(memoryKey) || { timestamp: Date.now() };
    const data: CollectionData = {
      ...existing,
      details,
      timestamp: Date.now()
    };
    
    // L1: Update memory cache
    this.detailsCache.set(memoryKey, data);
    
    // L2: Update Redis cache
    const redisKey = `{collection:details}:${collection}`.toLowerCase();
    try {
      const redis = getRedisConnection();
      if (redis && redisClient.isReady()) {
        // Set with 30 minute TTL
        await redis.setex(redisKey, 30 * 60, JSON.stringify(data));
      }
    } catch (error) {
      // Log error but don't fail the operation
      await logger.logDatabaseError('Failed to set collection details in Redis', {
        databaseOperation: DatabaseOperation.UPDATE,
        collection_name: 'redis_collection_cache',
        key: redisKey,
        collection,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
    }
  }

  // Get collection stats
  async getCollectionStats(collection: string): Promise<any | null> {
    const memoryKey = collection.toLowerCase();
    
    // L1: Check memory cache first
    const memoryCached = this.detailsCache.get(memoryKey);
    if (memoryCached?.stats) {
      return memoryCached.stats;
    }
    
    // L2: Check Redis cache
    const redisKey = `{collection:stats}:${collection}`.toLowerCase();
    try {
      const redis = getRedisConnection();
      if (redis && redisClient.isReady()) {
        const cached = await redis.get(redisKey);
        if (cached) {
          const data = JSON.parse(cached) as CollectionData;
          // Update memory cache with Redis data
          this.detailsCache.set(memoryKey, data);
          return data.stats;
        }
      }
    } catch (error) {
      // Log error but don't fail the operation
      await logger.logDatabaseError('Failed to get collection stats from Redis', {
        databaseOperation: DatabaseOperation.READ,
        collection_name: 'redis_collection_cache',
        key: redisKey,
        collection,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
    }
    
    return null;
  }

  // Set collection stats
  async setCollectionStats(collection: string, stats: any): Promise<void> {
    const memoryKey = collection.toLowerCase();
    const existing = this.detailsCache.get(memoryKey) || { timestamp: Date.now() };
    const data: CollectionData = {
      ...existing,
      stats,
      timestamp: Date.now()
    };
    
    // L1: Update memory cache
    this.detailsCache.set(memoryKey, data);
    
    // L2: Update Redis cache
    const redisKey = `{collection:stats}:${collection}`.toLowerCase();
    try {
      const redis = getRedisConnection();
      if (redis && redisClient.isReady()) {
        // Set with 30 minute TTL
        await redis.setex(redisKey, 30 * 60, JSON.stringify(data));
      }
    } catch (error) {
      // Log error but don't fail the operation
      await logger.logDatabaseError('Failed to set collection stats in Redis', {
        databaseOperation: DatabaseOperation.UPDATE,
        collection_name: 'redis_collection_cache',
        key: redisKey,
        collection,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
    }
  }

  // Batch fetch with deduplication
  async fetchWithDedup<T>(
    key: string,
    fetchFn: () => Promise<T>
  ): Promise<T> {
    // Check if there's already a pending request
    const pending = this.pendingRequests.get(key);
    if (pending) {
      // Silent deduplication - no logging for performance
      return pending;
    }

    // Create new request and track it
    const request = fetchFn()
      .then(result => {
        this.pendingRequests.delete(key);
        return result;
      })
      .catch(error => {
        this.pendingRequests.delete(key);
        throw error;
      });

    this.pendingRequests.set(key, request);
    return request;
  }

  // Invalidate all caches for a collection
  async invalidateCollection(collection: string): Promise<void> {
    const key = collection.toLowerCase();
    
    // Remove from memory caches
    this.detailsCache.delete(key);
    
    // Remove all floor prices for this collection from memory
    const floorKeys = Array.from(this.floorCache.keys())
      .filter(k => k.endsWith(`:${key}`));
    
    for (const floorKey of floorKeys) {
      this.floorCache.delete(floorKey);
    }
    
    // Remove from Redis
    try {
      const redis = getRedisConnection();
      if (redis && redisClient.isReady()) {
        // Remove collection details and stats
        await redis.del(`{collection:details}:${key}`);
        await redis.del(`{collection:stats}:${key}`);
        
        // Remove floor prices for all marketplaces
        const marketplaces = ['opensea', 'blur', 'magiceden'];
        for (const marketplace of marketplaces) {
          await redis.del(`{floor:${marketplace}}:${key}`);
        }
      }
    } catch (error) {
      // Log error but don't fail the operation
      await logger.logDatabaseError('Failed to invalidate collection in Redis', {
        databaseOperation: DatabaseOperation.DELETE,
        collection_name: 'redis_collection_cache',
        collection: key,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
    }
  }

  // Get cache statistics
  getStats() {
    return {
      floorCache: {
        size: this.floorCache.size,
        maxSize: 1000
      },
      detailsCache: {
        size: this.detailsCache.size,
        maxSize: 500
      },
      pendingRequests: this.pendingRequests.size
    };
  }

  // Clear all caches
  clear() {
    this.floorCache.clear();
    this.detailsCache.clear();
    this.pendingRequests.clear();
  }
}

// Export singleton instance
export const collectionCache = new CollectionCache();