import { BoundedCache } from './bounded-cache';
import { logger, ErrorCategory } from './logger';

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
  getFloorPrice(collection: string, marketplace: string): number | null {
    const key = `${marketplace}:${collection}`.toLowerCase();
    const cached = this.floorCache.get(key);
    
    if (cached) {
      // Silent cache hit - no logging for performance
      return cached.price;
    }
    
    return null;
  }

  // Set floor price for a collection on a specific marketplace
  setFloorPrice(collection: string, marketplace: string, price: number) {
    const key = `${marketplace}:${collection}`.toLowerCase();
    this.floorCache.set(key, {
      price,
      marketplace,
      timestamp: Date.now()
    });
  }

  // Get collection details
  getCollectionDetails(collection: string): any | null {
    const key = collection.toLowerCase();
    const cached = this.detailsCache.get(key);
    
    if (cached?.details) {
      // Silent cache hit - no logging for performance
      return cached.details;
    }
    
    return null;
  }

  // Set collection details
  setCollectionDetails(collection: string, details: any) {
    const key = collection.toLowerCase();
    const existing = this.detailsCache.get(key) || { timestamp: Date.now() };
    
    this.detailsCache.set(key, {
      ...existing,
      details,
      timestamp: Date.now()
    });
  }

  // Get collection stats
  getCollectionStats(collection: string): any | null {
    const key = collection.toLowerCase();
    const cached = this.detailsCache.get(key);
    
    if (cached?.stats) {
      // Silent cache hit - no logging for performance
      return cached.stats;
    }
    
    return null;
  }

  // Set collection stats
  setCollectionStats(collection: string, stats: any) {
    const key = collection.toLowerCase();
    const existing = this.detailsCache.get(key) || { timestamp: Date.now() };
    
    this.detailsCache.set(key, {
      ...existing,
      stats,
      timestamp: Date.now()
    });
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
  invalidateCollection(collection: string) {
    const key = collection.toLowerCase();
    
    // Remove from details cache
    this.detailsCache.delete(key);
    
    // Remove all floor prices for this collection
    const floorKeys = Array.from(this.floorCache.keys())
      .filter(k => k.endsWith(`:${key}`));
    
    for (const floorKey of floorKeys) {
      this.floorCache.delete(floorKey);
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