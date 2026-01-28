import Redis from 'ioredis';
import { Queue } from 'bullmq';
import mongoose from 'mongoose';
import { MemorySnapshot, IMemorySnapshot } from '../models/memory-snapshot.model';
import { debugLog, errorLog, infoLog } from '../utils/debug-file-logger';

// Import caches
import { tokenTraitCache } from '../utils/token-trait-cache';
import { getCacheStats as getCollectionCacheStats } from '../utils/collection-cache';
import { tradingDisabledCache } from '../utils/trading-disabled-cache';
import { listingPriceCache } from '../utils/listing-price-cache';
import { jobFallbackStore } from '../utils/redis-job-fallback';
import { approvalCache } from '../utils/approval-cache';
import { rarityCache } from '../utils/rarity-cache';
import { getLogsCacheStats } from '../utils/logs-cache';
import { walletCache } from '../utils/wallet-cache';
import { marketplaceApprovalCache } from '../utils/marketplace-approval-cache';
import { getOrderCacheStats } from '../utils/order-cache';
import { getDecryptedKeyCacheStats } from '../utils/crypto';

// Import OpenSea caches
import {
  collectionDetailsLRU,
  floorPriceLRU,
  balanceLRU,
  traitFloorLRU
} from '../marketplace/opensea/cache';

// Import global state
import {
  bidStats,
  skipStats,
  errorStats,
  counterbidCounts,
  processedOwnBids,
  taskStatus,
  bestOffers,
  floorPrices,
  collectionFloorCache
} from '../state/global-state';

interface CacheStats {
  name: string;
  size: number;
  maxSize: number;
  ttl: number;
  estimatedMemoryBytes?: number;
}

interface MemorySnapshotDependencies {
  memoryCleanupService: any;
  processedMessages: any; // LRUCache
  traitsToRemove: Record<string, string[]>;
  removedTokens: Record<string, any>;
  tasksBeingRestarted: Set<string>;
  tasksBeingCancelled: Set<string>;
  tasksInCompletionProcess: Set<string>;
  failedTaskStartQueue: Map<string, any>;
  failedTaskStopQueue: Map<string, any>;
  queueingProgress: Map<string, any>;
  taskLocks: Map<string, any>;
  loopSchedulingLocks: Map<string, any>;
}

class MemorySnapshotService {
  private redis: Redis | null = null;
  private mainQueue: Queue | null = null;
  private blurQueue: Queue | null = null;
  private deps: MemorySnapshotDependencies | null = null;

  public initialize(redis: Redis, mainQueue: Queue, blurQueue: Queue, deps?: MemorySnapshotDependencies): void {
    this.redis = redis;
    this.mainQueue = mainQueue;
    this.blurQueue = blurQueue;
    if (deps) {
      this.deps = deps;
    }
    infoLog('MemorySnapshotService', 'Initialized with Redis and BullMQ queues');
  }

  /**
   * Take a complete memory snapshot
   */
  public async takeSnapshot(): Promise<IMemorySnapshot> {
    const startTime = Date.now();
    const errors: string[] = [];
    let errorCount = 0;

    try {
      infoLog('MemorySnapshot', 'Starting memory snapshot collection');

      // Collect all metrics in parallel where possible
      const [
        processMemory,
        redisMemory,
        inMemoryCaches,
        globalState,
        queues,
        mongoCollections,
        memoryCleanup,
        lruCaches,
        unboundedDataStructures
      ] = await Promise.all([
        Promise.resolve(this.collectProcessMemory()),
        this.collectRedisMemory().catch((err: Error) => {
          errorCount++;
          errors.push(`Redis: ${err.message}`);
          return this.getEmptyRedisMemory();
        }),
        this.collectCacheStats().catch((err: Error) => {
          errorCount++;
          errors.push(`Caches: ${err.message}`);
          return [];
        }),
        Promise.resolve(this.collectGlobalStateStats()).catch((err: Error) => {
          errorCount++;
          errors.push(`GlobalState: ${err.message}`);
          return this.getEmptyGlobalState();
        }),
        this.collectQueueStats().catch((err: Error) => {
          errorCount++;
          errors.push(`Queues: ${err.message}`);
          return this.getEmptyQueueStats();
        }),
        this.collectMongoStats().catch((err: Error) => {
          errorCount++;
          errors.push(`MongoDB: ${err.message}`);
          return [];
        }),
        Promise.resolve(this.collectMemoryCleanupStats()).catch((err: Error) => {
          errorCount++;
          errors.push(`MemoryCleanup: ${err.message}`);
          return this.getEmptyMemoryCleanupStats();
        }),
        Promise.resolve(this.collectLRUCachesStats()).catch((err: Error) => {
          errorCount++;
          errors.push(`LRUCaches: ${err.message}`);
          return this.getEmptyLRUCachesStats();
        }),
        Promise.resolve(this.collectUnboundedDataStructuresStats()).catch((err: Error) => {
          errorCount++;
          errors.push(`UnboundedDataStructures: ${err.message}`);
          return this.getEmptyUnboundedDataStructuresStats();
        })
      ]);

      const snapshotDurationMs = Date.now() - startTime;

      const snapshot = new MemorySnapshot({
        timestamp: new Date(),
        processMemory,
        redisMemory,
        inMemoryCaches,
        globalState,
        queues,
        mongoCollections,
        memoryCleanup,
        lruCaches,
        unboundedDataStructures,
        metadata: {
          snapshotDurationMs,
          errorCount,
          errors: errors.length > 0 ? errors : undefined
        }
      });

      await snapshot.save();

      infoLog('MemorySnapshot', `Snapshot completed in ${snapshotDurationMs}ms | errors=${errorCount}`);
      return snapshot;
    } catch (error) {
      errorLog('MemorySnapshot', 'Failed to create snapshot', error);
      throw error;
    }
  }

  /**
   * Collect Node.js process memory statistics
   */
  private collectProcessMemory() {
    const mem = process.memoryUsage();
    return {
      rss: mem.rss,
      heapTotal: mem.heapTotal,
      heapUsed: mem.heapUsed,
      heapUsedPercentage: (mem.heapUsed / mem.heapTotal) * 100,
      external: mem.external,
      arrayBuffers: mem.arrayBuffers
    };
  }

  /**
   * Collect Redis memory statistics by category
   */
  private async collectRedisMemory() {
    if (!this.redis) {
      throw new Error('Redis client not initialized');
    }

    const categories = {
      bullMQMain: { pattern: 'bull:BIDDING_BOT:*', keyCount: 0, totalMemoryBytes: 0, sampleKeys: [] as string[] },
      bullMQBlur: { pattern: 'bull:BLUR_BIDDING_QUEUE:*', keyCount: 0, totalMemoryBytes: 0, sampleKeys: [] as string[] },
      loopLocks: { pattern: 'nft-bot:next-loop:*', keyCount: 0, totalMemoryBytes: 0, sampleKeys: [] as string[] },
      openseaCollection: { pattern: 'opensea:collection:*', keyCount: 0, totalMemoryBytes: 0, sampleKeys: [] as string[] },
      openseaTraits: { pattern: 'opensea:traits:*', keyCount: 0, totalMemoryBytes: 0, sampleKeys: [] as string[] },
      websocketMetrics: { pattern: 'websocket:*', keyCount: 0, totalMemoryBytes: 0, sampleKeys: [] as string[] },
      devPassword: { pattern: 'nft-bot:server:password', keyCount: 0, totalMemoryBytes: 0, sampleKeys: [] as string[] },
      other: { pattern: 'other', keyCount: 0, totalMemoryBytes: 0, sampleKeys: [] as string[] }
    };

    let totalKeys = 0;
    let totalMemoryBytes = 0;
    let cursor = '0';
    const batchSize = 100;

    // Use SCAN to iterate through all keys
    do {
      const result = await this.redis.scan(cursor, 'COUNT', batchSize);

      cursor = result[0];
      const keys = result[1];

      // Process each key
      for (const key of keys) {
        totalKeys++;

        // Get memory usage for this key
        let memoryUsage = 0;
        try {
          const mem = await this.redis.call('MEMORY', 'USAGE', key);
          memoryUsage = typeof mem === 'number' ? mem : 0;
        } catch {
          // Key might have been deleted, skip it
          continue;
        }

        totalMemoryBytes += memoryUsage;

        // Categorize the key
        let categorized = false;
        for (const [categoryName, category] of Object.entries(categories)) {
          if (categoryName === 'other') continue;

          const pattern = category.pattern.replace('*', '');
          if (key.startsWith(pattern)) {
            category.keyCount++;
            category.totalMemoryBytes += memoryUsage;

            // Store sample keys (up to 5 per category)
            if (category.sampleKeys && category.sampleKeys.length < 5) {
              category.sampleKeys.push(key);
            }

            categorized = true;
            break;
          }
        }

        // If not categorized, add to "other"
        if (!categorized) {
          categories.other.keyCount++;
          categories.other.totalMemoryBytes += memoryUsage;
          if (categories.other.sampleKeys && categories.other.sampleKeys.length < 5) {
            categories.other.sampleKeys.push(key);
          }
        }
      }

      // Add small delay to avoid blocking Redis
      if (cursor !== '0') {
        await new Promise(resolve => setTimeout(resolve, 10));
      }
    } while (cursor !== '0');

    return {
      categories,
      totals: {
        totalKeys,
        totalMemoryBytes,
        totalMemoryMB: totalMemoryBytes / (1024 * 1024)
      }
    };
  }

  /**
   * Collect in-memory cache statistics
   */
  private async collectCacheStats(): Promise<CacheStats[]> {
    const caches: CacheStats[] = [];

    try {
      // TokenTraitCache
      const tokenTraitStats = await tokenTraitCache.getCacheStats();
      caches.push({
        name: 'TokenTraitCache',
        size: tokenTraitStats.size,
        maxSize: tokenTraitStats.maxSize,
        ttl: 24 * 60 * 60 * 1000, // 24 hours in ms
        estimatedMemoryBytes: tokenTraitStats.size * 200 // Rough estimate
      });

      // CollectionCache (has two internal caches)
      const collectionStats = getCollectionCacheStats();
      caches.push({
        name: 'CollectionCache (Floor)',
        size: collectionStats.floorPrice.size,
        maxSize: collectionStats.floorPrice.max,
        ttl: 5 * 60 * 1000, // 5 minutes
        estimatedMemoryBytes: collectionStats.floorPrice.size * 100
      });
      caches.push({
        name: 'CollectionCache (Details)',
        size: collectionStats.collectionDetails.size,
        maxSize: collectionStats.collectionDetails.max,
        ttl: 30 * 60 * 1000, // 30 minutes
        estimatedMemoryBytes: collectionStats.collectionDetails.size * 500
      });

      // TradingDisabledCache
      const tradingDisabledStats = tradingDisabledCache.getStats();
      const tradingDisabledCacheStats = tradingDisabledCache.getCacheStats();
      caches.push({
        name: 'TradingDisabledCache',
        size: tradingDisabledStats.total,
        maxSize: tradingDisabledCacheStats.maxSize,
        ttl: tradingDisabledCacheStats.ttl || 0,
        estimatedMemoryBytes: tradingDisabledStats.total * 50
      });

      // ListingPriceCache
      const listingPriceStats = listingPriceCache.getStats();
      caches.push({
        name: 'ListingPriceCache',
        size: listingPriceStats.memoryCacheSize,
        maxSize: listingPriceStats.memoryCacheStats.maxSize,
        ttl: listingPriceStats.memoryCacheStats.ttl || 0,
        estimatedMemoryBytes: listingPriceStats.memoryCacheSize * 150
      });

      // JobFallbackStore
      caches.push({
        name: 'JobFallbackStore',
        size: jobFallbackStore.size(),
        maxSize: 10000,
        ttl: 30 * 60 * 1000,
        estimatedMemoryBytes: jobFallbackStore.size() * 300
      });

      // ApprovalCache
      const approvalStats = approvalCache.getStats();
      caches.push({
        name: 'ApprovalCache',
        size: approvalStats.size,
        maxSize: approvalStats.maxSize,
        ttl: approvalStats.ttl,
        estimatedMemoryBytes: approvalStats.estimatedMemoryBytes
      });
    } catch (error) {
      errorLog('MemorySnapshot', 'Error collecting cache stats', error);
    }

    return caches;
  }

  /**
   * Collect global state statistics
   */
  private collectGlobalStateStats() {
    try {
      return {
        bidStats: {
          taskCount: Object.keys(bidStats).length,
          totalEntries: Object.values(bidStats).reduce((sum, mp) =>
            sum + (typeof mp === 'object' ? Object.keys(mp).length : 0), 0)
        },
        skipStats: {
          taskCount: Object.keys(skipStats).length,
          totalEntries: Object.values(skipStats).reduce((sum, mp) =>
            sum + (typeof mp === 'object' ? Object.keys(mp).length : 0), 0)
        },
        errorStats: {
          taskCount: Object.keys(errorStats).length,
          totalEntries: Object.values(errorStats).reduce((sum, mp) =>
            sum + (typeof mp === 'object' ? Object.keys(mp).length : 0), 0)
        },
        counterbidCounts: {
          taskCount: Object.keys(counterbidCounts).length,
          totalEntries: Object.values(counterbidCounts).reduce((sum, mp) =>
            sum + (typeof mp === 'object' ? Object.keys(mp).length : 0), 0)
        },
        processedOwnBids: {
          setSize: processedOwnBids.size
        },
        taskStatus: {
          taskCount: Object.keys(taskStatus).length
        },
        bestOffers: {
          taskCount: Object.keys(bestOffers).length,
          totalEntries: Object.values(bestOffers).reduce((sum, mp) =>
            sum + (typeof mp === 'object' ? Object.keys(mp).length : 0), 0)
        },
        floorPrices: {
          taskCount: Object.keys(floorPrices).length,
          totalEntries: Object.values(floorPrices).reduce((sum, mp) =>
            sum + (typeof mp === 'object' ? Object.keys(mp).length : 0), 0)
        }
      };
    } catch (error) {
      errorLog('MemorySnapshot', 'Error collecting global state stats', error);
      return this.getEmptyGlobalState();
    }
  }

  /**
   * Collect BullMQ queue statistics
   */
  private async collectQueueStats() {
    if (!this.mainQueue || !this.blurQueue) {
      throw new Error('BullMQ queues not initialized');
    }

    try {
      const [mainCounts, blurCounts] = await Promise.all([
        this.mainQueue.getJobCounts(),
        this.blurQueue.getJobCounts()
      ]);

      return {
        main: {
          waiting: mainCounts.waiting || 0,
          active: mainCounts.active || 0,
          completed: mainCounts.completed || 0,
          failed: mainCounts.failed || 0,
          delayed: mainCounts.delayed || 0
        },
        blur: {
          waiting: blurCounts.waiting || 0,
          active: blurCounts.active || 0,
          completed: blurCounts.completed || 0,
          failed: blurCounts.failed || 0,
          delayed: blurCounts.delayed || 0
        }
      };
    } catch (error) {
      errorLog('MemorySnapshot', 'Error collecting queue stats', error);
      return this.getEmptyQueueStats();
    }
  }

  /**
   * Collect MongoDB collection statistics
   */
  private async collectMongoStats() {
    try {
      const db = mongoose.connection.db;
      if (!db) {
        throw new Error('MongoDB connection not available');
      }

      // Collections migrated to LRU caches (exclude from monitoring)
      const excludedCollections = [
        'taskruntimestates',
        'tokentraitcaches',
        'tokentraits',
        'raritycaches',
        'bidlogs',
        'approvals'
      ];

      const collections = await db.listCollections().toArray();
      const stats = [];

      for (const collectionInfo of collections) {
        // Skip migrated collections
        if (excludedCollections.includes(collectionInfo.name)) {
          debugLog('MemorySnapshot', `Skipping migrated collection: ${collectionInfo.name}`);
          continue;
        }

        try {
          const collectionStats = await db.command({
            collStats: collectionInfo.name
          });

          stats.push({
            name: collectionInfo.name,
            sizeBytes: collectionStats.size || 0,
            documentCount: collectionStats.count || 0,
            avgDocumentSize: collectionStats.avgObjSize || 0,
            indexSizes: collectionStats.indexSizes || {},
            totalIndexSize: collectionStats.totalIndexSize || 0
          });
        } catch (err) {
          // Skip collections we can't access
          debugLog('MemorySnapshot', `Skipping collection ${collectionInfo.name}: ${err}`);
        }
      }

      return stats;
    } catch (error) {
      errorLog('MemorySnapshot', 'Error collecting MongoDB stats', error);
      return [];
    }
  }

  /**
   * Collect memory cleanup service statistics
   */
  private collectMemoryCleanupStats() {
    if (!this.deps?.memoryCleanupService) {
      return this.getEmptyMemoryCleanupStats();
    }
    return this.deps.memoryCleanupService.getStats();
  }

  /**
   * Collect LRU caches statistics (all 35+ caches)
   */
  private collectLRUCachesStats() {
    try {
      // Helper function to estimate memory usage
      const estimateMemory = (size: number, bytesPerEntry: number) => size * bytesPerEntry;

      // Get cache stats
      const orderCacheStats = getOrderCacheStats();
      const logsCacheStats = getLogsCacheStats();
      const collectionCacheStats = getCollectionCacheStats();
      const decryptedKeyCacheStats = getDecryptedKeyCacheStats();

      // Large caches
      const orderLRU = {
        size: orderCacheStats.lru.size,
        maxSize: orderCacheStats.lru.max,
        estimatedMemoryBytes: estimateMemory(orderCacheStats.lru.size, 500)
      };

      const rarityCacheSize = (rarityCache as any).cache?.size || 0;
      const rarityCacheObj = {
        size: rarityCacheSize,
        maxSize: 100000,
        ttl: 24 * 60 * 60 * 1000,
        estimatedMemoryBytes: estimateMemory(rarityCacheSize, 2000)
      };

      const tokenTraitCacheStats = {
        size: logsCacheStats.size,
        maxSize: 100000,
        ttl: 24 * 60 * 60 * 1000,
        estimatedMemoryBytes: estimateMemory(logsCacheStats.size, 1000)
      };

      const logsCacheObj = {
        size: logsCacheStats.size,
        maxSize: logsCacheStats.maxSize,
        ttl: 24 * 60 * 60 * 1000,
        estimatedMemoryBytes: estimateMemory(logsCacheStats.size, 500)
      };

      const processedMessagesSize = this.deps?.processedMessages?.size || 0;
      const processedMessagesObj = {
        size: processedMessagesSize,
        maxSize: 1000000,
        estimatedMemoryBytes: estimateMemory(processedMessagesSize, 200)
      };

      const processedOwnBidsObj = {
        size: processedOwnBids.size,
        maxSize: 1000000,
        ttl: 60 * 1000,
        estimatedMemoryBytes: estimateMemory(processedOwnBids.size, 100)
      };

      const approvalCacheStats = approvalCache.getStats();
      const approvalCacheObj = {
        size: approvalCacheStats.size,
        maxSize: approvalCacheStats.maxSize,
        ttl: approvalCacheStats.ttl,
        estimatedMemoryBytes: approvalCacheStats.estimatedMemoryBytes
      };

      const traitFloorLRUObj = {
        size: traitFloorLRU.size,
        maxSize: 20000,
        ttl: 5 * 60 * 1000,
        estimatedMemoryBytes: estimateMemory(traitFloorLRU.size, 200)
      };

      // OpenSea caches
      const openseaCaches = {
        collectionDetailsLRU: { size: collectionDetailsLRU.size, maxSize: 500, ttl: 24 * 60 * 60 * 1000 },
        floorPriceLRU: { size: floorPriceLRU.size, maxSize: 500, ttl: 5 * 60 * 1000 },
        contractResolutionLRU: { size: 0, maxSize: 5000, ttl: 24 * 60 * 60 * 1000 }, // Not exported
        multiContractSlugLRU: { size: 0, maxSize: 500, ttl: 24 * 60 * 60 * 1000 }, // Not exported
        contractMapLRU: { size: 0, maxSize: 500, ttl: 24 * 60 * 60 * 1000 }, // Not exported
        balanceLRU: { size: balanceLRU.size, maxSize: 100, ttl: 60 * 1000 },
        totalEstimatedMemoryBytes: estimateMemory(
          collectionDetailsLRU.size + floorPriceLRU.size + balanceLRU.size, 500
        )
      };

      // Collection caches
      const collectionCaches = {
        collectionDetailsCache: {
          size: collectionCacheStats.collectionDetails.size,
          maxSize: 500,
          ttl: 7 * 24 * 60 * 60 * 1000
        },
        floorPriceCache: {
          size: collectionCacheStats.floorPrice.size,
          maxSize: 1000,
          ttl: 5 * 60 * 1000
        },
        totalEstimatedMemoryBytes: estimateMemory(
          collectionCacheStats.collectionDetails.size + collectionCacheStats.floorPrice.size, 500
        )
      };

      // Trading & Listing caches
      const tradingDisabledCacheSize = tradingDisabledCache.getStats().total;
      const listingPriceCacheSize = listingPriceCache.getStats().memoryCacheSize;
      const tradingListingCaches = {
        tradingDisabledCache: { size: tradingDisabledCacheSize, maxSize: 5000, ttl: 24 * 60 * 60 * 1000 },
        listingPriceCache: { size: listingPriceCacheSize, maxSize: 10000, ttl: 30 * 60 * 1000 },
        collectionFloorCache: { size: collectionFloorCache.size, maxSize: 1000, ttl: 5 * 60 * 1000 },
        totalEstimatedMemoryBytes: estimateMemory(
          tradingDisabledCacheSize + listingPriceCacheSize + collectionFloorCache.size, 300
        )
      };

      // Dedup caches (insufficientBalanceLogCache not exported, so we can't track it)
      const dedupCaches = {
        insufficientBalanceLogCache: { totalSize: 0, instanceCount: 3 }, // Not accessible
        loggedBalanceErrors: { size: 0, maxSize: 1000, ttl: 60 * 1000 }, // Not accessible
        totalEstimatedMemoryBytes: 0
      };

      // Small utility caches
      const walletCacheSize = (walletCache as any).cache?.size || 0;
      const marketplaceApprovalCacheSize = (marketplaceApprovalCache as any).cache?.size || 0;
      const smallUtilityCaches = {
        walletCache: { size: walletCacheSize, maxSize: 100 },
        decryptedKeyCache: { size: decryptedKeyCacheStats.size, maxSize: 1000 },
        marketplaceApprovalCache: { size: marketplaceApprovalCacheSize, maxSize: 1000 },
        totalEstimatedMemoryBytes: estimateMemory(
          walletCacheSize + decryptedKeyCacheStats.size + marketplaceApprovalCacheSize, 200
        )
      };

      // Calculate totals
      const totalSizeAllCaches =
        orderLRU.size + rarityCacheObj.size + tokenTraitCacheStats.size + logsCacheObj.size +
        processedMessagesObj.size + processedOwnBidsObj.size + approvalCacheObj.size +
        traitFloorLRUObj.size + collectionDetailsLRU.size + floorPriceLRU.size + balanceLRU.size +
        collectionCacheStats.collectionDetails.size + collectionCacheStats.floorPrice.size +
        tradingDisabledCacheSize + listingPriceCacheSize + collectionFloorCache.size +
        walletCacheSize + decryptedKeyCacheStats.size + marketplaceApprovalCacheSize;

      const totalEstimatedMemoryBytes =
        orderLRU.estimatedMemoryBytes + rarityCacheObj.estimatedMemoryBytes +
        tokenTraitCacheStats.estimatedMemoryBytes + logsCacheObj.estimatedMemoryBytes +
        processedMessagesObj.estimatedMemoryBytes + processedOwnBidsObj.estimatedMemoryBytes +
        approvalCacheObj.estimatedMemoryBytes + traitFloorLRUObj.estimatedMemoryBytes +
        openseaCaches.totalEstimatedMemoryBytes + collectionCaches.totalEstimatedMemoryBytes +
        tradingListingCaches.totalEstimatedMemoryBytes + dedupCaches.totalEstimatedMemoryBytes +
        smallUtilityCaches.totalEstimatedMemoryBytes;

      return {
        // Large caches
        orderLRU,
        rarityCache: rarityCacheObj,
        tokenTraitCache: tokenTraitCacheStats,
        logsCache: logsCacheObj,
        processedMessages: processedMessagesObj,
        processedOwnBids: processedOwnBidsObj,
        approvalCache: approvalCacheObj,
        traitFloorLRU: traitFloorLRUObj,

        // Grouped caches
        openseaCaches,
        collectionCaches,
        tradingListingCaches,
        dedupCaches,
        smallUtilityCaches,

        // Summary
        totalCaches: 25, // Approximate count of tracked caches
        totalSizeAllCaches,
        totalEstimatedMemoryBytes
      };
    } catch (error) {
      errorLog('MemorySnapshot', 'Error collecting LRU cache stats', error);
      return this.getEmptyLRUCachesStats();
    }
  }

  /**
   * Collect unbounded data structures statistics
   */
  private collectUnboundedDataStructuresStats() {
    if (!this.deps) {
      return this.getEmptyUnboundedDataStructuresStats();
    }

    return {
      traitsToRemove: Object.keys(this.deps.traitsToRemove || {}).length,
      removedTokens: Object.keys(this.deps.removedTokens || {}).length,
      tasksBeingRestarted: this.deps.tasksBeingRestarted?.size || 0,
      tasksBeingCancelled: this.deps.tasksBeingCancelled?.size || 0,
      tasksInCompletionProcess: this.deps.tasksInCompletionProcess?.size || 0,
      failedStartQueue: this.deps.failedTaskStartQueue?.size || 0,
      failedStopQueue: this.deps.failedTaskStopQueue?.size || 0,
      queueingProgress: this.deps.queueingProgress?.size || 0,
      taskLocks: this.deps.taskLocks?.size || 0,
      loopSchedulingLocks: this.deps.loopSchedulingLocks?.size || 0
    };
  }

  /**
   * Clean up old snapshots (older than 30 days)
   */
  public async cleanupOldSnapshots(): Promise<number> {
    try {
      const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
      const result = await MemorySnapshot.deleteMany({
        timestamp: { $lt: thirtyDaysAgo }
      });

      if (result.deletedCount > 0) {
        infoLog('MemorySnapshot', `Cleaned up ${result.deletedCount} old snapshots`);
      }

      return result.deletedCount;
    } catch (error) {
      errorLog('MemorySnapshot', 'Error cleaning up old snapshots', error);
      return 0;
    }
  }

  // Helper methods for error fallbacks
  private getEmptyRedisMemory() {
    const emptyCategory = { pattern: '', keyCount: 0, totalMemoryBytes: 0, sampleKeys: [] };
    return {
      categories: {
        bullMQMain: { ...emptyCategory, pattern: 'bull:BIDDING_BOT:*' },
        bullMQBlur: { ...emptyCategory, pattern: 'bull:BLUR_BIDDING_QUEUE:*' },
        loopLocks: { ...emptyCategory, pattern: 'nft-bot:next-loop:*' },
        openseaCollection: { ...emptyCategory, pattern: 'opensea:collection:*' },
        openseaTraits: { ...emptyCategory, pattern: 'opensea:traits:*' },
        websocketMetrics: { ...emptyCategory, pattern: 'websocket:*' },
        devPassword: { ...emptyCategory, pattern: 'nft-bot:server:password' },
        other: { ...emptyCategory, pattern: 'other' }
      },
      totals: {
        totalKeys: 0,
        totalMemoryBytes: 0,
        totalMemoryMB: 0
      }
    };
  }

  private getEmptyLRUCachesStats() {
    const emptyCache = { size: 0, maxSize: 0, estimatedMemoryBytes: 0 };
    const emptyCacheWithTTL = { size: 0, maxSize: 0, ttl: 0, estimatedMemoryBytes: 0 };

    return {
      // Large caches
      orderLRU: emptyCache,
      rarityCache: emptyCacheWithTTL,
      tokenTraitCache: emptyCacheWithTTL,
      logsCache: emptyCacheWithTTL,
      processedMessages: { size: 0, maxSize: 1000000, estimatedMemoryBytes: 0 },
      processedOwnBids: { size: 0, maxSize: 1000000, ttl: 0, estimatedMemoryBytes: 0 },
      approvalCache: emptyCacheWithTTL,
      traitFloorLRU: emptyCacheWithTTL,

      // Grouped caches
      openseaCaches: {
        collectionDetailsLRU: { size: 0, maxSize: 0, ttl: 0 },
        floorPriceLRU: { size: 0, maxSize: 0, ttl: 0 },
        contractResolutionLRU: { size: 0, maxSize: 0, ttl: 0 },
        multiContractSlugLRU: { size: 0, maxSize: 0, ttl: 0 },
        contractMapLRU: { size: 0, maxSize: 0, ttl: 0 },
        balanceLRU: { size: 0, maxSize: 0, ttl: 0 },
        totalEstimatedMemoryBytes: 0
      },
      collectionCaches: {
        collectionDetailsCache: { size: 0, maxSize: 0, ttl: 0 },
        floorPriceCache: { size: 0, maxSize: 0, ttl: 0 },
        totalEstimatedMemoryBytes: 0
      },
      tradingListingCaches: {
        tradingDisabledCache: { size: 0, maxSize: 0, ttl: 0 },
        listingPriceCache: { size: 0, maxSize: 0, ttl: 0 },
        collectionFloorCache: { size: 0, maxSize: 0, ttl: 0 },
        totalEstimatedMemoryBytes: 0
      },
      dedupCaches: {
        insufficientBalanceLogCache: { totalSize: 0, instanceCount: 0 },
        loggedBalanceErrors: { size: 0, maxSize: 0, ttl: 0 },
        totalEstimatedMemoryBytes: 0
      },
      smallUtilityCaches: {
        walletCache: { size: 0, maxSize: 0 },
        decryptedKeyCache: { size: 0, maxSize: 0 },
        marketplaceApprovalCache: { size: 0, maxSize: 0 },
        totalEstimatedMemoryBytes: 0
      },

      // Summary
      totalCaches: 0,
      totalSizeAllCaches: 0,
      totalEstimatedMemoryBytes: 0
    };
  }

  private getEmptyGlobalState() {
    return {
      bidStats: { taskCount: 0, totalEntries: 0 },
      skipStats: { taskCount: 0, totalEntries: 0 },
      errorStats: { taskCount: 0, totalEntries: 0 },
      counterbidCounts: { taskCount: 0, totalEntries: 0 },
      processedOwnBids: { setSize: 0 },
      taskStatus: { taskCount: 0 },
      bestOffers: { taskCount: 0, totalEntries: 0 },
      floorPrices: { taskCount: 0, totalEntries: 0 }
    };
  }

  private getEmptyQueueStats() {
    return {
      main: { waiting: 0, active: 0, completed: 0, failed: 0, delayed: 0 },
      blur: { waiting: 0, active: 0, completed: 0, failed: 0, delayed: 0 }
    };
  }

  private getEmptyMemoryCleanupStats() {
    return {
      initialized: false,
      activeCleanupIntervals: 0,
      currentSizes: {
        queueingProgress: 0,
        failedTaskStartQueue: 0,
        failedTaskStopQueue: 0,
        taskLocks: 0,
        loopSchedulingLocks: 0
      }
    };
  }

  private getEmptyUnboundedDataStructuresStats() {
    return {
      traitsToRemove: 0,
      removedTokens: 0,
      tasksBeingRestarted: 0,
      tasksBeingCancelled: 0,
      tasksInCompletionProcess: 0,
      failedStartQueue: 0,
      failedStopQueue: 0,
      queueingProgress: 0,
      taskLocks: 0,
      loopSchedulingLocks: 0
    };
  }
}

export const memorySnapshotService = new MemorySnapshotService();
