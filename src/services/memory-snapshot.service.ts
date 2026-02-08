import Redis from 'ioredis';
import { Queue, Worker } from 'bullmq';
import mongoose from 'mongoose';
import { MemorySnapshot, IMemorySnapshot } from '../models/memory-snapshot.model';
import { errorLog, warnLog } from '../utils/debug-file-logger';

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
  openseaFloorLRU,
  balanceLRU,
  traitFloorLRU,
  blurFloorLRU,
  magicEdenFloorLRU,
  collectionItemsLRU,
  collectionTraitsLRU,
  openseaBestOfferLRU,
  blurBestOfferLRU,
  magicedenBestOfferLRU,
  traitFloorsDetailLRU,
  collectionEventsLRU
} from '../marketplace/opensea/cache';
import { marketplaceUnavailableLRU } from '../utils/marketplace-availability';

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
  collectionStatsLRU: any; // LRUCache
  collectionDataLRU: any; // LRUCache
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
  private workers: Worker[] = [];
  private deps: MemorySnapshotDependencies | null = null;

  public initialize(
    redis: Redis,
    mainQueue: Queue,
    blurQueue: Queue | null,
    deps?: MemorySnapshotDependencies,
    workers?: Worker[]
  ): void {
    this.redis = redis;
    this.mainQueue = mainQueue;
    this.blurQueue = blurQueue;
    this.workers = workers || [];
    if (deps) {
      this.deps = deps;
    }
  }

  /**
   * Take a complete memory snapshot
   */
  public async takeSnapshot(): Promise<IMemorySnapshot> {
    const startTime = Date.now();
    const errors: string[] = [];
    let errorCount = 0;

    try {

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
        unboundedDataStructures,
        v8HeapStats,
        systemResources
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
        }),
        Promise.resolve(this.collectV8HeapStats()).catch((err: Error) => {
          errorCount++;
          errors.push(`V8HeapStats: ${err.message}`);
          return this.getEmptyV8HeapStats();
        }),
        Promise.resolve(this.collectSystemResources()).catch((err: Error) => {
          errorCount++;
          errors.push(`SystemResources: ${err.message}`);
          return this.getEmptySystemResources();
        })
      ]);

      const snapshotDurationMs = Date.now() - startTime;

      // Collect worker stats
      const workers = this.collectWorkerStats();

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
        v8HeapStats,
        systemResources,
        workers,
        metadata: {
          snapshotDurationMs,
          errorCount,
          errors: errors.length > 0 ? errors : undefined
        }
      });

      await snapshot.save();

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
   *
   * DEPRECATED: Redis memory stats removed - Redis is BullMQ-only
   * Use BullMQ metrics (queue.getJobCounts(), etc.) for queue monitoring
   *
   * Returns empty stats in schema-compatible format
   */
  private async collectRedisMemory() {
    // Return empty stats in the format expected by the schema
    return this.getEmptyRedisMemory();
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
    if (!this.mainQueue) {
      throw new Error('Main BullMQ queue not initialized');
    }

    try {
      const mainCounts = await this.mainQueue.getJobCounts();

      const result: any = {
        main: {
          waiting: mainCounts.waiting || 0,
          active: mainCounts.active || 0,
          completed: mainCounts.completed || 0,
          failed: mainCounts.failed || 0,
          delayed: mainCounts.delayed || 0
        }
      };

      // If blur queue exists, include its stats (optional)
      if (this.blurQueue) {
        const blurCounts = await this.blurQueue.getJobCounts();
        result.blur = {
          waiting: blurCounts.waiting || 0,
          active: blurCounts.active || 0,
          completed: blurCounts.completed || 0,
          failed: blurCounts.failed || 0,
          delayed: blurCounts.delayed || 0
        };
      }

      return result;
    } catch (error) {
      errorLog('MemorySnapshot', 'Error collecting queue stats', error);
      return this.getEmptyQueueStats();
    }
  }

  /**
   * Collect worker statistics (unified format)
   * Returns aggregated stats in schema-compatible format
   */
  private collectWorkerStats() {
    try {
      const totalConcurrency = this.workers.reduce((sum, w) => sum + w.concurrency, 0);
      const allRunning = this.workers.every(w => w.isRunning());

      return {
        totalConcurrency,
        workerCount: this.workers.length,
        running: allRunning
      };
    } catch (error) {
      errorLog('MemorySnapshot', 'Error collecting worker stats', error);
      return {
        totalConcurrency: 0,
        workerCount: 0,
        running: false
      };
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
        openseaFloorLRU: { size: openseaFloorLRU.size, maxSize: 500, ttl: 5 * 60 * 1000 },
        contractResolutionLRU: { size: 0, maxSize: 5000, ttl: 24 * 60 * 60 * 1000 }, // Not exported
        multiContractSlugLRU: { size: 0, maxSize: 500, ttl: 24 * 60 * 60 * 1000 }, // Not exported
        contractMapLRU: { size: 0, maxSize: 500, ttl: 24 * 60 * 60 * 1000 }, // Not exported
        balanceLRU: { size: balanceLRU.size, maxSize: 100, ttl: 60 * 1000 },
        // New marketplace floor caches
        blurFloorLRU: { size: blurFloorLRU.size, maxSize: 1000, ttl: 5 * 60 * 1000 },
        magicEdenFloorLRU: { size: magicEdenFloorLRU.size, maxSize: 1000, ttl: 60 * 1000 },
        // New collection data caches
        collectionItemsLRU: { size: collectionItemsLRU.size, maxSize: 500, ttl: 60 * 60 * 1000 },
        collectionTraitsLRU: { size: collectionTraitsLRU.size, maxSize: 1_000_000, ttl: 24 * 60 * 60 * 1000 },
        traitFloorsDetailLRU: { size: traitFloorsDetailLRU.size, maxSize: 5000, ttl: 60 * 1000 },
        collectionEventsLRU: { size: collectionEventsLRU.size, maxSize: 1000, ttl: 10 * 1000 },
        // New best offer caches (for client UI only)
        openseaBestOfferLRU: { size: openseaBestOfferLRU.size, maxSize: 1000, ttl: 5 * 60 * 1000 },
        blurBestOfferLRU: { size: blurBestOfferLRU.size, maxSize: 1000, ttl: 5 * 60 * 1000 },
        magicedenBestOfferLRU: { size: magicedenBestOfferLRU.size, maxSize: 1000, ttl: 5 * 60 * 1000 },
        totalEstimatedMemoryBytes: estimateMemory(
          collectionDetailsLRU.size + openseaFloorLRU.size + balanceLRU.size +
          blurFloorLRU.size + magicEdenFloorLRU.size + collectionItemsLRU.size +
          collectionTraitsLRU.size + traitFloorsDetailLRU.size + collectionEventsLRU.size +
          openseaBestOfferLRU.size + blurBestOfferLRU.size + magicedenBestOfferLRU.size, 500
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
      const collectionStatsLRUSize = this.deps?.collectionStatsLRU?.size || 0;
      const collectionDataLRUSize = this.deps?.collectionDataLRU?.size || 0;
      const tradingListingCaches = {
        tradingDisabledCache: { size: tradingDisabledCacheSize, maxSize: 5000, ttl: 24 * 60 * 60 * 1000 },
        listingPriceCache: { size: listingPriceCacheSize, maxSize: 10000, ttl: 30 * 60 * 1000 },
        collectionFloorCache: { size: collectionFloorCache.size, maxSize: 1000, ttl: 5 * 60 * 1000 },
        collectionStatsLRU: { size: collectionStatsLRUSize, maxSize: 1000, ttl: 24 * 60 * 60 * 1000 },
        collectionDataLRU: { size: collectionDataLRUSize, maxSize: 100, ttl: 24 * 60 * 60 * 1000 },
        totalEstimatedMemoryBytes: estimateMemory(
          tradingDisabledCacheSize + listingPriceCacheSize + collectionFloorCache.size +
          collectionStatsLRUSize + collectionDataLRUSize, 500
        )
      };

      // Dedup caches (insufficientBalanceLogCache not exported, so we can't track it)
      const dedupCaches = {
        insufficientBalanceLogCache: { totalSize: 0, instanceCount: 3 }, // Not accessible
        loggedBalanceErrors: { size: 0, maxSize: 1000, ttl: 60 * 1000 }, // Not accessible
        totalEstimatedMemoryBytes: 0
      };

      // Small utility caches
      const walletCacheSize = (walletCache as any).walletDataLRU?.size || 0;
      const marketplaceApprovalCacheSize = (marketplaceApprovalCache as any).marketplaceApprovalLRU?.size || 0;
      const smallUtilityCaches = {
        walletCache: { size: walletCacheSize, maxSize: 1000 },
        decryptedKeyCache: { size: decryptedKeyCacheStats.size, maxSize: 1000 },
        marketplaceApprovalCache: { size: marketplaceApprovalCacheSize, maxSize: 1000 },
        marketplaceUnavailableLRU: {
          size: marketplaceUnavailableLRU.size,
          maxSize: 1000,
          ttl: 3600 * 1000
        },
        totalEstimatedMemoryBytes: estimateMemory(
          walletCacheSize + decryptedKeyCacheStats.size + marketplaceApprovalCacheSize + marketplaceUnavailableLRU.size,
          100  // ~100 bytes per entry (key + boolean + metadata)
        )
      };

      // Calculate totals
      const totalSizeAllCaches =
        orderLRU.size + rarityCacheObj.size + tokenTraitCacheStats.size + logsCacheObj.size +
        processedMessagesObj.size + processedOwnBidsObj.size + approvalCacheObj.size +
        traitFloorLRUObj.size + collectionDetailsLRU.size + openseaFloorLRU.size + balanceLRU.size +
        collectionCacheStats.collectionDetails.size + collectionCacheStats.floorPrice.size +
        tradingDisabledCacheSize + listingPriceCacheSize + collectionFloorCache.size +
        collectionStatsLRUSize + collectionDataLRUSize +
        walletCacheSize + decryptedKeyCacheStats.size + marketplaceApprovalCacheSize + marketplaceUnavailableLRU.size;

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
      }

      return result.deletedCount;
    } catch (error) {
      errorLog('MemorySnapshot', 'Error cleaning up old snapshots', error);
      return 0;
    }
  }

  /**
   * Collect V8 heap statistics using Node.js v8 module
   */
  private collectV8HeapStats() {
    try {
      const v8 = require('v8');
      const stats = v8.getHeapStatistics();

      return {
        totalHeapSize: stats.total_heap_size,
        totalHeapSizeExecutable: stats.total_heap_size_executable,
        totalPhysicalSize: stats.total_physical_size,
        totalAvailableSize: stats.total_available_size,
        usedHeapSize: stats.used_heap_size,
        heapSizeLimit: stats.heap_size_limit,
        mallocedMemory: stats.malloced_memory,
        peakMallocedMemory: stats.peak_malloced_memory,
        doesZapGarbage: stats.does_zap_garbage,
        numberOfNativeContexts: stats.number_of_native_contexts,
        numberOfDetachedContexts: stats.number_of_detached_contexts
      };
    } catch (error) {
      console.error('[MemorySnapshot] Failed to collect V8 heap stats:', error);
      return this.getEmptyV8HeapStats();
    }
  }

  /**
   * Provide empty V8 heap stats for error scenarios
   */
  private getEmptyV8HeapStats() {
    return {
      totalHeapSize: 0,
      totalHeapSizeExecutable: 0,
      totalPhysicalSize: 0,
      totalAvailableSize: 0,
      usedHeapSize: 0,
      heapSizeLimit: 0,
      mallocedMemory: 0,
      peakMallocedMemory: 0,
      doesZapGarbage: 0,
      numberOfNativeContexts: 0,
      numberOfDetachedContexts: 0
    };
  }

  /**
   * Collect system resource statistics
   */
  private collectSystemResources() {
    try {
      // Count active handles and requests
      const activeHandles = (process as any)._getActiveHandles?.()?.length || 0;
      const activeRequests = (process as any)._getActiveRequests?.()?.length || 0;

      // Estimate timers (Node.js doesn't expose this directly)
      const timerCount = 0; // Placeholder - Node.js doesn't provide direct timer count

      // Count event listeners on process object
      const eventListenerCount = process.listenerCount ?
        ['exit', 'uncaughtException', 'unhandledRejection', 'SIGTERM', 'SIGINT']
          .reduce((sum, event) => sum + (process.listenerCount(event) || 0), 0) : 0;

      // Log buffer stats (from debug-file-logger if available)
      const logBufferEntries = 0; // Placeholder
      const logBufferSizeKB = 0; // Placeholder

      return {
        activeHandles,
        activeRequests,
        logBufferEntries,
        logBufferSizeKB,
        timerCount,
        eventListenerCount
      };
    } catch (error) {
      console.error('[MemorySnapshot] Failed to collect system resources:', error);
      return this.getEmptySystemResources();
    }
  }

  /**
   * Provide empty system resources for error scenarios
   */
  private getEmptySystemResources() {
    return {
      activeHandles: 0,
      activeRequests: 0,
      logBufferEntries: 0,
      logBufferSizeKB: 0,
      timerCount: 0,
      eventListenerCount: 0
    };
  }

  // Helper methods for error fallbacks
  private getEmptyRedisMemory() {
    const emptyCategory = { pattern: '', keyCount: 0, totalMemoryBytes: 0, sampleKeys: [] };
    return {
      categories: {
        bullMQMain: { ...emptyCategory, pattern: 'bull:BIDDING_BOT:*' },
        bullMQBlur: { ...emptyCategory, pattern: 'bull:BLUR_BIDDING_QUEUE:*' },
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
        openseaFloorLRU: { size: 0, maxSize: 0, ttl: 0 },
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
        collectionStatsLRU: { size: 0, maxSize: 0, ttl: 0 },
        collectionDataLRU: { size: 0, maxSize: 0, ttl: 0 },
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
        marketplaceUnavailableLRU: { size: 0, maxSize: 0, ttl: 0 },
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
      main: { waiting: 0, active: 0, completed: 0, failed: 0, delayed: 0 }
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
