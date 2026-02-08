import { ITask } from '../models/task.model';
import { walletCache } from './wallet-cache';
import { getCachedCollectionDetails, getCachedFloorPrice } from './collection-cache';
import { errorLog } from './debug-file-logger';

interface CachedTaskData {
  task: ITask;
  walletPrivateKey?: string;
  collectionDetails?: any;
  creatorFees?: any;
  lastUpdated: number;
}

/**
 * High-performance cache for counter-bid operations
 * Pre-warms data for active tasks to minimize latency during counter-bidding
 */
class CounterBidCache {
  private taskCache = new Map<string, CachedTaskData>();
  private contractToTaskIds = new Map<string, Set<string>>();
  private slugToTaskIds = new Map<string, Set<string>>();
  private cacheRefreshInterval: NodeJS.Timeout | null = null;
  private readonly CACHE_TTL = 5 * 60 * 1000; // 5 minutes
  private readonly REFRESH_INTERVAL = 60 * 1000; // 1 minute

  constructor() {
    this.startCacheRefresh();
  }

  /**
   * Pre-warm cache for a task when it starts
   */
  async preloadForTask(task: ITask): Promise<void> {
    const startTime = Date.now();
    
    try {
      // Skip if task is not running or no contract
      if (!task.running || !task.contract) return;

      const taskId = task._id.toString();
      
      // Fetch all required data in parallel
      const [walletData, collectionDetails] = await Promise.all([
        task.wallet ? walletCache.getWallet(task.wallet).catch(() => null) : null,
        task.contract.slug ? getCachedCollectionDetails(task.contract.slug).catch(() => null) : null
      ]);

      // Decrypt private key if wallet data exists
      let walletPrivateKey: string | undefined;
      if (walletData?.encryptedPrivateKey) {
        try {
          const { decryptPrivateKey } = await import('../utils/crypto');
          walletPrivateKey = decryptPrivateKey(walletData.encryptedPrivateKey);
        } catch (error) {
          console.error(`Failed to decrypt wallet for task ${taskId}:`, error);
        }
      }

      // Extract creator fees if collection details exist
      const creatorFees = collectionDetails?.creator_fees ? 
        Object.fromEntries(
          Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)])
        ) : undefined;

      // Store in cache
      const cachedData: CachedTaskData = {
        task,
        walletPrivateKey,
        collectionDetails,
        creatorFees,
        lastUpdated: Date.now()
      };

      this.taskCache.set(taskId, cachedData);

      // Update indexes for fast lookup
      const contractKey = task.contract.contractAddress?.toLowerCase();
      const slugKey = task.contract.slug?.toLowerCase();

      if (contractKey) {
        if (!this.contractToTaskIds.has(contractKey)) {
          this.contractToTaskIds.set(contractKey, new Set());
        }
        this.contractToTaskIds.get(contractKey)!.add(taskId);
      }

      if (slugKey) {
        if (!this.slugToTaskIds.has(slugKey)) {
          this.slugToTaskIds.set(slugKey, new Set());
        }
        this.slugToTaskIds.get(slugKey)!.add(taskId);
      }

      // Pre-warm collection floor prices for all marketplaces
      if (task.contract.contractAddress || task.contract.slug) {
        const marketplaces = ['opensea', 'magiceden', 'blur'];
        const floorPricePromises = marketplaces.map(marketplace =>
          getCachedFloorPrice(
            task.contract.slug || task.contract.contractAddress || '',
            marketplace
          ).catch(() => null)
        );
        await Promise.allSettled(floorPricePromises);
      }

      const loadTime = Date.now() - startTime;
      console.log(`⚡ [CounterBidCache] Pre-warmed task ${taskId} in ${loadTime}ms (wallet: ${!!walletData}, collection: ${!!collectionDetails})`);

    } catch (error) {
      console.error(`Failed to preload cache for task ${task._id}:`, error);
      errorLog('CounterBidCache', `Preload failed for task ${task._id}: ${error}`);
    }
  }

  /**
   * Get cached data for a task - O(1) lookup
   */
  getTaskData(taskId: string): CachedTaskData | undefined {
    const cached = this.taskCache.get(taskId);
    
    // Check if cache is expired
    if (cached && (Date.now() - cached.lastUpdated > this.CACHE_TTL)) {
      this.removeTask(taskId);
      return undefined;
    }
    
    return cached;
  }

  /**
   * Get all relevant tasks for a contract/slug - O(1) lookup
   */
  getRelevantTaskIds(contractAddress?: string, slug?: string): string[] {
    const taskIds = new Set<string>();
    
    if (contractAddress) {
      const contractTasks = this.contractToTaskIds.get(contractAddress.toLowerCase());
      if (contractTasks) {
        contractTasks.forEach(id => taskIds.add(id));
      }
    }
    
    if (slug) {
      const slugTasks = this.slugToTaskIds.get(slug.toLowerCase());
      if (slugTasks) {
        slugTasks.forEach(id => taskIds.add(id));
      }
    }
    
    return Array.from(taskIds);
  }

  /**
   * Get cached data for multiple tasks
   */
  getMultipleTaskData(taskIds: string[]): CachedTaskData[] {
    const results: CachedTaskData[] = [];
    
    for (const taskId of taskIds) {
      const data = this.getTaskData(taskId);
      if (data) {
        results.push(data);
      }
    }
    
    return results;
  }

  /**
   * Remove a task from cache when it stops
   */
  removeTask(taskId: string): void {
    const cached = this.taskCache.get(taskId);
    if (!cached) return;

    // Remove from indexes
    const contractKey = cached.task.contract?.contractAddress?.toLowerCase();
    const slugKey = cached.task.contract?.slug?.toLowerCase();

    if (contractKey && this.contractToTaskIds.has(contractKey)) {
      const tasks = this.contractToTaskIds.get(contractKey)!;
      tasks.delete(taskId);
      if (tasks.size === 0) {
        this.contractToTaskIds.delete(contractKey);
      }
    }

    if (slugKey && this.slugToTaskIds.has(slugKey)) {
      const tasks = this.slugToTaskIds.get(slugKey)!;
      tasks.delete(taskId);
      if (tasks.size === 0) {
        this.slugToTaskIds.delete(slugKey);
      }
    }

    // Clear sensitive data
    if (cached.walletPrivateKey) {
      cached.walletPrivateKey = undefined;
    }

    this.taskCache.delete(taskId);
  }

  /**
   * Update task in cache
   */
  updateTask(task: ITask): void {
    const taskId = task._id.toString();
    const existing = this.taskCache.get(taskId);
    
    if (existing) {
      existing.task = task;
      existing.lastUpdated = Date.now();
    } else if (task.running) {
      // If not in cache but running, preload it
      this.preloadForTask(task).catch(error => {
        console.error(`Failed to preload task ${taskId} on update:`, error);
      });
    }
  }

  /**
   * Clear all cached data
   */
  clear(): void {
    // Clear sensitive data before removing
    for (const [_, cached] of this.taskCache) {
      if (cached.walletPrivateKey) {
        cached.walletPrivateKey = undefined;
      }
    }
    
    this.taskCache.clear();
    this.contractToTaskIds.clear();
    this.slugToTaskIds.clear();
  }

  /**
   * Start periodic cache refresh to remove stale entries
   */
  private startCacheRefresh(): void {
    this.cacheRefreshInterval = setInterval(() => {
      const now = Date.now();
      const expiredTasks: string[] = [];
      
      for (const [taskId, cached] of this.taskCache) {
        if (now - cached.lastUpdated > this.CACHE_TTL) {
          expiredTasks.push(taskId);
        }
      }
      
      for (const taskId of expiredTasks) {
        this.removeTask(taskId);
      }
      
      if (expiredTasks.length > 0) {
        console.log(`[CounterBidCache] Cleared ${expiredTasks.length} expired entries`);
      }
    }, this.REFRESH_INTERVAL);
  }

  /**
   * Stop cache refresh interval
   */
  destroy(): void {
    if (this.cacheRefreshInterval) {
      clearInterval(this.cacheRefreshInterval);
      this.cacheRefreshInterval = null;
    }
    this.clear();
  }

  /**
   * Get cache statistics
   */
  getStats() {
    return {
      totalTasks: this.taskCache.size,
      contractIndexes: this.contractToTaskIds.size,
      slugIndexes: this.slugToTaskIds.size,
      memoryUsage: this.estimateMemoryUsage()
    };
  }

  /**
   * Estimate memory usage of cache
   */
  private estimateMemoryUsage(): string {
    // Rough estimation based on cached items
    const avgTaskSize = 5000; // 5KB per task (conservative estimate)
    const totalBytes = this.taskCache.size * avgTaskSize;
    
    if (totalBytes < 1024) return `${totalBytes} B`;
    if (totalBytes < 1024 * 1024) return `${(totalBytes / 1024).toFixed(2)} KB`;
    return `${(totalBytes / (1024 * 1024)).toFixed(2)} MB`;
  }
}

// Export singleton instance
export const counterBidCache = new CounterBidCache();