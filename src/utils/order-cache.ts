import Order from '../models/order.model';

export interface CachedOrder {
  _id: string;
  orderHash: string;
  expiresAt: number; // timestamp in ms
  offerPrice: string; // wei
  createdAt: number; // timestamp in ms
  identifier: string;
  marketplace: string;
  slug: string;
  taskId: string;
}

const DEBUG = process.env.DEBUG === 'true';

class OrderCache {
  private cache: Map<string, CachedOrder> = new Map();
  private taskIndex: Map<string, Set<string>> = new Map(); // taskId -> Set of cache keys
  private idIndex: Map<string, string> = new Map(); // orderId -> cache key (for deleteById)
  private initializedTasks: Set<string> = new Set(); // tracks which tasks have been initialized

  /**
   * Generate cache key from taskId, marketplace, and identifier
   * Format: "{taskId}:{marketplace}:{identifier}"
   */
  private getKey(taskId: string, marketplace: string, identifier: string): string {
    return `${taskId}:${marketplace.toLowerCase()}:${identifier}`;
  }

  /**
   * Add or update an order in the cache
   */
  set(order: CachedOrder): void {
    const key = this.getKey(order.taskId, order.marketplace, order.identifier);

    // Update main cache
    this.cache.set(key, order);

    // Update task index
    if (!this.taskIndex.has(order.taskId)) {
      this.taskIndex.set(order.taskId, new Set());
    }
    this.taskIndex.get(order.taskId)!.add(key);

    // Update id index
    this.idIndex.set(order._id, key);

    if (DEBUG) {
      console.log(`[OrderCache] SET: ${key} | price=${order.offerPrice} | expires=${new Date(order.expiresAt).toISOString()}`);
    }
  }

  /**
   * Get a single order from cache
   */
  get(taskId: string, marketplace: string, identifier: string): CachedOrder | undefined {
    const key = this.getKey(taskId, marketplace, identifier);
    const order = this.cache.get(key);

    if (DEBUG && order) {
      console.log(`[OrderCache] GET: ${key} | found=${!!order} | expired=${order ? this.isExpired(order) : 'N/A'}`);
    }

    return order;
  }

  /**
   * Delete a single order from cache
   */
  delete(taskId: string, marketplace: string, identifier: string): boolean {
    const key = this.getKey(taskId, marketplace, identifier);
    const order = this.cache.get(key);

    if (order) {
      // Remove from id index
      this.idIndex.delete(order._id);
    }

    // Remove from task index
    const taskKeys = this.taskIndex.get(taskId);
    if (taskKeys) {
      taskKeys.delete(key);
      if (taskKeys.size === 0) {
        this.taskIndex.delete(taskId);
      }
    }

    const deleted = this.cache.delete(key);

    if (DEBUG && deleted) {
      console.log(`[OrderCache] DELETE: ${key}`);
    }

    return deleted;
  }

  /**
   * Delete order by its _id (used in cancellation callbacks)
   */
  deleteById(orderId: string): boolean {
    const key = this.idIndex.get(orderId);
    if (!key) {
      return false;
    }

    const order = this.cache.get(key);
    if (order) {
      return this.delete(order.taskId, order.marketplace, order.identifier);
    }

    return false;
  }

  /**
   * Delete all orders for a task
   */
  deleteByTask(taskId: string): void {
    const taskKeys = this.taskIndex.get(taskId);
    if (!taskKeys) {
      return;
    }

    let deletedCount = 0;
    for (const key of taskKeys) {
      const order = this.cache.get(key);
      if (order) {
        this.idIndex.delete(order._id);
      }
      this.cache.delete(key);
      deletedCount++;
    }

    this.taskIndex.delete(taskId);
    this.initializedTasks.delete(taskId);

    if (DEBUG) {
      console.log(`[OrderCache] DELETE_BY_TASK: taskId=${taskId} | deleted=${deletedCount}`);
    }
  }

  /**
   * Delete all orders for a task/marketplace combination
   */
  deleteByTaskAndMarketplace(taskId: string, marketplace: string): void {
    const taskKeys = this.taskIndex.get(taskId);
    if (!taskKeys) {
      return;
    }

    const marketplaceLower = marketplace.toLowerCase();
    const keysToDelete: string[] = [];

    for (const key of taskKeys) {
      if (key.includes(`:${marketplaceLower}:`)) {
        keysToDelete.push(key);
      }
    }

    for (const key of keysToDelete) {
      const order = this.cache.get(key);
      if (order) {
        this.idIndex.delete(order._id);
      }
      this.cache.delete(key);
      taskKeys.delete(key);
    }

    if (taskKeys.size === 0) {
      this.taskIndex.delete(taskId);
    }

    if (DEBUG) {
      console.log(`[OrderCache] DELETE_BY_TASK_MARKETPLACE: taskId=${taskId} marketplace=${marketplace} | deleted=${keysToDelete.length}`);
    }
  }

  /**
   * Get all orders for a task (including expired)
   */
  getByTask(taskId: string): CachedOrder[] {
    const taskKeys = this.taskIndex.get(taskId);
    if (!taskKeys) {
      return [];
    }

    const orders: CachedOrder[] = [];
    for (const key of taskKeys) {
      const order = this.cache.get(key);
      if (order) {
        orders.push(order);
      }
    }

    return orders;
  }

  /**
   * Get all active (non-expired) orders for a task
   */
  getActiveByTask(taskId: string): CachedOrder[] {
    return this.getByTask(taskId).filter(order => !this.isExpired(order));
  }

  /**
   * Get all active orders for a task and marketplace
   */
  getByTaskAndMarketplace(taskId: string, marketplace: string): CachedOrder[] {
    const taskKeys = this.taskIndex.get(taskId);
    if (!taskKeys) {
      return [];
    }

    const marketplaceLower = marketplace.toLowerCase();
    const orders: CachedOrder[] = [];

    for (const key of taskKeys) {
      if (key.includes(`:${marketplaceLower}:`)) {
        const order = this.cache.get(key);
        if (order && !this.isExpired(order)) {
          orders.push(order);
        }
      }
    }

    return orders;
  }

  /**
   * Get orders by task and list of identifiers (for cancelRemovedTokenBids/cancelRemovedTraitBids)
   */
  getByTaskAndIdentifiers(taskId: string, identifiers: string[], marketplace?: string): CachedOrder[] {
    const taskKeys = this.taskIndex.get(taskId);
    if (!taskKeys) {
      return [];
    }

    const identifierSet = new Set(identifiers);
    const marketplaceLower = marketplace?.toLowerCase();
    const orders: CachedOrder[] = [];

    for (const key of taskKeys) {
      const order = this.cache.get(key);
      if (order && identifierSet.has(order.identifier)) {
        if (!marketplaceLower || order.marketplace.toLowerCase() === marketplaceLower) {
          orders.push(order);
        }
      }
    }

    return orders;
  }

  /**
   * Count active orders by task and marketplace
   */
  countByTaskAndMarketplace(taskId: string, marketplace: string): number {
    return this.getByTaskAndMarketplace(taskId, marketplace).length;
  }

  /**
   * Count all active orders grouped by marketplace (for bidStats)
   */
  countGroupedByMarketplace(taskId: string): { [marketplace: string]: number } {
    const taskKeys = this.taskIndex.get(taskId);
    if (!taskKeys) {
      return {};
    }

    const counts: { [marketplace: string]: number } = {};

    for (const key of taskKeys) {
      const order = this.cache.get(key);
      if (order && !this.isExpired(order)) {
        const mp = order.marketplace.toLowerCase();
        counts[mp] = (counts[mp] || 0) + 1;
      }
    }

    return counts;
  }

  /**
   * Check if an order is expired
   */
  isExpired(order: CachedOrder): boolean {
    return order.expiresAt <= Date.now();
  }

  /**
   * Clean up expired orders from cache
   */
  cleanupExpired(): void {
    const now = Date.now();
    let cleanedCount = 0;

    for (const [key, order] of this.cache) {
      if (order.expiresAt <= now) {
        this.idIndex.delete(order._id);
        this.cache.delete(key);

        const taskKeys = this.taskIndex.get(order.taskId);
        if (taskKeys) {
          taskKeys.delete(key);
          if (taskKeys.size === 0) {
            this.taskIndex.delete(order.taskId);
          }
        }

        cleanedCount++;
      }
    }

    if (DEBUG && cleanedCount > 0) {
      console.log(`[OrderCache] CLEANUP: removed ${cleanedCount} expired orders`);
    }
  }

  /**
   * Initialize cache from database for a specific task (lazy loading)
   */
  async initializeFromDatabase(taskId: string): Promise<void> {
    if (this.initializedTasks.has(taskId)) {
      if (DEBUG) {
        console.log(`[OrderCache] INIT: taskId=${taskId} already initialized, skipping`);
      }
      return;
    }

    try {
      const orders = await Order.find({
        taskId: taskId,
        expiresAt: { $gt: new Date() }
      }).lean();

      for (const order of orders) {
        this.set({
          _id: order._id.toString(),
          orderHash: order.orderHash,
          expiresAt: new Date(order.expiresAt).getTime(),
          offerPrice: order.offerPrice,
          createdAt: new Date(order.createdAt).getTime(),
          identifier: order.identifier,
          marketplace: order.marketplace,
          slug: order.slug,
          taskId: order.taskId
        });
      }

      this.initializedTasks.add(taskId);

      if (DEBUG) {
        console.log(`[OrderCache] INIT: taskId=${taskId} | loaded ${orders.length} orders from database`);
      }
    } catch (error) {
      console.error(`[OrderCache] INIT ERROR: taskId=${taskId}`, error);
    }
  }

  /**
   * Check if a task's cache has been initialized
   */
  isTaskInitialized(taskId: string): boolean {
    return this.initializedTasks.has(taskId);
  }

  /**
   * Get cache statistics for debugging
   */
  getStats(): { totalOrders: number; taskCount: number; initializedTasks: number } {
    return {
      totalOrders: this.cache.size,
      taskCount: this.taskIndex.size,
      initializedTasks: this.initializedTasks.size
    };
  }

  /**
   * Clear entire cache (for testing or server shutdown)
   */
  clear(): void {
    this.cache.clear();
    this.taskIndex.clear();
    this.idIndex.clear();
    this.initializedTasks.clear();

    if (DEBUG) {
      console.log(`[OrderCache] CLEAR: cache cleared`);
    }
  }
}

export const orderCache = new OrderCache();
