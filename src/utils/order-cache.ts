import { v4 as uuidv4 } from 'uuid';
import { LRUCache } from 'lru-cache';
import { errorLog, debugLog, warnLog, infoLog } from './debug-file-logger';
import { queue, CANCEL_OPENSEA_BID, CANCEL_BLUR_BID, CANCEL_MAGICEDEN_BID } from '../index';

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

// LRU cache for orders (in-memory only, no Redis)
// IMPORTANT: Each entry now stores an ARRAY of CachedOrder[] to support multiple orders per identifier
const orderLRU = new LRUCache<string, CachedOrder[]>({
  max: 1000000,           // 1M entries max (handle more concurrent active orders)
  ttl: 24 * 60 * 60 * 1000, // 24 hours TTL (safety net, manual expiration via expiresAt still primary)
  allowStale: false,
  updateAgeOnGet: false,
  updateAgeOnHas: false
});

// Cache statistics
interface CacheStats {
  hits: number;
  misses: number;
}

const stats = {
  lru: { hits: 0, misses: 0 } as CacheStats,
  replacements: { total: 0, cancelled: 0, failed: 0 },
  startTime: Date.now()
};

// Tracking functions
const trackLruHit = () => stats.lru.hits++;
const trackLruMiss = () => stats.lru.misses++;

/**
 * Get cache statistics for monitoring
 */
export function getOrderCacheStats() {
  const uptime = Math.floor((Date.now() - stats.startTime) / 1000 / 60);
  const lruTotal = stats.lru.hits + stats.lru.misses;

  return {
    lru: {
      hits: stats.lru.hits,
      misses: stats.lru.misses,
      hitRate: lruTotal > 0 ? `${((stats.lru.hits / lruTotal) * 100).toFixed(1)}%` : 'N/A',
      size: orderLRU.size,
      max: orderLRU.max
    },
    replacements: {
      total: stats.replacements.total,
      cancelled: stats.replacements.cancelled,
      failed: stats.replacements.failed,
      successRate: stats.replacements.total > 0
        ? `${((stats.replacements.cancelled / stats.replacements.total) * 100).toFixed(1)}%`
        : 'N/A'
    },
    uptime: `${uptime}min`
  };
}

/**
 * Reset cache statistics (for testing)
 */
export function resetOrderCacheStats() {
  stats.lru.hits = 0;
  stats.lru.misses = 0;
  stats.replacements.total = 0;
  stats.replacements.cancelled = 0;
  stats.replacements.failed = 0;
  stats.startTime = Date.now();
}

// Log stats every 5 minutes (aggregated, not per-access)
setInterval(() => {
  const cacheStats = getOrderCacheStats();
  infoLog('OrderCache',
    `[5min Summary] ` +
    `lru: ${cacheStats.lru.hits} hits, ${cacheStats.lru.misses} misses (${cacheStats.lru.hitRate}) | ` +
    `size: ${cacheStats.lru.size}/${cacheStats.lru.max} | ` +
    `uptime: ${cacheStats.uptime}`
  );

  // Log replacement stats if any occurred
  if (cacheStats.replacements.total > 0) {
    infoLog('OrderCache',
      `[REPLACEMENT_STATS] total=${cacheStats.replacements.total} | ` +
      `cancelled=${cacheStats.replacements.cancelled} | ` +
      `failed=${cacheStats.replacements.failed} | ` +
      `successRate=${cacheStats.replacements.successRate}`
    );
  }
}, 5 * 60 * 1000);

/**
 * Queue cancellation for a replaced order
 * Fire-and-forget pattern - doesn't block new bid placement
 * Accepts either a full CachedOrder or partial order data
 */
export async function queueReplacedOrderCancellation(
  oldOrder: CachedOrder | {
    taskId: string;
    orderHash: string;
    marketplace: string;
    identifier?: string;
    slug?: string;
    _id?: string;
    reason?: string;
  },
  reason?: string
): Promise<void> {
  try {
    stats.replacements.total++;

    // Extract reason from object or parameter
    const cancelReason = ('reason' in oldOrder && oldOrder.reason) || reason || 'replaced_by_new_bid';

    const marketplace = oldOrder.marketplace.toLowerCase();
    const jobType = marketplace === 'opensea'
      ? CANCEL_OPENSEA_BID
      : marketplace === 'blur'
        ? CANCEL_BLUR_BID
        : marketplace === 'magiceden'
          ? CANCEL_MAGICEDEN_BID
          : null;

    if (!jobType) {
      warnLog('OrderCache', `[CANCEL_SKIP] Unknown marketplace: ${marketplace}`);
      stats.replacements.failed++;
      return;
    }

    await queue.add(
      jobType,
      {
        taskId: oldOrder.taskId,
        orderHash: oldOrder.orderHash,
        orderId: oldOrder._id || '',
        slug: oldOrder.slug || '',
        marketplace: oldOrder.marketplace,
        reason: cancelReason
      },
      {
        jobId: `cancel-${oldOrder._id || oldOrder.orderHash}-${Date.now()}`,
        priority: 1 // Medium priority (after counterbids, before scheduled bids)
      }
    );

    stats.replacements.cancelled++;
    debugLog('OrderCache', `[CANCEL_QUEUED] ${oldOrder.marketplace} | orderHash=${oldOrder.orderHash} | reason=${cancelReason}`);
  } catch (error) {
    stats.replacements.failed++;
    errorLog('OrderCache', `[CANCEL_QUEUE_ERROR] Failed to queue cancellation for ${oldOrder.orderHash}`, error as Error);
  }
}

class OrderCache {
  /**
   * Generate LRU cache key
   * Format: "taskId:marketplace:identifier"
   */
  private getLruKey(taskId: string, marketplace: string, identifier: string): string {
    return `${taskId}:${marketplace.toLowerCase()}:${identifier}`;
  }

  /**
   * Check if an order is expired
   */
  isExpired(order: CachedOrder): boolean {
    return order.expiresAt <= Date.now();
  }

  /**
   * Clear all orders from LRU cache
   * Called during cleanup or testing
   */
  public clearLruCache(): void {
    orderLRU.clear();
    debugLog('Cache', '[OrderCache] LRU cache cleared');
  }

  /**
   * Get LRU cache size for monitoring
   */
  public getLruCacheSize(): number {
    return orderLRU.size;
  }

  /**
   * Generate a new order ID (replaces MongoDB ObjectId)
   */
  generateOrderId(): string {
    return uuidv4();
  }

  /**
   * Set/update an order in cache (sync, LRU only)
   * IMPORTANT: Appends to array instead of replacing (supports multiple orders per identifier)
   */
  set(order: CachedOrder): void {
    const lruKey = this.getLruKey(order.taskId, order.marketplace, order.identifier);
    const existing = orderLRU.get(lruKey) || [];

    // Check if order already exists (by _id or orderHash) to prevent duplicates
    const isDuplicate = existing.some(o => o._id === order._id || o.orderHash === order.orderHash);
    if (!isDuplicate) {
      existing.push(order);
      // Sort newest first (for getNewest() helper)
      existing.sort((a, b) => b.createdAt - a.createdAt);
    }

    orderLRU.set(lruKey, existing);
    debugLog('OrderCache', `SET | ${lruKey} | count=${existing.length} | newest=${new Date(existing[0].createdAt).toISOString()}`);
  }

  /**
   * Get all orders for taskId, marketplace, and identifier (sync)
   * Returns array of orders, or undefined if none found
   * IMPORTANT: Returns array to support multiple orders per identifier
   */
  get(taskId: string, marketplace: string, identifier: string): CachedOrder[] | undefined {
    const lruKey = this.getLruKey(taskId, marketplace, identifier);
    const cached = orderLRU.get(lruKey);

    if (!cached || cached.length === 0) {
      trackLruMiss();
      return undefined;
    }

    // Filter out expired orders (lazy cleanup)
    const activeOrders = cached.filter(order => !this.isExpired(order));

    if (activeOrders.length === 0) {
      orderLRU.delete(lruKey); // All expired
      trackLruMiss();
      return undefined;
    }

    // Update cache with only active orders if any were filtered out
    if (activeOrders.length !== cached.length) {
      orderLRU.set(lruKey, activeOrders);
    }

    trackLruHit();
    return activeOrders; // Already sorted by createdAt DESC from set()
  }

  /**
   * Get newest order for identifier (most recent createdAt)
   * Helper for single-order logic that needs latest bid
   */
  getNewest(taskId: string, marketplace: string, identifier: string): CachedOrder | undefined {
    const orders = this.get(taskId, marketplace, identifier);
    return orders?.[0]; // First element is newest (sorted DESC)
  }

  /**
   * Get oldest order for identifier (earliest createdAt)
   * Useful for FIFO cleanup or cancellation
   */
  getOldest(taskId: string, marketplace: string, identifier: string): CachedOrder | undefined {
    const orders = this.get(taskId, marketplace, identifier);
    return orders?.[orders.length - 1]; // Last element is oldest
  }

  /**
   * Get order count for identifier
   */
  getCount(taskId: string, marketplace: string, identifier: string): number {
    const orders = this.get(taskId, marketplace, identifier);
    return orders?.length || 0;
  }

  /**
   * Delete order by taskId, marketplace, and identifier (sync)
   * IMPORTANT: Deletes ALL orders for this identifier
   */
  delete(taskId: string, marketplace: string, identifier: string): boolean {
    const lruKey = this.getLruKey(taskId, marketplace, identifier);
    const existing = orderLRU.get(lruKey);
    const deleted = orderLRU.delete(lruKey);

    if (deleted) {
      const count = existing?.length || 0;
      debugLog('OrderCache', `DELETE | ${lruKey} | deletedCount=${count}`);
    }

    return deleted;
  }

  /**
   * Delete specific order by orderHash from identifier array
   * Useful for selective cancellation
   */
  deleteOne(taskId: string, marketplace: string, identifier: string, orderHash: string): boolean {
    const lruKey = this.getLruKey(taskId, marketplace, identifier);
    const existing = orderLRU.get(lruKey);

    if (!existing) {
      return false;
    }

    const filtered = existing.filter(order => order.orderHash !== orderHash);

    if (filtered.length === existing.length) {
      return false; // Not found
    }

    if (filtered.length === 0) {
      orderLRU.delete(lruKey); // No orders left
    } else {
      orderLRU.set(lruKey, filtered);
    }

    debugLog('OrderCache', `DELETE_ONE | ${lruKey} | remainingCount=${filtered.length}`);
    return true;
  }

  /**
   * Delete order by order ID (O(n) scan)
   * Rarely called - only from cancellation callbacks
   * IMPORTANT: Scans arrays for matching order
   */
  deleteById(orderId: string): boolean {
    for (const [lruKey, orders] of orderLRU.entries()) {
      const index = orders.findIndex(o => o._id === orderId);
      if (index !== -1) {
        orders.splice(index, 1);

        if (orders.length === 0) {
          orderLRU.delete(lruKey); // No orders left
        } else {
          orderLRU.set(lruKey, orders); // Update array
        }

        debugLog('OrderCache', `DELETE_BY_ID | orderId=${orderId} | key=${lruKey} | remainingCount=${orders.length}`);
        return true;
      }
    }
    return false;
  }

  /**
   * Get all active (non-expired) orders for a task
   * IMPORTANT: Flattens arrays to return all orders
   */
  getActiveByTask(taskId: string): CachedOrder[] {
    const allOrders: CachedOrder[] = [];

    for (const [, orders] of orderLRU.entries()) {
      for (const order of orders) {
        if (order.taskId === taskId) {
          if (!this.isExpired(order)) {
            allOrders.push(order);
          }
          // Note: Lazy cleanup handled by get() method
        }
      }
    }

    return allOrders;
  }

  /**
   * Get all active (non-expired) orders for a task and marketplace
   * IMPORTANT: Flattens arrays to return all orders
   */
  getByTaskAndMarketplace(taskId: string, marketplace: string): CachedOrder[] {
    const allOrders: CachedOrder[] = [];
    const marketplaceLower = marketplace.toLowerCase();

    for (const [, orders] of orderLRU.entries()) {
      for (const order of orders) {
        if (order.taskId === taskId && order.marketplace.toLowerCase() === marketplaceLower) {
          if (!this.isExpired(order)) {
            allOrders.push(order);
          }
        }
      }
    }

    return allOrders;
  }

  /**
   * Get all orders for a task (including expired, for cleanup)
   * IMPORTANT: Flattens arrays to return all orders
   */
  getAllByTask(taskId: string): CachedOrder[] {
    const allOrders: CachedOrder[] = [];

    for (const [, orders] of orderLRU.entries()) {
      for (const order of orders) {
        if (order.taskId === taskId) {
          allOrders.push(order);
        }
      }
    }

    return allOrders;
  }

  /**
   * Count active orders grouped by marketplace for a task
   * IMPORTANT: Counts all orders in arrays (not unique identifiers)
   */
  countGroupedByMarketplace(taskId: string): Record<string, number> {
    const counts: Record<string, number> = {};

    // Iterate through all LRU entries and clean up expired orders (lazy cleanup)
    for (const [lruKey, orders] of orderLRU.entries()) {
      // Filter expired orders for this key
      const activeOrders = orders.filter(order => {
        // Only process orders for this task
        if (order.taskId === taskId) {
          if (!this.isExpired(order)) {
            // Count non-expired order
            const marketplace = order.marketplace.toLowerCase();
            counts[marketplace] = (counts[marketplace] || 0) + 1;
            return true;  // Keep in array
          }
          // Expired order for this task - don't count, remove from array
          return false;
        }
        // Order belongs to different task - keep in array
        return true;
      });

      // Update cache if we removed any orders
      if (activeOrders.length !== orders.length) {
        if (activeOrders.length === 0) {
          orderLRU.delete(lruKey);  // All orders expired - delete key
          debugLog('OrderCache', `DELETE_EXPIRED | key=${lruKey} | expired=${orders.length}`);
        } else {
          orderLRU.set(lruKey, activeOrders);  // Update with filtered array
          debugLog('OrderCache', `CLEANUP_EXPIRED | key=${lruKey} | removed=${orders.length - activeOrders.length} | remaining=${activeOrders.length}`);
        }
      }
    }

    return counts;
  }

  /**
   * Delete all orders for a task (both active and expired)
   * IMPORTANT: Counts total orders deleted from all arrays
   */
  deleteByTask(taskId: string): number {
    let deletedCount = 0;

    for (const [lruKey, orders] of orderLRU.entries()) {
      // Check if any order in this array belongs to the task
      const hasTaskOrder = orders.some(order => order.taskId === taskId);
      if (hasTaskOrder) {
        // Count how many orders we're deleting
        const taskOrders = orders.filter(order => order.taskId === taskId);
        deletedCount += taskOrders.length;
        orderLRU.delete(lruKey);
      }
    }

    if (deletedCount > 0) {
      debugLog('OrderCache', `DELETE_BY_TASK | taskId=${taskId} | deleted=${deletedCount}`);
    }

    return deletedCount;
  }

  /**
   * Get order by orderHash within a task (O(n) scan)
   * IMPORTANT: Scans arrays for matching order
   */
  getByOrderHash(taskId: string, orderHash: string): CachedOrder | undefined {
    for (const [, orders] of orderLRU.entries()) {
      for (const order of orders) {
        if (order.taskId === taskId && order.orderHash === orderHash) {
          if (!this.isExpired(order)) {
            trackLruHit();
            return order;
          } else {
            // Order found but expired - don't return it
            trackLruMiss();
            return undefined;
          }
        }
      }
    }

    trackLruMiss();
    return undefined;
  }

  /**
   * Check if order exists (by taskId, marketplace, identifier)
   */
  has(taskId: string, marketplace: string, identifier: string): boolean {
    const order = this.get(taskId, marketplace, identifier);
    return order !== undefined;
  }

  /**
   * Update order's expiresAt timestamp
   * IMPORTANT: Updates the newest order for the identifier
   */
  updateExpiresAt(taskId: string, marketplace: string, identifier: string, newExpiresAt: number): boolean {
    const order = this.getNewest(taskId, marketplace, identifier);
    if (!order) {
      return false;
    }

    order.expiresAt = newExpiresAt;
    this.set(order);
    return true;
  }

  /**
   * Count total orders for a task (active only)
   * Optional marketplace filter (can be single string or array)
   */
  countByTask(taskId: string, options?: { marketplace?: string | string[] }): number {
    if (options?.marketplace) {
      const marketplaces = Array.isArray(options.marketplace)
        ? options.marketplace
        : [options.marketplace];

      return this.getActiveByTask(taskId).filter(order =>
        marketplaces.some(m => order.marketplace.toLowerCase() === m.toLowerCase())
      ).length;
    }
    return this.getActiveByTask(taskId).length;
  }

  /**
   * Count orders for task and marketplace (active only)
   */
  countByTaskAndMarketplace(taskId: string, marketplace: string): number {
    return this.getByTaskAndMarketplace(taskId, marketplace).length;
  }

  /**
   * Count valid (non-expired) orders grouped by marketplace for a task
   * Returns record with counts per marketplace
   */
  countValidByTaskAndMarketplace(taskId: string): Record<string, number> {
    return this.countGroupedByMarketplace(taskId);
  }

  /**
   * Delete orders by task and marketplace
   * IMPORTANT: Counts total orders deleted from all arrays
   */
  deleteByTaskAndMarketplace(taskId: string, marketplace: string): number {
    let deletedCount = 0;
    const marketplaceLower = marketplace.toLowerCase();

    for (const [lruKey, orders] of orderLRU.entries()) {
      // Check if any order in this array matches task and marketplace
      const matchingOrders = orders.filter(
        order => order.taskId === taskId && order.marketplace.toLowerCase() === marketplaceLower
      );

      if (matchingOrders.length > 0) {
        deletedCount += matchingOrders.length;
        orderLRU.delete(lruKey);
      }
    }

    if (deletedCount > 0) {
      debugLog('OrderCache', `DELETE_BY_TASK_MARKETPLACE | taskId=${taskId} | marketplace=${marketplace} | deleted=${deletedCount}`);
    }

    return deletedCount;
  }

  /**
   * Get orders by task and list of identifiers
   * Can be called with 2 args (all marketplaces) or 3 args (specific marketplace)
   * IMPORTANT: Returns all orders for matching identifiers (flattens arrays)
   */
  getByTaskAndIdentifiers(taskId: string, identifiers: string[], marketplace?: string): CachedOrder[] {
    const allOrders: CachedOrder[] = [];
    const identifierSet = new Set(identifiers);

    if (marketplace) {
      // Specific marketplace - use direct get (returns arrays now)
      for (const identifier of identifierSet) {
        const orders = this.get(taskId, marketplace, identifier);
        if (orders) {
          allOrders.push(...orders); // Flatten array
        }
      }
    } else {
      // All marketplaces - scan cache
      for (const [, orders] of orderLRU.entries()) {
        for (const order of orders) {
          if (order.taskId === taskId && identifierSet.has(order.identifier)) {
            if (!this.isExpired(order)) {
              allOrders.push(order);
            }
          }
        }
      }
    }

    return allOrders;
  }

  /**
   * Get offer price for an order (as number for arithmetic)
   * Returns undefined if order not found
   * IMPORTANT: Returns price of newest order
   */
  getOfferPrice(taskId: string, marketplace: string, identifier: string): number | undefined {
    const order = this.getNewest(taskId, marketplace, identifier);
    return order ? Number(order.offerPrice) : undefined;
  }

  /**
   * Get orders by task with pagination
   * Returns object with orders array and total count
   * Marketplace filter can be single string or array of strings
   */
  getByTaskPaginated(
    taskId: string,
    options: { marketplace?: string | string[]; skip?: number; limit?: number }
  ): { orders: CachedOrder[]; total: number } {
    const { marketplace, skip = 0 } = options;
    const limit = 'limit' in options ? options.limit : 100;

    let allOrders: CachedOrder[];

    if (marketplace) {
      const marketplaces = Array.isArray(marketplace) ? marketplace : [marketplace];
      allOrders = this.getActiveByTask(taskId).filter(order =>
        marketplaces.some(m => order.marketplace.toLowerCase() === m.toLowerCase())
      );
    } else {
      allOrders = this.getActiveByTask(taskId);
    }

    // Sort by createdAt DESC (newest first)
    allOrders.sort((a, b) => b.createdAt - a.createdAt);

    // If limit is undefined, return all orders (no slicing)
    if (limit === undefined) {
      return {
        orders: allOrders,
        total: allOrders.length
      };
    }

    return {
      orders: allOrders.slice(skip, skip + limit),
      total: allOrders.length
    };
  }

  /**
   * Set multiple orders at once (batch operation)
   */
  setMany(orders: CachedOrder[]): void {
    for (const order of orders) {
      this.set(order);
    }
  }

  /**
   * Get the internal orderLRU cache for debugging/monitoring
   * @internal - For use by lru-cache-sampler.ts only
   */
  getOrderLRU(): LRUCache<string, CachedOrder[]> {
    return orderLRU;
  }
}

export const orderCache = new OrderCache();
export default orderCache;
