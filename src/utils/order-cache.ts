import { v4 as uuidv4 } from 'uuid';
import { getRedisConnection, withRedisRetry } from './redis';
import { errorLog, debugLog, warnLog, infoLog } from './debug-file-logger';

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

// Redis key prefixes
const ORDER_KEY_PREFIX = 'order:';
const ORDER_ID_PREFIX = 'order:id:';
const TASK_ORDERS_PREFIX = 'task:orders:';

class OrderCache {
  /**
   * Generate primary cache key from taskId, marketplace, and identifier
   * Format: "order:{taskId}:{marketplace}:{identifier}"
   */
  private getKey(taskId: string, marketplace: string, identifier: string): string {
    return `${ORDER_KEY_PREFIX}${taskId}:${marketplace.toLowerCase()}:${identifier}`;
  }

  /**
   * Generate order ID key for reverse lookup
   * Format: "order:id:{orderId}"
   */
  private getIdKey(orderId: string): string {
    return `${ORDER_ID_PREFIX}${orderId}`;
  }

  /**
   * Generate task orders set key
   * Format: "task:orders:{taskId}" or "task:orders:{taskId}:{marketplace}"
   */
  private getTaskOrdersKey(taskId: string, marketplace?: string): string {
    if (marketplace) {
      return `${TASK_ORDERS_PREFIX}${taskId}:${marketplace.toLowerCase()}`;
    }
    return `${TASK_ORDERS_PREFIX}${taskId}`;
  }

  /**
   * Calculate TTL in seconds from expiresAt timestamp
   * Returns at least 1 second to ensure valid TTL
   */
  private calculateTTL(expiresAt: number): number {
    return Math.max(1, Math.ceil((expiresAt - Date.now()) / 1000));
  }

  /**
   * Check if an order is expired
   */
  isExpired(order: CachedOrder): boolean {
    return order.expiresAt <= Date.now();
  }

  /**
   * Generate a new order ID (replaces MongoDB ObjectId)
   */
  generateOrderId(): string {
    return uuidv4();
  }

  /**
   * Add or update an order in the cache with TTL
   */
  async set(order: CachedOrder): Promise<void> {
    const redis = getRedisConnection();
    const key = this.getKey(order.taskId, order.marketplace, order.identifier);
    const ttl = this.calculateTTL(order.expiresAt);

    await withRedisRetry(async () => {
      const pipeline = redis.pipeline();

      // Store order data with TTL
      pipeline.setex(key, ttl, JSON.stringify(order));

      // Store ID -> key mapping with same TTL
      pipeline.setex(this.getIdKey(order._id), ttl, key);

      // Add to task orders set (no TTL - managed manually)
      pipeline.sadd(this.getTaskOrdersKey(order.taskId), key);

      // Add to task/marketplace orders set
      pipeline.sadd(this.getTaskOrdersKey(order.taskId, order.marketplace), key);

      await pipeline.exec();
    }, 'OrderCache.set');

    if (DEBUG) {
      debugLog('Cache', `[OrderCache] SET | key=${key} | orderId=${order._id} | ttl=${ttl}s | marketplace=${order.marketplace}`, { taskId: order.taskId, marketplace: order.marketplace as 'blur' | 'opensea' | 'magiceden' });
    }
  }

  /**
   * Add multiple orders in batch (for MagicEden bulk operations)
   */
  async setMany(orders: CachedOrder[]): Promise<void> {
    if (orders.length === 0) return;

    const redis = getRedisConnection();

    await withRedisRetry(async () => {
      const pipeline = redis.pipeline();

      for (const order of orders) {
        const key = this.getKey(order.taskId, order.marketplace, order.identifier);
        const ttl = this.calculateTTL(order.expiresAt);

        pipeline.setex(key, ttl, JSON.stringify(order));
        pipeline.setex(this.getIdKey(order._id), ttl, key);
        pipeline.sadd(this.getTaskOrdersKey(order.taskId), key);
        pipeline.sadd(this.getTaskOrdersKey(order.taskId, order.marketplace), key);
      }

      await pipeline.exec();
    }, 'OrderCache.setMany');

    if (DEBUG) {
      debugLog('Cache', `[OrderCache] SET_MANY | count=${orders.length} | taskId=${orders[0]?.taskId}`, { taskId: orders[0]?.taskId });
    }
  }

  /**
   * Get a single order from cache (returns undefined if expired or not found)
   */
  async get(taskId: string, marketplace: string, identifier: string): Promise<CachedOrder | undefined> {
    const redis = getRedisConnection();
    const key = this.getKey(taskId, marketplace, identifier);

    const result = await withRedisRetry(async () => {
      return redis.get(key);
    }, 'OrderCache.get');

    if (!result) {
      return undefined;
    }

    try {
      const order: CachedOrder = JSON.parse(result);

      // Double-check expiration (Redis TTL might have slight variance)
      if (this.isExpired(order)) {
        if (DEBUG) {
          debugLog('Cache', `[OrderCache] GET | key=${key} | EXPIRED`, { taskId, marketplace: marketplace as 'blur' | 'opensea' | 'magiceden' });
        }
        return undefined;
      }

      return order;
    } catch (error) {
      errorLog('Cache', `[OrderCache] GET | Parse error for key=${key}`, error);
      return undefined;
    }
  }

  /**
   * Delete a single order from cache
   */
  async delete(taskId: string, marketplace: string, identifier: string): Promise<boolean> {
    const redis = getRedisConnection();
    const key = this.getKey(taskId, marketplace, identifier);

    // First get the order to find its ID
    const orderJson = await withRedisRetry(async () => {
      return redis.get(key);
    }, 'OrderCache.delete.get');

    if (!orderJson) {
      return false;
    }

    let orderId: string | undefined;
    try {
      const order: CachedOrder = JSON.parse(orderJson);
      orderId = order._id;
    } catch (error) {
      warnLog('OrderCache', `JSON parse failed in delete | key=${key} | error=${error instanceof Error ? error.message : String(error)}`);
    }

    const deleted = await withRedisRetry(async () => {
      const pipeline = redis.pipeline();

      pipeline.del(key);
      if (orderId) {
        pipeline.del(this.getIdKey(orderId));
      }
      pipeline.srem(this.getTaskOrdersKey(taskId), key);
      pipeline.srem(this.getTaskOrdersKey(taskId, marketplace), key);

      const results = await pipeline.exec();
      // First result is the del command for the main key
      return results && results[0] && results[0][1] === 1;
    }, 'OrderCache.delete');

    if (DEBUG) {
      debugLog('Cache', `[OrderCache] DELETE | key=${key} | deleted=${deleted}`, { taskId, marketplace: marketplace as 'blur' | 'opensea' | 'magiceden' });
    }

    return deleted || false;
  }

  /**
   * Delete order by its _id (used in cancellation callbacks)
   */
  async deleteById(orderId: string): Promise<boolean> {
    const redis = getRedisConnection();

    // Get the key from ID index
    const key = await withRedisRetry(async () => {
      return redis.get(this.getIdKey(orderId));
    }, 'OrderCache.deleteById.getKey');

    if (!key) {
      return false;
    }

    // Get the order to find taskId and marketplace
    const orderJson = await withRedisRetry(async () => {
      return redis.get(key);
    }, 'OrderCache.deleteById.getOrder');

    if (!orderJson) {
      // Key in ID index but order already deleted (expired by TTL)
      await withRedisRetry(async () => {
        await redis.del(this.getIdKey(orderId));
      }, 'OrderCache.deleteById.cleanupIdKey');
      return false;
    }

    try {
      const order: CachedOrder = JSON.parse(orderJson);
      return this.delete(order.taskId, order.marketplace, order.identifier);
    } catch (error) {
      errorLog('Cache', `[OrderCache] DELETE_BY_ID | Parse error for orderId=${orderId}`, error);
      return false;
    }
  }

  /**
   * Delete all orders for a task
   */
  async deleteByTask(taskId: string): Promise<void> {
    const redis = getRedisConnection();
    const taskOrdersKey = this.getTaskOrdersKey(taskId);

    // Get all order keys for this task
    const keys = await withRedisRetry(async () => {
      return redis.smembers(taskOrdersKey);
    }, 'OrderCache.deleteByTask.getKeys');

    if (!keys || keys.length === 0) {
      return;
    }

    // Collect all order IDs to delete their index entries
    const orderIds: string[] = [];
    const orderJsons = await withRedisRetry(async () => {
      return redis.mget(...keys);
    }, 'OrderCache.deleteByTask.getOrders');

    if (orderJsons) {
      for (const json of orderJsons) {
        if (json) {
          try {
            const order: CachedOrder = JSON.parse(json);
            orderIds.push(order._id);
          } catch (error) {
            warnLog('OrderCache', `JSON parse failed in deleteByTask | taskId=${taskId} | error=${error instanceof Error ? error.message : String(error)}`);
          }
        }
      }
    }

    await withRedisRetry(async () => {
      const pipeline = redis.pipeline();

      // Delete all order keys
      if (keys.length > 0) {
        pipeline.del(...keys);
      }

      // Delete all ID index entries
      if (orderIds.length > 0) {
        pipeline.del(...orderIds.map(id => this.getIdKey(id)));
      }

      // Delete task orders sets (both main and marketplace-specific)
      pipeline.del(taskOrdersKey);
      // Delete marketplace-specific sets
      for (const mp of ['opensea', 'magiceden', 'blur']) {
        pipeline.del(this.getTaskOrdersKey(taskId, mp));
      }

      await pipeline.exec();
    }, 'OrderCache.deleteByTask.delete');

    if (DEBUG) {
      debugLog('Cache', `[OrderCache] DELETE_BY_TASK | taskId=${taskId} | deleted=${keys.length}`, { taskId });
    }
  }

  /**
   * Delete all orders for a task/marketplace combination
   */
  async deleteByTaskAndMarketplace(taskId: string, marketplace: string): Promise<void> {
    const redis = getRedisConnection();
    const taskMpOrdersKey = this.getTaskOrdersKey(taskId, marketplace);

    // Get all order keys for this task/marketplace
    const keys = await withRedisRetry(async () => {
      return redis.smembers(taskMpOrdersKey);
    }, 'OrderCache.deleteByTaskAndMarketplace.getKeys');

    if (!keys || keys.length === 0) {
      return;
    }

    // Collect all order IDs to delete their index entries
    const orderIds: string[] = [];
    const orderJsons = await withRedisRetry(async () => {
      return redis.mget(...keys);
    }, 'OrderCache.deleteByTaskAndMarketplace.getOrders');

    if (orderJsons) {
      for (const json of orderJsons) {
        if (json) {
          try {
            const order: CachedOrder = JSON.parse(json);
            orderIds.push(order._id);
          } catch (error) {
            warnLog('OrderCache', `JSON parse failed in deleteByTaskAndMarketplace | taskId=${taskId} | marketplace=${marketplace} | error=${error instanceof Error ? error.message : String(error)}`);
          }
        }
      }
    }

    await withRedisRetry(async () => {
      const pipeline = redis.pipeline();

      // Delete all order keys
      if (keys.length > 0) {
        pipeline.del(...keys);
      }

      // Delete all ID index entries
      if (orderIds.length > 0) {
        pipeline.del(...orderIds.map(id => this.getIdKey(id)));
      }

      // Remove from task orders set
      for (const key of keys) {
        pipeline.srem(this.getTaskOrdersKey(taskId), key);
      }

      // Delete marketplace-specific set
      pipeline.del(taskMpOrdersKey);

      await pipeline.exec();
    }, 'OrderCache.deleteByTaskAndMarketplace.delete');

    if (DEBUG) {
      debugLog('Cache', `[OrderCache] DELETE_BY_TASK_MARKETPLACE | taskId=${taskId} | marketplace=${marketplace} | deleted=${keys.length}`, { taskId, marketplace: marketplace as 'blur' | 'opensea' | 'magiceden' });
    }
  }

  /**
   * Get all orders for a task (including potentially expired ones from Redis)
   */
  async getByTask(taskId: string): Promise<CachedOrder[]> {
    const redis = getRedisConnection();
    const taskOrdersKey = this.getTaskOrdersKey(taskId);

    const keys = await withRedisRetry(async () => {
      return redis.smembers(taskOrdersKey);
    }, 'OrderCache.getByTask.getKeys');

    if (!keys || keys.length === 0) {
      return [];
    }

    const orderJsons = await withRedisRetry(async () => {
      return redis.mget(...keys);
    }, 'OrderCache.getByTask.getOrders');

    const orders: CachedOrder[] = [];
    const expiredKeys: string[] = [];

    if (orderJsons) {
      for (let i = 0; i < orderJsons.length; i++) {
        const json = orderJsons[i];
        if (json) {
          try {
            const order: CachedOrder = JSON.parse(json);
            orders.push(order);
          } catch (error) {
            warnLog('OrderCache', `JSON parse failed in getByTask | taskId=${taskId} | error=${error instanceof Error ? error.message : String(error)}`);
          }
        } else {
          // Key in set but order expired/deleted - track for cleanup
          expiredKeys.push(keys[i]);
        }
      }
    }

    // Clean up stale set entries (keys that no longer exist)
    if (expiredKeys.length > 0) {
      await withRedisRetry(async () => {
        await redis.srem(taskOrdersKey, ...expiredKeys);
      }, 'OrderCache.getByTask.cleanupStaleKeys');
    }

    return orders;
  }

  /**
   * Get all active (non-expired) orders for a task
   */
  async getActiveByTask(taskId: string): Promise<CachedOrder[]> {
    const orders = await this.getByTask(taskId);
    return orders.filter(order => !this.isExpired(order));
  }

  /**
   * Get all active orders for a task and marketplace
   */
  async getByTaskAndMarketplace(taskId: string, marketplace: string): Promise<CachedOrder[]> {
    const redis = getRedisConnection();
    const taskMpOrdersKey = this.getTaskOrdersKey(taskId, marketplace);

    const keys = await withRedisRetry(async () => {
      return redis.smembers(taskMpOrdersKey);
    }, 'OrderCache.getByTaskAndMarketplace.getKeys');

    if (!keys || keys.length === 0) {
      return [];
    }

    const orderJsons = await withRedisRetry(async () => {
      return redis.mget(...keys);
    }, 'OrderCache.getByTaskAndMarketplace.getOrders');

    const orders: CachedOrder[] = [];
    const expiredKeys: string[] = [];

    if (orderJsons) {
      for (let i = 0; i < orderJsons.length; i++) {
        const json = orderJsons[i];
        if (json) {
          try {
            const order: CachedOrder = JSON.parse(json);
            if (!this.isExpired(order)) {
              orders.push(order);
            }
          } catch (error) {
            warnLog('OrderCache', `JSON parse failed in getByTaskAndMarketplace | taskId=${taskId} | marketplace=${marketplace} | error=${error instanceof Error ? error.message : String(error)}`);
          }
        } else {
          expiredKeys.push(keys[i]);
        }
      }
    }

    // Clean up stale set entries
    if (expiredKeys.length > 0) {
      await withRedisRetry(async () => {
        const pipeline = redis.pipeline();
        pipeline.srem(taskMpOrdersKey, ...expiredKeys);
        pipeline.srem(this.getTaskOrdersKey(taskId), ...expiredKeys);
        await pipeline.exec();
      }, 'OrderCache.getByTaskAndMarketplace.cleanupStaleKeys');
    }

    return orders;
  }

  /**
   * Get orders by task and list of identifiers (for cancelRemovedTokenBids/cancelRemovedTraitBids)
   */
  async getByTaskAndIdentifiers(taskId: string, identifiers: string[], marketplace?: string): Promise<CachedOrder[]> {
    if (identifiers.length === 0) {
      return [];
    }

    const orders = marketplace
      ? await this.getByTaskAndMarketplace(taskId, marketplace)
      : await this.getActiveByTask(taskId);

    const identifierSet = new Set(identifiers);
    return orders.filter(order => identifierSet.has(order.identifier));
  }

  /**
   * Count active orders by task and marketplace
   */
  async countByTaskAndMarketplace(taskId: string, marketplace: string): Promise<number> {
    const orders = await this.getByTaskAndMarketplace(taskId, marketplace);
    return orders.length;
  }

  /**
   * Count total active (non-expired) orders for a task
   */
  async countByTask(taskId: string): Promise<number> {
    const orders = await this.getActiveByTask(taskId);
    return orders.length;
  }

  /**
   * Count all active orders grouped by marketplace (for bidStats)
   */
  async countGroupedByMarketplace(taskId: string): Promise<{ [marketplace: string]: number }> {
    const orders = await this.getActiveByTask(taskId);

    const counts: { [marketplace: string]: number } = {};
    for (const order of orders) {
      const mp = order.marketplace.toLowerCase();
      counts[mp] = (counts[mp] || 0) + 1;
    }

    return counts;
  }

  /**
   * Count only VALID orders (that actually exist in Redis) grouped by marketplace.
   * This method validates that order keys exist before counting, and cleans up
   * stale entries from the task:orders set that point to expired/deleted orders.
   *
   * Used during stopTask to get accurate counts for cancellation tracking.
   */
  async countValidByTaskAndMarketplace(taskId: string): Promise<Record<string, number>> {
    const redis = getRedisConnection();
    const setKey = `${TASK_ORDERS_PREFIX}${taskId}`;

    debugLog('OrderCache', `COUNT_VALID_START | taskId=${taskId} | setKey=${setKey}`);

    const result = await withRedisRetry(async () => {
      // Get all members from the task's order set
      const members = await redis.smembers(setKey);
      debugLog('OrderCache', `COUNT_VALID_SET_MEMBERS | taskId=${taskId} | memberCount=${members.length}`);

      const counts: Record<string, number> = { opensea: 0, magiceden: 0, blur: 0 };
      const staleMembers: string[] = [];
      let validCount = 0;
      let expiredCount = 0;

      // Check each member to see if the order key still exists
      for (const member of members) {
        const exists = await redis.exists(member);

        if (exists) {
          // Extract marketplace from key pattern: order:{taskId}:{marketplace}:{identifier}
          const parts = member.split(':');
          if (parts.length >= 3) {
            const marketplace = parts[2].toLowerCase();
            if (counts[marketplace] !== undefined) {
              counts[marketplace]++;
              validCount++;
            } else {
              debugLog('OrderCache', `COUNT_VALID_UNKNOWN_MP | taskId=${taskId} | member=${member} | marketplace=${marketplace}`);
            }
          }
        } else {
          staleMembers.push(member);
          expiredCount++;
        }
      }

      debugLog('OrderCache', `COUNT_VALID_SCAN_COMPLETE | taskId=${taskId} | valid=${validCount} | expired=${expiredCount} | stale=${staleMembers.length}`);

      // Clean up stale entries from the set
      if (staleMembers.length > 0) {
        await redis.srem(setKey, ...staleMembers);
        infoLog('OrderCache', `COUNT_VALID_CLEANUP | taskId=${taskId} | removedStaleEntries=${staleMembers.length} | staleKeys=${staleMembers.slice(0, 5).join(', ')}${staleMembers.length > 5 ? '...' : ''}`);
      }

      debugLog('OrderCache', `COUNT_VALID_RESULT | taskId=${taskId} | opensea=${counts.opensea} | magiceden=${counts.magiceden} | blur=${counts.blur} | total=${validCount}`);

      return counts;
    }, 'OrderCache.countValidByTaskAndMarketplace');

    return result || { opensea: 0, magiceden: 0, blur: 0 };
  }

  /**
   * Get the offer price for a specific order (for counterbid comparison)
   * Returns price as number (wei), or null if no active order exists
   */
  async getOfferPrice(taskId: string, marketplace: string, identifier: string): Promise<number | null> {
    const order = await this.get(taskId, marketplace, identifier);
    if (!order || this.isExpired(order)) {
      return null;
    }
    return Number(order.offerPrice);
  }

  /**
   * Get paginated orders for a task with optional marketplace filter
   * Returns orders sorted by createdAt descending
   */
  async getByTaskPaginated(
    taskId: string,
    options: {
      marketplace?: string | string[];
      skip?: number;
      limit?: number;
    } = {}
  ): Promise<{ orders: CachedOrder[]; total: number }> {
    // Get all active orders for the task
    let orders = await this.getActiveByTask(taskId);

    // Apply marketplace filter if specified
    if (options.marketplace) {
      const marketplaces = Array.isArray(options.marketplace)
        ? options.marketplace.map(m => m.toLowerCase())
        : [options.marketplace.toLowerCase()];
      orders = orders.filter(order => marketplaces.includes(order.marketplace.toLowerCase()));
    }

    // Sort by createdAt descending
    orders.sort((a, b) => b.createdAt - a.createdAt);

    // Get total count before pagination
    const total = orders.length;

    // Apply pagination
    const skip = options.skip || 0;
    const limit = options.limit || orders.length;
    orders = orders.slice(skip, skip + limit);

    return { orders, total };
  }

  /**
   * Get cache statistics for debugging
   */
  async getStats(): Promise<{ totalOrders: number; taskCount: number }> {
    const redis = getRedisConnection();

    const result = await withRedisRetry(async () => {
      // Count all order keys
      const orderKeys = await redis.keys(`${ORDER_KEY_PREFIX}*`);
      // Filter out ID keys
      const actualOrderKeys = orderKeys.filter(k => !k.startsWith(ORDER_ID_PREFIX));

      // Count task sets
      const taskKeys = await redis.keys(`${TASK_ORDERS_PREFIX}*`);
      // Only count main task keys (not marketplace-specific ones)
      const mainTaskKeys = taskKeys.filter(k => {
        const parts = k.split(':');
        return parts.length === 3; // task:orders:{taskId}
      });

      return {
        totalOrders: actualOrderKeys.length,
        taskCount: mainTaskKeys.length
      };
    }, 'OrderCache.getStats');

    return result || { totalOrders: 0, taskCount: 0 };
  }

  /**
   * Clear all orders (for testing or server shutdown)
   * WARNING: This clears ALL order data in Redis matching our prefixes
   */
  async clear(): Promise<void> {
    const redis = getRedisConnection();

    await withRedisRetry(async () => {
      // Find all our keys
      const orderKeys = await redis.keys(`${ORDER_KEY_PREFIX}*`);
      const taskOrderKeys = await redis.keys(`${TASK_ORDERS_PREFIX}*`);

      const allKeys = [...orderKeys, ...taskOrderKeys];

      if (allKeys.length > 0) {
        // Delete in batches of 1000 to avoid blocking Redis
        const batchSize = 1000;
        for (let i = 0; i < allKeys.length; i += batchSize) {
          const batch = allKeys.slice(i, i + batchSize);
          await redis.del(...batch);
        }
      }
    }, 'OrderCache.clear');

    if (DEBUG) {
      debugLog('Cache', `[OrderCache] CLEAR | cache cleared`);
    }
  }
}

export const orderCache = new OrderCache();
