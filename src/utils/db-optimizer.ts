import { PrismaClient, Order as PrismaOrder } from '@prisma/client';
import { logger, DatabaseOperation, ErrorCategory } from './logger';

interface OrderQuery {
  taskId: string;
  marketplace: string;
  identifier?: string;
}

interface BulkOrderQuery {
  queries: OrderQuery[];
}

class DatabaseOptimizer {
  private prisma: PrismaClient;
  private orderCache: Map<string, PrismaOrder[]> = new Map();
  private readonly CACHE_TTL = 5000; // 5 seconds for order cache
  private cacheTimestamps: Map<string, number> = new Map();

  constructor(prisma: PrismaClient) {
    this.prisma = prisma;
    
    // Clear old cache entries periodically
    setInterval(() => this.cleanupCache(), 30000); // Every 30 seconds
  }

  private getCacheKey(query: OrderQuery): string {
    return `${query.taskId}-${query.marketplace}-${query.identifier || 'all'}`;
  }

  private cleanupCache() {
    const now = Date.now();
    for (const [key, timestamp] of this.cacheTimestamps.entries()) {
      if (now - timestamp > this.CACHE_TTL) {
        this.orderCache.delete(key);
        this.cacheTimestamps.delete(key);
      }
    }
  }

  // Get orders with caching
  async getOrders(query: OrderQuery): Promise<PrismaOrder[]> {
    const cacheKey = this.getCacheKey(query);
    const cached = this.orderCache.get(cacheKey);
    const cacheTimestamp = this.cacheTimestamps.get(cacheKey);
    
    if (cached && cacheTimestamp && Date.now() - cacheTimestamp < this.CACHE_TTL) {
      return cached;
    }

    try {
      const where: any = {
        taskId: query.taskId,
        marketplace: query.marketplace
      };
      
      if (query.identifier) {
        where.identifier = query.identifier;
      }

      const orders = await this.prisma.order.findMany({
        where,
        orderBy: { createdAt: 'desc' }
      });

      // Cache the result
      this.orderCache.set(cacheKey, orders);
      this.cacheTimestamps.set(cacheKey, Date.now());

      return orders;
    } catch (error) {
      await logger.logDatabaseError('Failed to fetch orders', {
        databaseOperation: DatabaseOperation.READ,
        collection_name: 'orders',
        error: error as Error,
        query
      });
      throw error;
    }
  }

  // Batch fetch orders for multiple queries
  async batchGetOrders(queries: OrderQuery[]): Promise<Map<string, PrismaOrder[]>> {
    const results = new Map<string, PrismaOrder[]>();
    const uncachedQueries: OrderQuery[] = [];

    // Check cache first
    for (const query of queries) {
      const cacheKey = this.getCacheKey(query);
      const cached = this.orderCache.get(cacheKey);
      const cacheTimestamp = this.cacheTimestamps.get(cacheKey);
      
      if (cached && cacheTimestamp && Date.now() - cacheTimestamp < this.CACHE_TTL) {
        results.set(cacheKey, cached);
      } else {
        uncachedQueries.push(query);
      }
    }

    // Batch fetch uncached queries
    if (uncachedQueries.length > 0) {
      try {
        // Build OR conditions for batch query
        const conditions = uncachedQueries.map(q => ({
          taskId: q.taskId,
          marketplace: q.marketplace,
          ...(q.identifier ? { identifier: q.identifier } : {})
        }));

        const orders = await this.prisma.order.findMany({
          where: {
            OR: conditions
          },
          orderBy: { createdAt: 'desc' }
        });

        // Group results by query
        for (const query of uncachedQueries) {
          const queryOrders = orders.filter(o => 
            o.taskId === query.taskId && 
            o.marketplace === query.marketplace &&
            (!query.identifier || o.identifier === query.identifier)
          );
          
          const cacheKey = this.getCacheKey(query);
          results.set(cacheKey, queryOrders);
          
          // Cache the result
          this.orderCache.set(cacheKey, queryOrders);
          this.cacheTimestamps.set(cacheKey, Date.now());
        }
      } catch (error) {
        await logger.logDatabaseError('Failed to batch fetch orders', {
          databaseOperation: DatabaseOperation.READ,
          collection_name: 'orders',
          error: error as Error,
          queryCount: uncachedQueries.length
        });
        throw error;
      }
    }

    return results;
  }

  // Invalidate cache for a specific task
  invalidateTaskCache(taskId: string) {
    for (const key of this.orderCache.keys()) {
      if (key.startsWith(`${taskId}-`)) {
        this.orderCache.delete(key);
        this.cacheTimestamps.delete(key);
      }
    }
  }

  // Create order with cache invalidation
  async createOrder(data: any): Promise<PrismaOrder> {
    try {
      const order = await this.prisma.order.create({ data });
      
      // Invalidate relevant caches
      this.invalidateTaskCache(order.taskId);
      
      return order;
    } catch (error) {
      await logger.logDatabaseError('Failed to create order', {
        databaseOperation: DatabaseOperation.CREATE,
        collection_name: 'orders',
        error: error as Error
      });
      throw error;
    }
  }

  // Bulk create orders
  async bulkCreateOrders(data: any[]): Promise<number> {
    try {
      const result = await this.prisma.order.createMany({ data });
      
      // Invalidate caches for all affected tasks
      const taskIds = new Set(data.map(d => d.taskId));
      for (const taskId of taskIds) {
        this.invalidateTaskCache(taskId);
      }
      
      return result.count;
    } catch (error) {
      await logger.logDatabaseError('Failed to bulk create orders', {
        databaseOperation: DatabaseOperation.CREATE,
        collection_name: 'orders',
        error: error as Error,
        count: data.length
      });
      throw error;
    }
  }

  // Delete orders with cache invalidation
  async deleteOrders(ids: string[]): Promise<number> {
    try {
      // First get the orders to know which tasks to invalidate
      const orders = await this.prisma.order.findMany({
        where: { id: { in: ids } },
        select: { taskId: true }
      });

      const result = await this.prisma.order.deleteMany({
        where: { id: { in: ids } }
      });

      // Invalidate caches
      const taskIds = new Set(orders.map(o => o.taskId));
      for (const taskId of taskIds) {
        this.invalidateTaskCache(taskId);
      }

      return result.count;
    } catch (error) {
      await logger.logDatabaseError('Failed to delete orders', {
        databaseOperation: DatabaseOperation.DELETE,
        collection_name: 'orders',
        error: error as Error,
        count: ids.length
      });
      throw error;
    }
  }
}

// Export singleton instance
let dbOptimizer: DatabaseOptimizer;

export function initializeDbOptimizer(prisma: PrismaClient) {
  dbOptimizer = new DatabaseOptimizer(prisma);
  return dbOptimizer;
}

export function getDbOptimizer(): DatabaseOptimizer {
  if (!dbOptimizer) {
    throw new Error('Database optimizer not initialized');
  }
  return dbOptimizer;
}