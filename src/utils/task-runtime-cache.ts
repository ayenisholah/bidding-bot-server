import { getRedisConnection, withRedisRetry } from './redis';
import { errorLog, debugLog, infoLog } from './debug-file-logger';

/**
 * Redis-based cache for task runtime state
 * Replaces MongoDB TaskRuntimeState model with pure Redis implementation
 *
 * Key structure: taskruntime:{taskId}
 * TTL: 7 days (604800 seconds)
 */

const RUNTIME_KEY_PREFIX = 'taskruntime:';
const RUNTIME_TTL_SECONDS = 7 * 24 * 60 * 60; // 7 days

export interface MarketplaceCounts {
  opensea: number;
  magiceden: number;
  blur: number;
}

export interface MarketplaceTokenIds {
  opensea: string[];
  magiceden: string[];
}

export interface MarketplaceTraits {
  opensea: Record<string, string[]>;
  magiceden: Record<string, string[]>;
  blur: Record<string, string[]>;
}

export interface MarketplaceBooleans {
  opensea: boolean;
  magiceden: boolean;
  blur: boolean;
}

export interface MarketplaceDates {
  opensea?: string; // ISO date string
  magiceden?: string;
  blur?: string;
}

export interface TaskStateInfo {
  status: 'STARTED' | 'QUEUEING' | 'RUNNING' | 'COMPLETED' | 'SCHEDULED' | 'STOPPED';
  queuedAt?: string; // ISO date string
  jobsTotal?: number;
  jobsCompleted?: number;
  lastActivityAt?: string; // ISO date string
}

export interface TaskRuntimeData {
  taskId: string;
  bidStats: MarketplaceCounts;
  skipStats: MarketplaceCounts;
  errorStats: MarketplaceCounts;
  currentTokenIds: MarketplaceTokenIds;
  currentTraits: MarketplaceTraits;
  floorPrices: MarketplaceCounts;
  bestOffers: MarketplaceCounts;
  warningBids: MarketplaceBooleans;
  taskState?: TaskStateInfo;
  nextLoopTimes?: MarketplaceDates;
  accessedAt: string; // ISO date string
  createdAt: string; // ISO date string
  updatedAt: string; // ISO date string
}

class TaskRuntimeCache {
  /**
   * Generate Redis key for a task runtime state
   */
  private getKey(taskId: string): string {
    return `${RUNTIME_KEY_PREFIX}${taskId}`;
  }

  /**
   * Get default runtime data structure
   */
  private getDefaultData(taskId: string): TaskRuntimeData {
    const now = new Date().toISOString();
    return {
      taskId,
      bidStats: { opensea: 0, magiceden: 0, blur: 0 },
      skipStats: { opensea: 0, magiceden: 0, blur: 0 },
      errorStats: { opensea: 0, magiceden: 0, blur: 0 },
      currentTokenIds: { opensea: [], magiceden: [] },
      currentTraits: { opensea: {}, magiceden: {}, blur: {} },
      floorPrices: { opensea: 0, magiceden: 0, blur: 0 },
      bestOffers: { opensea: 0, magiceden: 0, blur: 0 },
      warningBids: { opensea: false, magiceden: false, blur: false },
      accessedAt: now,
      createdAt: now,
      updatedAt: now,
    };
  }

  /**
   * Get task runtime state from cache
   */
  async getState(taskId: string): Promise<TaskRuntimeData | null> {
    const key = this.getKey(taskId);

    try {
      const redis = getRedisConnection();
      const result = await withRedisRetry(async () => {
        return redis.get(key);
      }, 'TaskRuntimeCache.getState');

      if (!result) {
        return null;
      }

      const data = JSON.parse(result) as TaskRuntimeData;
      debugLog('TaskRuntimeCache', `GET | key=${key} | status=${data.taskState?.status || 'none'}`);
      return data;
    } catch (error) {
      errorLog('TaskRuntimeCache', `Failed to get state | key=${key}`, error);
      return null;
    }
  }

  /**
   * Set/update task runtime state in cache
   */
  async setState(taskId: string, data: Partial<TaskRuntimeData>): Promise<void> {
    const key = this.getKey(taskId);

    try {
      const redis = getRedisConnection();
      const now = new Date().toISOString();

      // Get existing or create new
      const existing = await this.getState(taskId);
      const defaultData = this.getDefaultData(taskId);

      const runtimeData: TaskRuntimeData = {
        ...defaultData,
        ...existing,
        ...data,
        taskId,
        accessedAt: now,
        updatedAt: now,
        createdAt: existing?.createdAt || data.createdAt || now,
      };

      await withRedisRetry(async () => {
        await redis.setex(key, RUNTIME_TTL_SECONDS, JSON.stringify(runtimeData));
      }, 'TaskRuntimeCache.setState');

      debugLog('TaskRuntimeCache', `SET | key=${key} | status=${runtimeData.taskState?.status || 'none'} | ttl=${RUNTIME_TTL_SECONDS}s`);
    } catch (error) {
      errorLog('TaskRuntimeCache', `Failed to set state | key=${key}`, error);
    }
  }

  /**
   * Update specific fields of a runtime state
   */
  async updateState(
    taskId: string,
    updates: Partial<Omit<TaskRuntimeData, 'taskId' | 'createdAt' | 'updatedAt' | 'accessedAt'>>
  ): Promise<TaskRuntimeData | null> {
    const key = this.getKey(taskId);

    try {
      const existing = await this.getState(taskId);
      if (!existing) {
        debugLog('TaskRuntimeCache', `UPDATE_SKIP | key=${key} | reason=not_found`);
        return null;
      }

      const redis = getRedisConnection();
      const now = new Date().toISOString();

      const runtimeData: TaskRuntimeData = {
        ...existing,
        ...updates,
        accessedAt: now,
        updatedAt: now,
      };

      await withRedisRetry(async () => {
        await redis.setex(key, RUNTIME_TTL_SECONDS, JSON.stringify(runtimeData));
      }, 'TaskRuntimeCache.updateState');

      debugLog('TaskRuntimeCache', `UPDATE | key=${key} | status=${runtimeData.taskState?.status || 'none'}`);
      return runtimeData;
    } catch (error) {
      errorLog('TaskRuntimeCache', `Failed to update state | key=${key}`, error);
      return null;
    }
  }

  /**
   * Delete task runtime state from cache
   */
  async deleteState(taskId: string): Promise<void> {
    const key = this.getKey(taskId);

    try {
      const redis = getRedisConnection();
      await withRedisRetry(async () => {
        await redis.del(key);
      }, 'TaskRuntimeCache.deleteState');

      debugLog('TaskRuntimeCache', `DELETE | key=${key}`);
    } catch (error) {
      errorLog('TaskRuntimeCache', `Failed to delete state | key=${key}`, error);
    }
  }

  /**
   * Update task state info
   */
  async updateTaskState(taskId: string, taskState: Partial<TaskStateInfo>): Promise<TaskRuntimeData | null> {
    const existing = await this.getState(taskId);
    const currentState = existing?.taskState || {
      status: 'STARTED' as const,
      lastActivityAt: new Date().toISOString(),
    };

    return this.updateState(taskId, {
      taskState: {
        ...currentState,
        ...taskState,
      },
    });
  }

  /**
   * Update bid stats for a marketplace
   */
  async updateBidStats(taskId: string, marketplace: keyof MarketplaceCounts, count: number): Promise<void> {
    const existing = await this.getState(taskId);
    const currentBidStats = existing?.bidStats || { opensea: 0, magiceden: 0, blur: 0 };

    await this.updateState(taskId, {
      bidStats: {
        ...currentBidStats,
        [marketplace]: count,
      },
    });
  }

  /**
   * Increment bid stats for a marketplace
   */
  async incrementBidStats(taskId: string, marketplace: keyof MarketplaceCounts, increment: number = 1): Promise<void> {
    const existing = await this.getState(taskId);
    const currentBidStats = existing?.bidStats || { opensea: 0, magiceden: 0, blur: 0 };

    await this.updateState(taskId, {
      bidStats: {
        ...currentBidStats,
        [marketplace]: currentBidStats[marketplace] + increment,
      },
    });
  }

  /**
   * Update skip stats for a marketplace
   */
  async updateSkipStats(taskId: string, marketplace: keyof MarketplaceCounts, count: number): Promise<void> {
    const existing = await this.getState(taskId);
    const currentSkipStats = existing?.skipStats || { opensea: 0, magiceden: 0, blur: 0 };

    await this.updateState(taskId, {
      skipStats: {
        ...currentSkipStats,
        [marketplace]: count,
      },
    });
  }

  /**
   * Update error stats for a marketplace
   */
  async updateErrorStats(taskId: string, marketplace: keyof MarketplaceCounts, count: number): Promise<void> {
    const existing = await this.getState(taskId);
    const currentErrorStats = existing?.errorStats || { opensea: 0, magiceden: 0, blur: 0 };

    await this.updateState(taskId, {
      errorStats: {
        ...currentErrorStats,
        [marketplace]: count,
      },
    });
  }

  /**
   * Update floor price for a marketplace
   */
  async updateFloorPrice(taskId: string, marketplace: keyof MarketplaceCounts, price: number): Promise<void> {
    const existing = await this.getState(taskId);
    const currentFloorPrices = existing?.floorPrices || { opensea: 0, magiceden: 0, blur: 0 };

    await this.updateState(taskId, {
      floorPrices: {
        ...currentFloorPrices,
        [marketplace]: price,
      },
    });
  }

  /**
   * Update best offer for a marketplace
   */
  async updateBestOffer(taskId: string, marketplace: keyof MarketplaceCounts, offer: number): Promise<void> {
    const existing = await this.getState(taskId);
    const currentBestOffers = existing?.bestOffers || { opensea: 0, magiceden: 0, blur: 0 };

    await this.updateState(taskId, {
      bestOffers: {
        ...currentBestOffers,
        [marketplace]: offer,
      },
    });
  }

  /**
   * Update next loop time for a marketplace
   */
  async updateNextLoopTime(taskId: string, marketplace: keyof MarketplaceDates, time: Date | null): Promise<void> {
    const existing = await this.getState(taskId);
    const currentNextLoopTimes = existing?.nextLoopTimes || {};

    await this.updateState(taskId, {
      nextLoopTimes: {
        ...currentNextLoopTimes,
        [marketplace]: time ? time.toISOString() : undefined,
      },
    });
  }

  /**
   * Check if a runtime state exists
   */
  async exists(taskId: string): Promise<boolean> {
    const key = this.getKey(taskId);

    try {
      const redis = getRedisConnection();
      const result = await withRedisRetry(async () => {
        return redis.exists(key);
      }, 'TaskRuntimeCache.exists');

      return result === 1;
    } catch (error) {
      errorLog('TaskRuntimeCache', `Failed to check existence | key=${key}`, error);
      return false;
    }
  }

  /**
   * Get or create runtime state
   */
  async getOrCreate(taskId: string): Promise<TaskRuntimeData> {
    const existing = await this.getState(taskId);
    if (existing) {
      return existing;
    }

    const defaultData = this.getDefaultData(taskId);
    await this.setState(taskId, defaultData);
    return defaultData;
  }

  /**
   * Cleanup old states (for maintenance)
   */
  async cleanupOldStates(maxAgeMs: number = 7 * 24 * 60 * 60 * 1000): Promise<number> {
    const pattern = `${RUNTIME_KEY_PREFIX}*`;

    try {
      const redis = getRedisConnection();
      let deletedCount = 0;
      const cutoffTime = Date.now() - maxAgeMs;

      await withRedisRetry(async () => {
        let cursor = '0';
        const keysToDelete: string[] = [];

        do {
          const [newCursor, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
          cursor = newCursor;

          if (keys.length > 0) {
            const values = await redis.mget(...keys);
            for (let i = 0; i < values.length; i++) {
              if (values[i]) {
                const data = JSON.parse(values[i] as string) as TaskRuntimeData;
                if (new Date(data.accessedAt).getTime() < cutoffTime) {
                  keysToDelete.push(keys[i]);
                }
              }
            }
          }
        } while (cursor !== '0');

        if (keysToDelete.length > 0) {
          deletedCount = await redis.del(...keysToDelete);
        }
      }, 'TaskRuntimeCache.cleanupOldStates');

      if (deletedCount > 0) {
        infoLog('TaskRuntimeCache', `CLEANUP | deletedStates=${deletedCount} | maxAgeMs=${maxAgeMs}`);
      }
      return deletedCount;
    } catch (error) {
      errorLog('TaskRuntimeCache', 'Failed to cleanup old states', error);
      return 0;
    }
  }
}

// Export singleton instance
export const taskRuntimeCache = new TaskRuntimeCache();
