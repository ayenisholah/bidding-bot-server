import { getRedisConnection, withRedisRetry } from './redis';
import { errorLog, debugLog, infoLog } from './debug-file-logger';

/**
 * Redis-based cache for task scheduling data
 * Replaces MongoDB TaskSchedule model with pure Redis implementation
 *
 * Key structure: taskschedule:{taskId}:{marketplace}
 * TTL: 24 hours (86400 seconds)
 */

const SCHEDULE_KEY_PREFIX = 'taskschedule:';
const SCHEDULE_TTL_SECONDS = 24 * 60 * 60; // 24 hours

export type ScheduleStatus = 'PENDING' | 'FIRING' | 'COMPLETED' | 'CANCELLED';

export interface TaskScheduleData {
  taskId: string;
  marketplace: string;
  scheduledAt: string; // ISO date string
  nextFireTime: string; // ISO date string
  loopInterval: number;
  status: ScheduleStatus;
  executionId: string;
  firedAt?: string; // ISO date string
  attempts: number;
  createdAt: string; // ISO date string
  updatedAt: string; // ISO date string
}

class TaskScheduleCache {
  /**
   * Generate Redis key for a task schedule
   */
  private getKey(taskId: string, marketplace: string): string {
    return `${SCHEDULE_KEY_PREFIX}${taskId}:${marketplace.toLowerCase()}`;
  }

  /**
   * Get a task schedule from cache
   */
  async getSchedule(taskId: string, marketplace: string): Promise<TaskScheduleData | null> {
    const key = this.getKey(taskId, marketplace);

    try {
      const redis = getRedisConnection();
      const result = await withRedisRetry(async () => {
        return redis.get(key);
      }, 'TaskScheduleCache.getSchedule');

      if (!result) {
        return null;
      }

      const data = JSON.parse(result) as TaskScheduleData;
      return data;
    } catch (error) {
      errorLog('TaskScheduleCache', `Failed to get schedule | key=${key}`, error);
      return null;
    }
  }

  /**
   * Set/update a task schedule in cache
   */
  async setSchedule(
    taskId: string,
    marketplace: string,
    data: Omit<TaskScheduleData, 'taskId' | 'marketplace' | 'createdAt' | 'updatedAt'> & { createdAt?: string }
  ): Promise<void> {
    const key = this.getKey(taskId, marketplace);

    try {
      const redis = getRedisConnection();
      const now = new Date().toISOString();

      // Check if schedule exists to preserve createdAt
      const existing = await this.getSchedule(taskId, marketplace);

      const scheduleData: TaskScheduleData = {
        taskId,
        marketplace: marketplace.toLowerCase(),
        scheduledAt: data.scheduledAt,
        nextFireTime: data.nextFireTime,
        loopInterval: data.loopInterval,
        status: data.status,
        executionId: data.executionId,
        firedAt: data.firedAt,
        attempts: data.attempts,
        createdAt: data.createdAt || existing?.createdAt || now,
        updatedAt: now,
      };

      await withRedisRetry(async () => {
        await redis.setex(key, SCHEDULE_TTL_SECONDS, JSON.stringify(scheduleData));
      }, 'TaskScheduleCache.setSchedule');
    } catch (error) {
      errorLog('TaskScheduleCache', `Failed to set schedule | key=${key}`, error);
    }
  }

  /**
   * Update specific fields of a schedule
   */
  async updateSchedule(
    taskId: string,
    marketplace: string,
    updates: Partial<Omit<TaskScheduleData, 'taskId' | 'marketplace' | 'createdAt' | 'updatedAt'>>
  ): Promise<TaskScheduleData | null> {
    const key = this.getKey(taskId, marketplace);

    try {
      const existing = await this.getSchedule(taskId, marketplace);
      if (!existing) {
        return null;
      }

      const redis = getRedisConnection();
      const now = new Date().toISOString();

      const scheduleData: TaskScheduleData = {
        ...existing,
        ...updates,
        updatedAt: now,
      };

      await withRedisRetry(async () => {
        await redis.setex(key, SCHEDULE_TTL_SECONDS, JSON.stringify(scheduleData));
      }, 'TaskScheduleCache.updateSchedule');

      return scheduleData;
    } catch (error) {
      errorLog('TaskScheduleCache', `Failed to update schedule | key=${key}`, error);
      return null;
    }
  }

  /**
   * Delete a task schedule from cache
   */
  async deleteSchedule(taskId: string, marketplace: string): Promise<void> {
    const key = this.getKey(taskId, marketplace);

    try {
      const redis = getRedisConnection();
      await withRedisRetry(async () => {
        await redis.del(key);
      }, 'TaskScheduleCache.deleteSchedule');
    } catch (error) {
      errorLog('TaskScheduleCache', `Failed to delete schedule | key=${key}`, error);
    }
  }

  /**
   * Delete all schedules for a task (all marketplaces)
   */
  async deleteTaskSchedules(taskId: string): Promise<number> {
    const pattern = `${SCHEDULE_KEY_PREFIX}${taskId}:*`;

    try {
      const redis = getRedisConnection();
      let deletedCount = 0;

      await withRedisRetry(async () => {
        // Use SCAN to find all keys matching the pattern
        let cursor = '0';
        const keysToDelete: string[] = [];

        do {
          const [newCursor, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
          cursor = newCursor;
          keysToDelete.push(...keys);
        } while (cursor !== '0');

        if (keysToDelete.length > 0) {
          deletedCount = await redis.del(...keysToDelete);
        }
      }, 'TaskScheduleCache.deleteTaskSchedules');

      infoLog('TaskScheduleCache', `DELETE_TASK | taskId=${taskId} | deletedKeys=${deletedCount}`);
      return deletedCount;
    } catch (error) {
      errorLog('TaskScheduleCache', `Failed to delete task schedules | taskId=${taskId}`, error);
      return 0;
    }
  }

  /**
   * Get all schedules for a task
   */
  async getTaskSchedules(taskId: string): Promise<TaskScheduleData[]> {
    const pattern = `${SCHEDULE_KEY_PREFIX}${taskId}:*`;

    try {
      const redis = getRedisConnection();
      const schedules: TaskScheduleData[] = [];

      await withRedisRetry(async () => {
        let cursor = '0';
        const keys: string[] = [];

        do {
          const [newCursor, foundKeys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
          cursor = newCursor;
          keys.push(...foundKeys);
        } while (cursor !== '0');

        if (keys.length > 0) {
          const values = await redis.mget(...keys);
          for (const value of values) {
            if (value) {
              schedules.push(JSON.parse(value) as TaskScheduleData);
            }
          }
        }
      }, 'TaskScheduleCache.getTaskSchedules');

      return schedules;
    } catch (error) {
      errorLog('TaskScheduleCache', `Failed to get task schedules | taskId=${taskId}`, error);
      return [];
    }
  }

  /**
   * Check if a schedule exists
   */
  async exists(taskId: string, marketplace: string): Promise<boolean> {
    const key = this.getKey(taskId, marketplace);

    try {
      const redis = getRedisConnection();
      const result = await withRedisRetry(async () => {
        return redis.exists(key);
      }, 'TaskScheduleCache.exists');

      return result === 1;
    } catch (error) {
      errorLog('TaskScheduleCache', `Failed to check existence | key=${key}`, error);
      return false;
    }
  }

  /**
   * Cleanup old completed/cancelled schedules
   */
  async cleanupOldSchedules(maxAgeMs: number = 24 * 60 * 60 * 1000): Promise<number> {
    const pattern = `${SCHEDULE_KEY_PREFIX}*`;

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
                const data = JSON.parse(values[i] as string) as TaskScheduleData;
                if (
                  (data.status === 'COMPLETED' || data.status === 'CANCELLED') &&
                  new Date(data.updatedAt).getTime() < cutoffTime
                ) {
                  keysToDelete.push(keys[i]);
                }
              }
            }
          }
        } while (cursor !== '0');

        if (keysToDelete.length > 0) {
          deletedCount = await redis.del(...keysToDelete);
        }
      }, 'TaskScheduleCache.cleanupOldSchedules');

      if (deletedCount > 0) {
        infoLog('TaskScheduleCache', `CLEANUP | deletedSchedules=${deletedCount} | maxAgeMs=${maxAgeMs}`);
      }
      return deletedCount;
    } catch (error) {
      errorLog('TaskScheduleCache', 'Failed to cleanup old schedules', error);
      return 0;
    }
  }
}

// Export singleton instance
export const taskScheduleCache = new TaskScheduleCache();
