import { Redis } from 'ioredis';
import RedisClient from './redis';
import { errorLog, warnLog } from './debug-file-logger';

export class DistributedLockManager {
  private readonly lockPrefix: string;
  private readonly defaultTTLSeconds: number;

  constructor(options?: {
    lockPrefix?: string;
    defaultTTLSeconds?: number;
  }) {
    this.lockPrefix = options?.lockPrefix ?? 'lock:';
    this.defaultTTLSeconds = options?.defaultTTLSeconds ?? 60;
  }

  private get redis(): Redis | null {
    try {
      return RedisClient.getClient();
    } catch {
      return null;
    }
  }

  private getLockKey(key: string): string {
    return `${this.lockPrefix}${key}`;
  }

  private async ensureConnection(): Promise<void> {
    if (!RedisClient.isReady() || !this.redis) {
      await new Promise(resolve => setTimeout(resolve, 1000));
      if (!RedisClient.isReady() || !this.redis) {
        warnLog('Lock', `[Redis] Connection not ready for lock operation | prefix=${this.lockPrefix}`);
        throw new Error('Redis connection not ready');
      }
    }
  }

  async acquireLock(
    key: string,
    ttlSeconds: number = this.defaultTTLSeconds
  ): Promise<boolean> {
    await this.ensureConnection();
    const lockKey = this.getLockKey(key);

    if (!this.redis) {
      throw new Error('Redis connection not available');
    }
    const acquired = await this.redis.setex(
      lockKey,
      ttlSeconds,
      '1',
    );

    return acquired === 'OK';
  }

  async releaseLock(key: string): Promise<void> {
    await this.ensureConnection();
    const lockKey = this.getLockKey(key);
    if (this.redis) {
      await this.redis.del(lockKey);
    }
  }

  async extendLock(key: string, additionalTTLSeconds: number): Promise<boolean> {
    await this.ensureConnection();
    const lockKey = this.getLockKey(key);
    
    if (!this.redis) {
      throw new Error('Redis connection not available');
    }
    
    // Check if lock exists first
    const exists = await this.redis.exists(lockKey);
    if (exists === 0) {
      return false;
    }
    
    // Extend the TTL
    const result = await this.redis.expire(lockKey, additionalTTLSeconds);
    return result === 1;
  }

  async checkExistLock(key: string): Promise<Boolean> {
    try {
      await this.ensureConnection();
      const lockKey = this.getLockKey(key);
      const existLockKey = this.redis ? await this.redis.exists(lockKey) : 0;
      return existLockKey === 1;
    } catch {
      warnLog('Lock', `[Redis] Error checking lock exists | key=${this.getLockKey(key)}`);
      return false;
    }
  }

  async withLock<T>(
    key: string,
    operation: () => Promise<T>,
    ttlSeconds: number = this.defaultTTLSeconds
  ): Promise<T | null> {
    try {
      const acquired = await this.acquireLock(key, ttlSeconds);

      if (!acquired) {
        return null;
      }

      try {
        return await operation();
      } finally {
        try {
        await this.releaseLock(key);
      } catch (releaseError) {
        // Log but don't throw if we can't release the lock
        errorLog('Lock', `Failed to release lock | key=${key}`, releaseError);
      }
      }
    } catch (error) {
      errorLog('Lock', `[Redis] Error in withLock | key=${key} | ttl=${ttlSeconds}`, error);
      try {
        await this.releaseLock(key);
      } catch (releaseError) {
        // Log but don't throw if we can't release the lock
        errorLog('Lock', `[Redis] Failed to release lock | key=${key}`, releaseError);
      }
      throw error;
    }
  }
} 