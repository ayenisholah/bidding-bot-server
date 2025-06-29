import { Cluster } from 'ioredis';
import RedisClient from './redis';
import { logger, DatabaseOperation } from './logger';

export class DistributedLockManager {
  private readonly lockPrefix: string;
  private readonly defaultTTLSeconds: number;

  constructor(options?: {
    lockPrefix?: string;
    defaultTTLSeconds?: number;
  }) {
    this.lockPrefix = options?.lockPrefix ?? '{lock}:';
    this.defaultTTLSeconds = options?.defaultTTLSeconds ?? 30;
  }

  private get redis(): Cluster | null {
    try {
      return RedisClient.getClient();
    } catch {
      return null;
    }
  }

  private getLockKey(key: string): string {
    return `{${this.lockPrefix}${key}}`;
  }

  private async ensureConnection(): Promise<void> {
    if (!RedisClient.isReady() || !this.redis) {
      await new Promise(resolve => setTimeout(resolve, 1000));
      if (!RedisClient.isReady() || !this.redis) {
        await logger.logDatabaseConnectionError('Redis connection not ready for lock operation', {
          operation: 'distributed_lock',
          lockPrefix: this.lockPrefix
        });
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

  async checkExistLock(key: string): Promise<Boolean> {
    try {
      await this.ensureConnection();
      const lockKey = this.getLockKey(key);
      const existLockKey = this.redis ? await this.redis.exists(lockKey) : 0;
      return existLockKey === 1;
    } catch (e) {
      await logger.logDatabaseError('Error checking if lock exists', {
        databaseOperation: DatabaseOperation.READ,
        collection_name: 'redis_locks',
        lockKey: this.getLockKey(key),
        error: e as Error,
        errorMessage: e instanceof Error ? e.message : String(e)
      });
      console.log('error in check ExistLock', e)
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
        console.error(`Failed to release lock for key ${key}:`, releaseError);
      }
      }
    } catch (error) {
      await logger.logDatabaseError(`Error in withLock for key ${key}`, {
        databaseOperation: DatabaseOperation.UPDATE,
        collection_name: 'redis_locks',
        lockKey: this.getLockKey(key),
        ttlSeconds,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
      console.error(`Error in withLock for key ${key}:`, error);
      try {
        await this.releaseLock(key);
      } catch (releaseError) {
        // Log but don't throw if we can't release the lock
        console.error(`Failed to release lock for key ${key}:`, releaseError);
      }
      throw error;
    }
  }
} 