/**
 * Cursor Manager for Cursor-Based Pull Architecture
 *
 * Manages the cursor position for the event pull loop. The cursor tracks
 * which events have been processed, enabling:
 * - Resume from last position after restart
 * - Gap detection when too far behind
 * - Periodic persistence to Redis
 */

import Redis from 'ioredis';
import { debugLog, warnLog, errorLog, infoLog } from './debug-file-logger';

const CURSOR_KEY = 'cursor:events:seq';
const CURSOR_BACKUP_KEY = 'cursor:events:seq:backup';
const CURSOR_TTL = 86400 * 7; // 7 days

export class CursorManager {
  private redis: Redis;
  private localCursor: number = 0;
  private lastPersisted: number = 0;
  private persistThreshold: number;
  private name: string;

  /**
   * Create a new cursor manager
   *
   * @param redis - Redis client instance
   * @param persistThreshold - Persist to Redis every N events (default 100)
   * @param name - Name for logging (default 'CursorManager')
   */
  constructor(redis: Redis, persistThreshold: number = 100, name: string = 'CursorManager') {
    this.redis = redis;
    this.persistThreshold = persistThreshold;
    this.name = name;
  }

  /**
   * Initialize cursor from Redis or start fresh
   *
   * @returns Current cursor position (0 if not found)
   */
  async initialize(): Promise<number> {
    try {
      // Try main cursor first
      const cursor = await this.redis.get(CURSOR_KEY);

      if (cursor) {
        this.localCursor = parseInt(cursor, 10);
        infoLog('PullCursor', `Restored cursor: ${this.localCursor}`);
        return this.localCursor;
      }

      // Try backup cursor
      const backup = await this.redis.get(CURSOR_BACKUP_KEY);
      if (backup) {
        this.localCursor = parseInt(backup, 10);
        infoLog('PullCursor', `Restored from backup: ${this.localCursor}`);
        return this.localCursor;
      }

      // No cursor found - will start from current head
      infoLog('PullCursor', 'No existing cursor, will sync to head');
      return 0;
    } catch (error) {
      errorLog('PullCursor', `Failed to load cursor: ${error instanceof Error ? error.message : String(error)}`);
      return 0;
    }
  }

  /**
   * Get current cursor position
   */
  getCursor(): number {
    return this.localCursor;
  }

  /**
   * Advance cursor after processing events
   * Persists to Redis periodically for efficiency
   *
   * @param newSeq - New sequence number to advance to
   */
  async advance(newSeq: number): Promise<void> {
    if (newSeq <= this.localCursor) {
      return; // Already at or past this position
    }

    this.localCursor = newSeq;

    // Persist periodically to reduce Redis writes
    if (this.localCursor - this.lastPersisted >= this.persistThreshold) {
      await this.persist();
    }
  }

  /**
   * Force persist cursor to Redis
   */
  async persist(): Promise<void> {
    try {
      const pipeline = this.redis.pipeline();

      // Set main cursor with TTL
      pipeline.setex(CURSOR_KEY, CURSOR_TTL, this.localCursor.toString());

      // Backup cursor
      pipeline.setex(CURSOR_BACKUP_KEY, CURSOR_TTL, this.localCursor.toString());

      await pipeline.exec();

      this.lastPersisted = this.localCursor;
      debugLog('PullCursor', `Persisted cursor: ${this.localCursor}`);
    } catch (error) {
      errorLog('PullCursor', `Failed to persist cursor: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Handle gap detection - reset cursor to tail
   *
   * @param tail - New tail position from ws-proxy
   */
  async resetToTail(tail: number): Promise<void> {
    warnLog('PullCursor', `Gap detected! Resetting cursor from ${this.localCursor} to tail ${tail}`);
    this.localCursor = tail - 1; // Will start reading from tail
    await this.persist();
  }

  /**
   * Sync cursor to head (skip all current events)
   * Used on first start to skip historical events
   *
   * @param head - Current head position from ws-proxy
   */
  async syncToHead(head: number): Promise<void> {
    infoLog('PullCursor', `Syncing to head: ${head}`);
    this.localCursor = head;
    await this.persist();
  }

  /**
   * Get cursor statistics
   */
  getStats(): {
    cursor: number;
    lastPersisted: number;
    persistThreshold: number;
    pendingPersist: number;
  } {
    return {
      cursor: this.localCursor,
      lastPersisted: this.lastPersisted,
      persistThreshold: this.persistThreshold,
      pendingPersist: this.localCursor - this.lastPersisted
    };
  }

  /**
   * Calculate lag from head
   *
   * @param head - Current head from ws-proxy
   * @returns Number of events behind
   */
  getLag(head: number): number {
    return Math.max(0, head - this.localCursor);
  }

  /**
   * Check if cursor is in a valid range
   *
   * @param tail - Current tail from ws-proxy
   * @param head - Current head from ws-proxy
   * @returns true if cursor is valid (between tail-1 and head)
   */
  isValid(tail: number, head: number): boolean {
    // Cursor 0 means not initialized yet
    if (this.localCursor === 0) return true;

    // Valid if cursor is >= tail - 1 and <= head
    return this.localCursor >= tail - 1 && this.localCursor <= head;
  }

  /**
   * Check if gap detected (cursor too far behind)
   *
   * @param tail - Current tail from ws-proxy
   * @returns true if gap detected
   */
  hasGap(tail: number): boolean {
    if (this.localCursor === 0) return false;
    return this.localCursor < tail - 1;
  }
}
