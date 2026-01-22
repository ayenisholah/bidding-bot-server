import crypto from 'crypto';

/**
 * Delta detection for broadcast messages using MD5 hashing.
 * Prevents broadcasting unchanged data to WebSocket clients.
 */
export class BroadcastDeltaTracker {
  private lastHashes = new Map<string, string>();

  /**
   * Check if data has changed since last check.
   * @param taskId - Task identifier
   * @param data - Data to check for changes
   * @returns Object with changed flag and current hash
   */
  hasChanged(taskId: string, data: any): { changed: boolean; hash: string } {
    const dataString = JSON.stringify(data);
    const dataHash = crypto.createHash('md5').update(dataString).digest('hex');
    const lastHash = this.lastHashes.get(taskId);
    const changed = dataHash !== lastHash;

    if (changed) {
      this.lastHashes.set(taskId, dataHash);
    }

    return { changed, hash: dataHash };
  }

  /**
   * Clear delta tracking for a specific task.
   * Call this when a task is stopped/deleted.
   * @param taskId - Task identifier to clear
   */
  clearTask(taskId: string): void {
    this.lastHashes.delete(taskId);
  }

  /**
   * Clear all delta tracking state.
   * Call this on server shutdown or reset.
   */
  clearAll(): void {
    this.lastHashes.clear();
  }

  /**
   * Get current number of tracked tasks.
   * Useful for monitoring/debugging.
   */
  getSize(): number {
    return this.lastHashes.size;
  }
}
