import WebSocket from 'ws';
import { BroadcastDeltaTracker } from './broadcast-delta-tracker';
import { debugLog } from './debug-file-logger';

/**
 * Client connection metadata
 */
interface ClientMetadata {
  ws: WebSocket;
  subscriptions: Set<string>; // Set of taskIds
  connectedAt: number;
}

/**
 * Statistics for monitoring
 */
export interface BroadcastStats {
  clients: number;
  totalSubscriptions: number;
  deltaTrackerSize: number;
}

/**
 * Direct WebSocket broadcast manager with delta detection and message reuse.
 * Replaces Redis Streams architecture for single-instance deployments.
 *
 * Optimized for memory efficiency:
 * - No message buffering (saves ~50MB for 100 tasks)
 * - Message reuse (serialize once, send to all subscribed clients)
 * - Hash-based delta detection (skip unchanged broadcasts)
 */
export class BroadcastManager {
  private clients = new Map<WebSocket, ClientMetadata>();
  private deltaTracker = new BroadcastDeltaTracker();
  private warnedTasks = new Set<string>(); // Track tasks warned about missing subscribers

  constructor() {
    // Constructor kept for API compatibility
  }

  /**
   * Register a new WebSocket client
   */
  addClient(ws: WebSocket): void {
    const metadata: ClientMetadata = {
      ws,
      subscriptions: new Set(),
      connectedAt: Date.now()
    };

    this.clients.set(ws, metadata);

  }

  /**
   * Remove client and cleanup subscriptions
   */
  removeClient(ws: WebSocket): void {
    const metadata = this.clients.get(ws);
    if (metadata) {
      this.clients.delete(ws);
    }
  }

  /**
   * Subscribe client to task updates
   */
  subscribe(ws: WebSocket, taskId: string): boolean {
    const metadata = this.clients.get(ws);
    if (!metadata) {
      return false;
    }
    metadata.subscriptions.add(taskId)
    return true;
  }

  /**
   * Subscribe all connected clients to a task
   * Used when a new task is started after clients have connected
   *
   * @param taskId - Task ID to subscribe clients to
   * @returns Number of clients subscribed
   */
  subscribeAllClients(taskId: string): number {
    let count = 0;
    for (const [ws] of this.clients.entries()) {
      if (this.subscribe(ws, taskId)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Unsubscribe client from task updates
   */
  unsubscribe(ws: WebSocket, taskId: string): boolean {
    const metadata = this.clients.get(ws);
    if (!metadata) {
      return false;
    }

    const removed = metadata.subscriptions.delete(taskId);
    return removed;
  }

  /**
   * Broadcast task update to subscribed clients (with delta detection and message reuse)
   */
  broadcastTaskUpdate(taskId: string, data: any): void {
    // Check if data changed (skip unchanged broadcasts)
    const { changed } = this.deltaTracker.hasChanged(taskId, data);

    // TEMPORARY FIX: Disable delta detection to ensure messages are always sent
    const forceUpdate = true;

    // Always log delta status

    if (!changed && !forceUpdate) {
      return;
    }

    // Find subscribed clients
    let subscriberCount = 0;
    for (const metadata of this.clients.values()) {
      if (metadata.subscriptions.has(taskId)) {
        subscriberCount++;
      }
    }

    // Always log subscriber count
    debugLog('BroadcastManager', `Subscriber check | taskId=${taskId} | count=${subscriberCount}`);

    if (subscriberCount === 0) {
      // Only warn if clients exist but aren't subscribed to this specific task
      // If no clients are connected at all, this is expected (e.g., during server startup)
      if (this.clients.size > 0 && !this.warnedTasks.has(taskId)) {

        this.warnedTasks.add(taskId);
      }
      return;
    }

    // Clear warning flag when task has subscribers again
    if (this.warnedTasks.has(taskId) && subscriberCount > 0) {
      this.warnedTasks.delete(taskId);
    }

    // Serialize message ONCE (message reuse optimization)
    const message = JSON.stringify({
      type: 'taskUpdate',
      taskId,
      data,
      timestamp: Date.now()
    });

    // Send to all subscribed clients (reuse serialized message)
    for (const [ws, metadata] of this.clients.entries()) {
      if (metadata.subscriptions.has(taskId) && ws.readyState === WebSocket.OPEN) {
        try {
          ws.send(message);
        } catch (error) {
          debugLog('BroadcastManager', `Send failed | taskId=${taskId} | error=${error}`);
        }
      }
    }
  }

  /**
   * Broadcast global update to ALL clients (no subscription filtering)
   */
  broadcastGlobalUpdate(data: any): void {
    const message = JSON.stringify({
      type: 'globalUpdate',
      data,
      timestamp: Date.now()
    });

    for (const [ws] of this.clients.entries()) {
      if (ws.readyState === WebSocket.OPEN) {
        try {
          ws.send(message);
        } catch (error) {
          debugLog('BroadcastManager', `Global send failed | error=${error}`);
        }
      }
    }
  }

  /**
   * Broadcast raw message to ALL clients (for heartbeat, instant price match, etc.)
   */
  broadcastRaw(message: string): number {
    let successCount = 0;
    for (const [ws] of this.clients.entries()) {
      if (ws.readyState === WebSocket.OPEN) {
        try {
          ws.send(message);
          successCount++;
        } catch (error) {
          debugLog('BroadcastManager', `Raw send failed | error=${error}`);
        }
      }
    }
    return successCount;
  }

  /**
   * Cleanup task delta tracking
   * Call this when a task is stopped/deleted
   */
  cleanupTask(taskId: string): void {
    this.deltaTracker.clearTask(taskId);
  }

  /**
   * Clear delta hash for a task to force next broadcast
   * Used for periodic full refresh to ensure client sync
   */
  clearDeltaForTask(taskId: string): void {
    this.deltaTracker.clearTask(taskId);
  }

  /**
   * Get statistics for monitoring
   */
  getStats(): BroadcastStats {
    let totalSubscriptions = 0;
    for (const metadata of this.clients.values()) {
      totalSubscriptions += metadata.subscriptions.size;
    }

    return {
      clients: this.clients.size,
      totalSubscriptions,
      deltaTrackerSize: this.deltaTracker.getSize()
    };
  }

  /**
   * Cleanup all resources
   * Call this on server shutdown
   */
  destroy(): void {
    this.clients.clear();
    this.deltaTracker.clearAll();
  }
}
// trigger reload 1768897074
