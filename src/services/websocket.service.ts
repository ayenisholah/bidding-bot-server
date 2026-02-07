import WebSocket, { Server as WebSocketServer } from 'ws';
import { LRUCache } from 'lru-cache';
import { ITask } from '../models/task.model';
import { BroadcastManager } from '../utils/broadcast-manager';
import { errorLog } from '../utils/debug-file-logger';

// Message deduplication with automatic TTL expiration
// Matches configuration in index.ts for consistency
const processedMessages = new LRUCache<string, number>({
  max: 1000000,         // 1M entries for production-scale traffic bursts
  ttl: 60 * 1000,       // 60 seconds
  updateAgeOnGet: false,
  allowStale: false
});

// Handler function types
interface WebSocketHandlers {
  updateTaskData: (task: ITask) => Promise<any>;
  processNewTask: (task: ITask) => Promise<any>;
  processImportedTasks: (tasks: ITask[]) => Promise<any>;
  updateStatus: (task: ITask & { stoppedByWarning?: boolean }) => Promise<any>;
  stopTask: (task: ITask, fromShutdown: boolean) => Promise<any>;
  deleteTask: (task: ITask) => Promise<any>;
  updateMultipleTasksStatus: (data: { tasks: ITask[], running: boolean }) => Promise<any>;
  processGlobalMarketplaces: (marketplaces: string[]) => Promise<any>;
  updateMarketplace: (task: ITask) => Promise<any>;
  handleStartListing: (data: { _id: string }, ws: WebSocket) => Promise<any>;
  handleStopListing: (data: { listingId: string }, ws: WebSocket) => Promise<any>;
  handleUpdateListing: (data: { _id: string, updates: any }, ws: WebSocket) => Promise<any>;
}

// Stats data structure
interface StatsData {
  bidRates: any;
  bidStats: any;
  skipStats: any;
  errorStats: any;
  floorPrices: any;
  bestOffers: any;
  warningBids: any;
  warningMessage: any;
  rps: any;
  taskStatus: any;
  ownBidCounts: any;
  loopCounterStats: any;
  counterbidCounts: any;
}

/**
 * WebSocket Service
 *
 * Manages WebSocket connections and message routing for the bidding bot.
 * Handles:
 * - Connection lifecycle (connect, disconnect)
 * - Auto-subscription to active tasks
 * - Message idempotency and deduplication
 * - Message routing to appropriate handlers
 * - Acknowledgment responses
 */
class WebSocketService {
  private wss: WebSocketServer | null = null;
  private broadcastManager: BroadcastManager | null = null;
  private activeTasks: Map<string, ITask> | null = null;
  private handlers: WebSocketHandlers | null = null;
  private getStatsData: (() => StatsData) | null = null;

  /**
   * Initialize the WebSocket service
   */
  public initialize(
    wss: WebSocketServer,
    broadcastManager: BroadcastManager,
    activeTasks: Map<string, ITask>,
    handlers: WebSocketHandlers,
    getStatsData: () => StatsData
  ): void {
    this.wss = wss;
    this.broadcastManager = broadcastManager;
    this.activeTasks = activeTasks;
    this.handlers = handlers;
    this.getStatsData = getStatsData;

    this.setupConnectionHandler();
  }

  /**
   * Set up the connection handler
   */
  private setupConnectionHandler(): void {
    if (!this.wss) {
      throw new Error('WebSocket server not initialized');
    }

    this.wss.on('connection', async (ws) => {
      await this.handleConnection(ws);
    });
  }

  /**
   * Handle new WebSocket connection
   */
  private async handleConnection(ws: WebSocket): Promise<void> {
    if (!this.broadcastManager || !this.activeTasks) {
      errorLog('WebSocket', 'Cannot handle connection: service not initialized');
      return;
    }

    // Register with broadcast manager
    this.broadcastManager.addClient(ws);

    // AUTO-SUBSCRIBE to all active tasks
    const activeTaskIds = Array.from(this.activeTasks.keys());
    let successCount = 0;
    let failCount = 0;
    for (const taskId of activeTaskIds) {
      try {
        this.broadcastManager.subscribe(ws, taskId);
        successCount++;
        // Per-task log removed - was logging 28+ lines per connection (125MB/day)
      } catch (error) {
        failCount++;
        errorLog('WebSocket', `Auto-subscribe failed | taskId=${taskId}`, error as Error);
      }
    }
    // Summary log preserved - single line shows count

    // Send the latest bid rates immediately upon connection
    if (ws.readyState === WebSocket.OPEN && this.getStatsData) {
      const stats = this.getStatsData();
      const initialMessage = JSON.stringify({
        type: "bidRatesUpdate",
        data: stats,
      });

      ws.send(initialMessage);
    }

    // Set up message handler for this connection
    ws.onmessage = async (event: WebSocket.MessageEvent) => {
      await this.handleMessage(ws, event);
    };

    // Set up close handler
    ws.onclose = () => {
      this.handleDisconnection(ws);
    };
  }

  /**
   * Handle incoming WebSocket message
   */
  private async handleMessage(ws: WebSocket, event: WebSocket.MessageEvent): Promise<void> {
    let messageId: string | undefined;
    let endpoint: string = 'unknown';

    try {
      const message = JSON.parse(event.data as string);
      messageId = message._id;
      endpoint = message.endpoint;

      // Validate message structure
      if (!endpoint) {
        throw new Error("Invalid message structure: missing endpoint");
      }

      // Check for duplicate (idempotency)
      if (messageId && processedMessages.has(messageId)) {
        // Send acknowledgment for duplicate
        ws.send(JSON.stringify({
          type: 'ack',
          messageId,
          endpoint,
          status: 'duplicate',
          timestamp: Date.now()
        }));
        return;
      }

      // Store message ID to prevent duplicates
      if (messageId) {
        processedMessages.set(messageId, Date.now());
      }

      // Route message to appropriate handler
      const result = await this.routeMessage(endpoint, message, ws);

      // Send success acknowledgment
      ws.send(JSON.stringify({
        type: 'ack',
        messageId,
        endpoint,
        status: 'success',
        result,
        timestamp: Date.now()
      }));

    } catch (error) {
      errorLog('API', `WebSocket message handler failed: endpoint=${endpoint}, messageId=${messageId}`, error as Error);

      // Send error acknowledgment to client
      try {
        ws.send(JSON.stringify({
          type: 'ack',
          messageId,
          endpoint,
          status: 'error',
          error: (error as Error).message,
          timestamp: Date.now()
        }));
      } catch (sendError) {
        // WebSocket might be closed
        errorLog('API', 'Failed to send error acknowledgment', sendError as Error);
      }
    }
  }

  /**
   * Route message to appropriate handler based on endpoint
   */
  private async routeMessage(endpoint: string, message: any, ws: WebSocket): Promise<any> {
    if (!this.handlers || !this.broadcastManager) {
      throw new Error('WebSocket service not fully initialized');
    }

    switch (endpoint) {
      case 'updated-task':
        // Validate task data
        if (!message.data || !message.data._id) {
          throw new Error("Missing task data or task ID in update");
        }
        return await this.handlers.updateTaskData(message.data);

      case 'new-task':
        return await this.handlers.processNewTask(message.data);

      case "import-tasks":
        return await this.handlers.processImportedTasks(message.data);

      case 'toggle-status':
        return await this.handlers.updateStatus(message.data);

      case 'stop-task':
        return await this.handlers.stopTask(message.data, false);

      case 'delete-task':
        return await this.handlers.deleteTask(message.data);

      case 'update-multiple-tasks-status':
        return await this.handlers.updateMultipleTasksStatus(message.data);

      case 'globalMarketplaces':
        return await this.handlers.processGlobalMarketplaces(message.data);

      case 'update-marketplace':
        return await this.handlers.updateMarketplace(message.data);

      case 'create-listing':
        return await this.handlers.handleStartListing(message.data, ws);

      case 'stop-listing':
        return await this.handlers.handleStopListing(message.data, ws);

      case 'update-listing':
        return await this.handlers.handleUpdateListing(message.data, ws);

      case 'subscribe':
        // Subscribe to task updates via broadcast manager
        if (!message.data?.taskId) {
          throw new Error("Missing taskId in subscribe message");
        }
        const subscribeSuccess = this.broadcastManager.subscribe(ws, message.data.taskId);
        return { subscribed: subscribeSuccess, taskId: message.data.taskId };

      case 'unsubscribe':
        // Unsubscribe from task updates
        if (!message.data?.taskId) {
          throw new Error("Missing taskId in unsubscribe message");
        }
        const unsubscribeSuccess = this.broadcastManager.unsubscribe(ws, message.data.taskId);
        return { unsubscribed: unsubscribeSuccess, taskId: message.data.taskId };

      case 'catchup':
        // Catchup no longer supported (MessageBuffer removed for memory optimization)
        // Clients will receive next scheduled update after reconnection
        return { error: 'Catchup no longer supported', taskId: message.data?.taskId };

      default:
        throw new Error(`Unknown endpoint: ${endpoint}`);
    }
  }

  /**
   * Handle WebSocket disconnection
   */
  private handleDisconnection(ws: WebSocket): void {
    if (!this.broadcastManager) {
      return;
    }

    // Remove client and cleanup subscriptions
    this.broadcastManager.removeClient(ws);
  }


  /**
   * Get statistics about the service
   */
  public getStats(): {
    processedMessagesCount: number;
  } {
    return {
      processedMessagesCount: processedMessages.size
    };
  }
}

export const websocketService = new WebSocketService();

/**
 * Get processed messages cache for debugging/monitoring
 * @internal - For use by lru-cache-sampler.ts only
 */
export function getProcessedMessages(): LRUCache<string, number> {
  return processedMessages;
}
