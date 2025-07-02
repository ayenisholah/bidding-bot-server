import WebSocket from 'ws';
import { EventEmitter } from 'events';
import { logger, ErrorCategory } from './logger';

export interface WebSocketManagerConfig {
  url: string;
  clientId: string;
  maxRetries?: number;
  reconnectBaseDelay?: number;
  reconnectMaxDelay?: number;
  heartbeatInterval?: number;
  connectionTimeout?: number;
  messageQueueLimit?: number;
}

export interface ConnectionMetrics {
  connectionStartTime: number | null;
  connectionEndTime: number | null;
  connectionDuration: number | null;
  currentConnectionId: string | null;
  totalConnections: number;
  totalDisconnections: number;
  totalMessages: number;
  totalErrors: number;
  connectionQuality: 'excellent' | 'good' | 'poor' | 'disconnected';
  averageLatency: number;
  messageRate: number;
  subscriptionCount: number;
  lastError?: Error;
}

export interface SubscriptionInfo {
  slug: string;
  contractAddress: string;
  marketplace: string;
  subscribedAt: number;
}

class WebSocketManager extends EventEmitter {
  private ws: WebSocket | null = null;
  private config: Required<WebSocketManagerConfig>;
  private intervals: Map<string, NodeJS.Timeout> = new Map();
  private messageQueue: any[] = [];
  private isReconnecting = false;
  private retryCount = 0;
  private activeSubscriptions: Map<string, SubscriptionInfo> = new Map();
  private pendingMessages: Map<string, { resolve: Function; reject: Function; timeout: NodeJS.Timeout }> = new Map();

  // Metrics
  private metrics: ConnectionMetrics = {
    connectionStartTime: null,
    connectionEndTime: null,
    connectionDuration: null,
    currentConnectionId: null,
    totalConnections: 0,
    totalDisconnections: 0,
    totalMessages: 0,
    totalErrors: 0,
    connectionQuality: 'disconnected',
    averageLatency: 0,
    messageRate: 0,
    subscriptionCount: 0
  };

  // Heartbeat tracking
  private lastPingSent = 0;
  private lastPongReceived = 0;
  private missedPongs = 0;
  private latencies: number[] = [];
  private messageTimestamps: number[] = [];

  constructor(config: WebSocketManagerConfig) {
    super();
    this.config = {
      maxRetries: 0, // 0 means infinite retries
      reconnectBaseDelay: 1000,
      reconnectMaxDelay: 60000,
      heartbeatInterval: 30000, // CLIENT_PROTOCOL.md specifies 30 seconds
      connectionTimeout: 10000,
      messageQueueLimit: 1000,
      ...config
    };
  }

  async connect(): Promise<void> {
    // Check if already connected
    if (this.ws?.readyState === WebSocket.OPEN) {
      logger.logInfo(ErrorCategory.WEBSOCKET, 'WebSocket already connected', {
        readyState: this.ws?.readyState
      });
      return;
    }

    // Check if already attempting to connect
    if (this.isReconnecting) {
      logger.logInfo(ErrorCategory.WEBSOCKET, 'WebSocket connection already in progress');
      return;
    }

    this.isReconnecting = true;
    logger.logInfo(ErrorCategory.WEBSOCKET, 'Attempting WebSocket connection', {
      url: this.config.url,
      clientId: this.config.clientId,
      attempt: this.retryCount + 1
    });

    try {
      // Clean up existing connection
      if (this.ws) {
        this.ws.removeAllListeners();
        if (this.ws.readyState !== WebSocket.CLOSED && this.ws.readyState !== WebSocket.CLOSING) {
          this.ws.close();
        }
        this.ws = null;
      }

      this.ws = new WebSocket(this.config.url);

      // Connection timeout
      const connectionTimeout = setTimeout(() => {
        logger.logWarning(ErrorCategory.WEBSOCKET, 'Connection timeout, closing and retrying');
        this.isReconnecting = false;
        if (this.ws) {
          this.ws.removeAllListeners();
          this.ws.close();
        }
        this.attemptReconnect();
      }, this.config.connectionTimeout);

      this.ws.once('open', () => {
        clearTimeout(connectionTimeout);
        this.isReconnecting = false;
        this.retryCount = 0;
        logger.logInfo(ErrorCategory.WEBSOCKET, 'WebSocket connection opened', {
          url: this.config.url,
          clientId: this.config.clientId
        });
        this.onConnectionEstablished();
      });

      this.ws.on('message', this.handleMessage.bind(this));
      this.ws.on('close', this.handleClose.bind(this));
      this.ws.on('error', this.handleError.bind(this));

    } catch (error) {
      logger.logWebSocketError('Connection attempt failed', error as Error);
      this.isReconnecting = false;
      this.attemptReconnect();
    }
  }

  private onConnectionEstablished(): void {
    // Initialize connection metrics
    const connectionId = `conn_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    this.metrics.connectionStartTime = Date.now();
    this.metrics.currentConnectionId = connectionId;
    this.metrics.totalConnections++;
    this.metrics.connectionQuality = 'good';

    logger.logInfo(ErrorCategory.WEBSOCKET, 'WebSocket connected', {
      connectionId,
      url: this.config.url
    });

    // Send initial ping immediately after connection (per CLIENT_PROTOCOL.md)
    this.sendPing();

    // Start heartbeat
    this.startHeartbeat();

    // Process queued messages
    this.processMessageQueue();

    // Sync subscriptions (don't await to prevent blocking connection)
    this.syncSubscriptions().catch(error => {
      logger.logWarning(ErrorCategory.WEBSOCKET, 'Subscription sync failed on connection', {
        error: error instanceof Error ? error : new Error('Unknown error')
      });
    });

    this.emit('connected', connectionId);
  }

  private startHeartbeat(): void {
    this.clearInterval('heartbeat');

    this.setInterval('heartbeat', () => {
      const now = Date.now();

      // Check for missed pongs
      if (this.lastPingSent && this.lastPingSent > this.lastPongReceived) {
        this.missedPongs++;
        if (this.missedPongs >= 3) {
          logger.logWarning(ErrorCategory.WEBSOCKET, 'Connection appears dead, reconnecting...', {
            missedPongs: this.missedPongs
          });
          this.ws?.close();
          return;
        }
      }

      // Send ping - server only needs event and clientId
      this.lastPingSent = now;

      this.sendMessage({
        event: 'ping',
        clientId: this.config.clientId
      });

    }, this.config.heartbeatInterval);
  }

  private async syncSubscriptions(): Promise<void> {
    try {
      // Skip sync if no active subscriptions
      if (this.activeSubscriptions.size === 0) {
        logger.logInfo(ErrorCategory.WEBSOCKET, 'No active subscriptions to sync');
        return;
      }

      logger.logInfo(ErrorCategory.WEBSOCKET, 'Syncing subscriptions', {
        localSubscriptions: this.activeSubscriptions.size
      });

      // Simply resubscribe to all collections
      // Server subscribes to ALL marketplaces when you subscribe once
      const subscribedSlugs = new Set<string>();

      for (const [, subInfo] of this.activeSubscriptions) {
        // Only subscribe once per collection (server handles all marketplaces)
        if (!subscribedSlugs.has(subInfo.slug)) {
          const message = {
            event: 'join_the_party',
            clientId: this.config.clientId,
            slug: subInfo.slug,
            contractAddress: subInfo.contractAddress
          };

          this.sendMessage(message);
          subscribedSlugs.add(subInfo.slug);

          // Small delay between subscriptions
          await new Promise(resolve => setTimeout(resolve, 100));
        }
      }

      logger.logInfo(ErrorCategory.WEBSOCKET, 'Subscription sync complete', {
        collections: subscribedSlugs.size
      });

    } catch (error) {
      logger.logWebSocketError('Failed to sync subscriptions', error instanceof Error ? error : new Error(String(error)));
      // Don't let sync failure kill the connection
      logger.logWarning(ErrorCategory.WEBSOCKET, 'Continuing without subscription sync', {
        error: error instanceof Error ? error : new Error('Unknown error')
      });
    }
  }

  private handleMessage(data: WebSocket.Data): void {
    try {
      const message = JSON.parse(data.toString());
      this.metrics.totalMessages++;
      this.recordMessage();

      // Handle pong messages
      if (message.type === 'pong') {
        this.handlePong(message.timestamp || Date.now());
        return;
      }

      // Handle subscription sync response
      if (message.type === 'subscription_sync') {
        logger.logInfo(ErrorCategory.WEBSOCKET, 'Received subscription sync', {
          subscriptions: message.subscriptions?.length || 0
        });
        // Server returns stringified JSON objects in subscriptions array
        // Format: ["{\"slug\":\"collection-slug\",\"contractAddress\":\"0x...\"}"]
        return;
      }

      // Handle server status
      if (message.type === 'server_status') {
        this.handleServerStatus(message);
        return;
      }

      // Handle server shutdown notification
      if (message.type === 'server_shutdown') {
        logger.logWarning(ErrorCategory.WEBSOCKET, 'Server shutdown notification received', {
          reconnectIn: message.reconnectIn
        });
        this.emit('server_shutdown', message);
        return;
      }

      // Emit message for external handling (marketplace events)
      this.emit('message', message);

    } catch (error) {
      logger.logGeneralError('Failed to parse WebSocket message', {
        operation: 'websocket_message_parse',
        error: error instanceof Error ? error : new Error(String(error))
      });
    }
  }

  private handlePong(timestamp: number): void {
    this.lastPongReceived = Date.now();
    this.missedPongs = 0;
    const latency = this.lastPongReceived - timestamp;
    this.recordLatency(latency);

    logger.logDebug(ErrorCategory.WEBSOCKET, 'Pong received', { latency });
  }

  private handleServerStatus(status: any): void {
    logger.logInfo(ErrorCategory.WEBSOCKET, 'Server status update', status);

    // Acknowledge receipt
    this.sendMessage({
      event: 'status_ack',
      clientId: this.config.clientId,
      timestamp: Date.now()
    });

    this.emit('server_status', status);
  }

  private handleClose(event: { code: number; reason: string }): void {
    // Reset reconnecting flag
    this.isReconnecting = false;

    // Calculate connection duration
    if (this.metrics.connectionStartTime) {
      this.metrics.connectionEndTime = Date.now();
      this.metrics.connectionDuration = this.metrics.connectionEndTime - this.metrics.connectionStartTime;
      this.metrics.totalDisconnections++;
    }

    this.metrics.connectionQuality = 'disconnected';

    logger.logWarning(ErrorCategory.WEBSOCKET, 'WebSocket disconnected', {
      code: event.code,
      reason: event.reason,
      connectionDuration: this.metrics.connectionDuration
    });

    // Clear intervals
    this.clearAllIntervals();

    // Clean up WebSocket instance
    if (this.ws) {
      this.ws.removeAllListeners();
      this.ws = null;
    }

    // Emit disconnected event
    this.emit('disconnected', event);

    // Always attempt reconnection unless explicitly disconnected (code 1000)
    if (event.code !== 1000) {
      this.attemptReconnect();
    } else {
      logger.logInfo(ErrorCategory.WEBSOCKET, 'Normal disconnection, not reconnecting');
    }
  }

  private handleError(error: Error): void {
    this.metrics.totalErrors++;
    this.metrics.lastError = error;

    logger.logWebSocketError('WebSocket error', error);
    this.emit('error', error);

    // If we get ECONNREFUSED or similar errors, the connection is dead
    if (error.message?.includes('ECONNREFUSED') || 
        error.message?.includes('ETIMEDOUT') ||
        error.message?.includes('ENETUNREACH')) {
      logger.logWarning(ErrorCategory.WEBSOCKET, 'Connection error detected, will reconnect on close event');
      // The close event will handle reconnection
    }
  }

  private attemptReconnect(): void {
    // Reset reconnecting flag to allow new connection attempts
    this.isReconnecting = false;

    // Check if we should stop retrying (0 means infinite retries)
    if (this.config.maxRetries > 0 && this.retryCount >= this.config.maxRetries) {
      logger.logWebSocketError('Max reconnection attempts exceeded', new Error('Max retries reached'));
      this.emit('max_retries_exceeded');
      return;
    }

    // Simple delay in multiples of 2 seconds, cap at 60 seconds
    const delay = Math.min((this.retryCount + 1) * 2000, 60000); // 2s, 4s, 6s... max 60s

    logger.logInfo(ErrorCategory.WEBSOCKET, 'Scheduling reconnection', {
      attempt: this.retryCount + 1,
      maxRetries: this.config.maxRetries || 'unlimited',
      delayMs: delay
    });

    this.retryCount++;

    setTimeout(() => {
      logger.logInfo(ErrorCategory.WEBSOCKET, 'Executing scheduled reconnection');
      this.connect().catch(error => {
        logger.logWebSocketError('Reconnection failed', error as Error);
        // Connection will retry through its own error handling
      });
    }, delay);
  }

  sendMessage(message: any): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      try {
        this.ws.send(JSON.stringify(message));
      } catch (error) {
        logger.logWebSocketError('Failed to send message', error as Error);
        this.queueMessage(message);
      }
    } else {
      this.queueMessage(message);
    }
  }

  private sendPing(): void {
    this.sendMessage({
      event: 'ping',
      clientId: this.config.clientId
    });
  }

  async sendMessageWithResponse(message: any, timeoutMs = 5000): Promise<any> {
    return new Promise((resolve, reject) => {
      const messageId = message.id || `msg_${Date.now()}`;
      message.id = messageId;

      const timeout = setTimeout(() => {
        this.pendingMessages.delete(messageId);
        reject(new Error(`Message timeout: ${messageId}`));
      }, timeoutMs);

      this.pendingMessages.set(messageId, { resolve, reject, timeout });
      this.sendMessage(message);
    });
  }

  private queueMessage(message: any): void {
    if (this.messageQueue.length >= this.config.messageQueueLimit) {
      this.messageQueue.shift(); // Remove oldest message
    }
    this.messageQueue.push(message);
  }

  private async processMessageQueue(): Promise<void> {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return;
    }

    while (this.messageQueue.length > 0) {
      const message = this.messageQueue.shift();
      try {
        this.ws.send(JSON.stringify(message));
        await new Promise(resolve => setTimeout(resolve, 100)); // Rate limit
      } catch {
        // Put back in queue
        this.messageQueue.unshift(message);
        break;
      }
    }
  }

  async subscribe(slug: string, contractAddress: string, marketplace: string): Promise<void> {
    const subscriptionId = `${slug}:${marketplace}`;

    // Check if already subscribed
    if (this.activeSubscriptions.has(subscriptionId)) {
      logger.logInfo(ErrorCategory.WEBSOCKET, 'Already subscribed', { subscriptionId });
      return;
    }

    // Server expects this exact format - no marketplace or topic fields
    const message = {
      event: 'join_the_party',
      clientId: this.config.clientId,
      slug: slug,
      contractAddress: contractAddress
    };

    try {
      // Send message without waiting for response
      this.sendMessage(message);

      // Store subscription locally
      this.activeSubscriptions.set(subscriptionId, {
        slug,
        contractAddress,
        marketplace,
        subscribedAt: Date.now()
      });

      this.metrics.subscriptionCount = this.activeSubscriptions.size;

      logger.logInfo(ErrorCategory.WEBSOCKET, 'Subscribed to collection', {
        slug,
        marketplace,
        subscriptionId
      });

      this.emit('subscribed', { slug, marketplace });

    } catch (error) {
      logger.logWebSocketError('Failed to subscribe', error as Error);
      throw error;
    }
  }

  async unsubscribe(slug: string, contractAddress: string, marketplace?: string): Promise<void> {
    // Server expects this exact format
    const message = {
      event: 'leave_the_party',
      clientId: this.config.clientId,
      slug: slug,
      contractAddress: contractAddress
    };

    try {
      // Send message without waiting for response
      this.sendMessage(message);

      // Update local state
      if (marketplace) {
        this.activeSubscriptions.delete(`${slug}:${marketplace}`);
      } else {
        // Remove all marketplaces for this collection
        ['OPENSEA', 'MAGICEDEN', 'BLUR'].forEach(m =>
          this.activeSubscriptions.delete(`${slug}:${m}`)
        );
      }

      this.metrics.subscriptionCount = this.activeSubscriptions.size;

      logger.logInfo(ErrorCategory.WEBSOCKET, 'Unsubscribed from collection', {
        slug,
        marketplace: marketplace || 'all'
      });

      this.emit('unsubscribed', { slug, marketplace });

    } catch (error) {
      logger.logWebSocketError('Failed to unsubscribe', error as Error);
      throw error;
    }
  }

  // Request current subscriptions from server
  requestSubscriptions(): void {
    this.sendMessage({
      event: 'get_subscriptions',
      clientId: this.config.clientId
    });
  }

  // Interval management
  private setInterval(name: string, callback: () => void, delay: number): NodeJS.Timeout {
    this.clearInterval(name);
    const interval = setInterval(callback, delay) as NodeJS.Timeout;
    this.intervals.set(name, interval);
    return interval;
  }

  private clearInterval(name: string): void {
    const interval = this.intervals.get(name);
    if (interval) {
      clearInterval(interval);
      this.intervals.delete(name);
    }
  }

  private clearAllIntervals(): void {
    this.intervals.forEach(interval => clearInterval(interval));
    this.intervals.clear();
  }

  // Metrics helpers
  private recordLatency(latency: number): void {
    this.latencies.push(latency);
    if (this.latencies.length > 100) {
      this.latencies.shift();
    }

    // Update average latency
    this.metrics.averageLatency = this.latencies.reduce((a, b) => a + b, 0) / this.latencies.length || 0;

    // Update connection quality
    this.updateConnectionQuality();
  }

  private recordMessage(): void {
    this.messageTimestamps.push(Date.now());

    // Keep only last 5 minutes
    const fiveMinutesAgo = Date.now() - 300000;
    this.messageTimestamps = this.messageTimestamps.filter(t => t > fiveMinutesAgo);

    // Calculate message rate (per minute)
    this.metrics.messageRate = this.messageTimestamps.length / 5;
  }

  private updateConnectionQuality(): void {
    if (this.ws?.readyState !== WebSocket.OPEN) {
      this.metrics.connectionQuality = 'disconnected';
    } else if (this.metrics.averageLatency < 50 && this.metrics.messageRate > 1) {
      this.metrics.connectionQuality = 'excellent';
    } else if (this.metrics.averageLatency < 200) {
      this.metrics.connectionQuality = 'good';
    } else {
      this.metrics.connectionQuality = 'poor';
    }
  }

  getMetrics(): ConnectionMetrics {
    return { ...this.metrics };
  }

  getActiveSubscriptions(): Map<string, SubscriptionInfo> {
    return new Map(this.activeSubscriptions);
  }

  isConnected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN;
  }

  disconnect(): void {
    // Prevent reconnection
    this.retryCount = this.config.maxRetries > 0 ? this.config.maxRetries : Number.MAX_SAFE_INTEGER;
    this.isReconnecting = false;

    this.clearAllIntervals();
    if (this.ws) {
      this.ws.removeAllListeners();
      this.ws.close(1000, 'Client disconnect'); // Normal closure
      this.ws = null;
    }
  }
}

export default WebSocketManager;