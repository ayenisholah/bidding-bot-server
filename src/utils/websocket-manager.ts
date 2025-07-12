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
  subscriptionType?: 'bidding' | 'listing'; // Track subscription type
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
  private lastMessageReceived = 0;
  private connectionStaleThreshold = 180000; // 3 minutes without any messages - more aggressive than server's 5 min threshold

  // State persistence for listings (per CLIENT_CONNECTION_GUIDE.md)
  private persistentListingSubscriptions: Map<string, SubscriptionInfo> = new Map();
  private subscriptionStateFile = '.listing-subscriptions.json';

  constructor(config: WebSocketManagerConfig) {
    super();
    this.config = {
      maxRetries: 0, // 0 means infinite retries
      reconnectBaseDelay: 1000,
      reconnectMaxDelay: 60000,
      heartbeatInterval: 25000, // Send ping every 25 seconds (before 30s timeout)
      connectionTimeout: 10000,
      messageQueueLimit: 1000,
      ...config
    };

    logger.logInfo(ErrorCategory.WEBSOCKET, 'WebSocket Manager initialized', {
      clientId: this.config.clientId,
      url: this.config.url,
      heartbeatInterval: this.config.heartbeatInterval
    });

    // Load persistent listing subscriptions on initialization
    this.loadPersistentSubscriptions();
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
          // Only close if the WebSocket is not already closing/closed
          if (this.ws.readyState === WebSocket.CONNECTING || this.ws.readyState === WebSocket.OPEN) {
            try {
              this.ws.close();
            } catch (error) {
              // Ignore close errors during connection phase
              logger.logDebug(ErrorCategory.WEBSOCKET, 'Error closing WebSocket during timeout', {
                error: error instanceof Error ? error.message : String(error)
              });
            }
          }
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

      // Handle any errors during connection phase
      this.ws.once('error', (error) => {
        clearTimeout(connectionTimeout);
        logger.logWebSocketError('WebSocket connection error', error);
        // The error handler and close handler will take care of reconnection
      });

    } catch (error) {
      logger.logWebSocketError('Connection attempt failed', error as Error);
      this.isReconnecting = false;
      this.attemptReconnect();
    }
  }

  private onConnectionEstablished(): void {
    // Initialize connection metrics
    const connectionId = `conn_${Date.now()}_${Math.random().toString(36).substring(2, 11)}`;
    this.metrics.connectionStartTime = Date.now();
    this.metrics.currentConnectionId = connectionId;
    this.metrics.totalConnections++;
    this.metrics.connectionQuality = 'good';

    // Initialize message tracking
    this.lastMessageReceived = Date.now();
    this.missedPongs = 0;

    logger.logInfo(ErrorCategory.WEBSOCKET, 'WebSocket connected', {
      connectionId,
      url: this.config.url
    });

    // Send initial ping immediately after connection (per CLIENT_PROTOCOL.md)
    this.sendPing();

    // Start heartbeat
    this.startHeartbeat();

    // Start health monitoring
    this.startHealthMonitoring();

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
    this.clearTimeout('pongTimeout');

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

      // Check for stale connection (no messages received for extended period)
      // Only check if we have active subscriptions that should be sending messages
      if (this.lastMessageReceived > 0 &&
        (this.activeSubscriptions.size > 0 || this.persistentListingSubscriptions.size > 0) &&
        (now - this.lastMessageReceived) > this.connectionStaleThreshold) {
        logger.logWarning(ErrorCategory.WEBSOCKET, 'Connection appears stale (no messages received), reconnecting...', {
          lastMessageReceived: this.lastMessageReceived,
          staleDuration: now - this.lastMessageReceived,
          threshold: this.connectionStaleThreshold,
          activeSubscriptions: this.activeSubscriptions.size,
          persistentListingSubscriptions: this.persistentListingSubscriptions.size
        });
        // Force clean disconnection to ensure server cleans up state
        this.ws?.close(1001, 'Stale connection detected');
        return;
      }

      // Send ping - server only needs event and clientId
      this.lastPingSent = now;

      this.sendMessage({
        event: 'ping',
        clientId: this.config.clientId
      });

      // Set pong timeout (per CLIENT_CONNECTION_GUIDE.md recommendation)
      this.clearTimeout('pongTimeout');
      this.setTimeout('pongTimeout', () => {
        this.missedPongs++;
        logger.logWarning(ErrorCategory.WEBSOCKET, `Pong timeout - missed ${this.missedPongs}/3 pongs`);

        if (this.missedPongs >= 3) {
          logger.logWebSocketError('Max missed pongs reached, forcing reconnection');
          this.ws?.close();
        }
      }, 5000); // 5 second pong timeout

    }, this.config.heartbeatInterval);
  }

  private startHealthMonitoring(): void {
    this.clearInterval('healthMonitor');

    this.setInterval('healthMonitor', () => {
      if (!this.isConnected()) {
        logger.logWarning(ErrorCategory.WEBSOCKET, 'Health monitor detected disconnection');
        return;
      }

      // Check for various health indicators
      const now = Date.now();
      const timeSinceLastMessage = this.lastMessageReceived ? now - this.lastMessageReceived : 0;
      const timeSinceLastPong = this.lastPongReceived ? now - this.lastPongReceived : 0;

      // Log health status periodically
      if (timeSinceLastMessage > 60000 || timeSinceLastPong > 60000) { // 1 minute
        logger.logInfo(ErrorCategory.WEBSOCKET, 'WebSocket health status', {
          connectionQuality: this.metrics.connectionQuality,
          timeSinceLastMessage,
          timeSinceLastPong,
          missedPongs: this.missedPongs,
          activeSubscriptions: this.activeSubscriptions.size,
          persistentListingSubscriptions: this.persistentListingSubscriptions.size,
          messageRate: this.metrics.messageRate,
          averageLatency: this.metrics.averageLatency,
          lastMessageReceived: new Date(this.lastMessageReceived).toISOString()
        });

        // Log subscription details for debugging
        if (this.persistentListingSubscriptions.size > 0) {
          const listingSubs = Array.from(this.persistentListingSubscriptions.entries())
            .map(([, info]) => `${info.slug}:${info.marketplace}`);
          logger.logInfo(ErrorCategory.WEBSOCKET, 'Active listing subscriptions', {
            subscriptions: listingSubs
          });
        }
      }

      // Force reconnection if connection appears severely degraded
      if (timeSinceLastMessage > 600000) { // 10 minutes
        logger.logWarning(ErrorCategory.WEBSOCKET, 'Forcing reconnection due to severely stale connection', {
          timeSinceLastMessage,
          threshold: 600000
        });
        this.ws?.close();
      }

      // Periodic subscription verification (every 5 minutes)
      if (this.activeSubscriptions.size > 0 && timeSinceLastMessage > 300000) { // 5 minutes
        logger.logInfo(ErrorCategory.WEBSOCKET, 'Performing periodic subscription verification');
        this.verifySubscriptions().catch(error => {
          logger.logWarning(ErrorCategory.WEBSOCKET, 'Periodic subscription verification failed', {
            error: error instanceof Error ? error : new Error('Unknown error')
          });
        });
      }
      
      // Add specific listing subscription health check every 2 minutes
      if (this.persistentListingSubscriptions.size > 0 && timeSinceLastMessage > 120000) { // 2 minutes
        this.verifyListingSubscriptions().catch(error => {
          logger.logWarning(ErrorCategory.WEBSOCKET, 'Listing subscription verification failed', {
            error: error instanceof Error ? error : new Error('Unknown error')
          });
        });
      }

    }, 60000); // Check every minute
  }

  private async syncSubscriptions(): Promise<void> {
    try {
      // First, request current subscriptions from server to verify state
      this.requestSubscriptions();
      
      // Wait a bit for server response
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Clear active subscriptions to ensure clean state
      this.activeSubscriptions.clear();

      // Restore persistent listing subscriptions to active subscriptions
      for (const [key, value] of this.persistentListingSubscriptions) {
        this.activeSubscriptions.set(key, value);
      }

      // Skip sync if no subscriptions
      if (this.activeSubscriptions.size === 0 && this.persistentListingSubscriptions.size === 0) {
        logger.logInfo(ErrorCategory.WEBSOCKET, 'No subscriptions to sync');
        return;
      }

      logger.logInfo(ErrorCategory.WEBSOCKET, 'Syncing subscriptions after reconnection', {
        activeSubscriptions: this.activeSubscriptions.size,
        persistentListings: this.persistentListingSubscriptions.size
      });

      // Simply resubscribe to all collections
      // Server subscribes to ALL marketplaces when you subscribe once
      const subscribedSlugs = new Set<string>();
      const subscribedListings = new Set<string>();
      const failedSubscriptions: string[] = [];

      // Use activeSubscriptions which now includes persistent listings
      for (const [subscriptionId, subInfo] of this.activeSubscriptions) {
        const subscriptionType = subInfo.subscriptionType || 'bidding';
        const isListing = subscriptionType === 'listing';
        const trackingSet = isListing ? subscribedListings : subscribedSlugs;

        // Only subscribe once per collection per type
        if (!trackingSet.has(subInfo.slug)) {
          try {
            const eventName = isListing ? 'join_listings' : 'join_the_party';
            const message = {
              event: eventName,
              clientId: this.config.clientId,
              slug: subInfo.slug,
              contractAddress: subInfo.contractAddress
            };

            logger.logDebug(ErrorCategory.WEBSOCKET, 'Resubscribing to collection', {
              subscriptionId,
              slug: subInfo.slug,
              eventName,
              subscriptionType,
              clientId: this.config.clientId
            });

            this.sendMessage(message);
            trackingSet.add(subInfo.slug);

            // Small delay between subscriptions
            await new Promise(resolve => setTimeout(resolve, 100));
          } catch (error) {
            failedSubscriptions.push(subInfo.slug);
            logger.logWarning(ErrorCategory.WEBSOCKET, 'Failed to resubscribe to collection', {
              slug: subInfo.slug,
              subscriptionType,
              error: error instanceof Error ? error : new Error('Unknown error')
            });
          }
        }
      }

      logger.logInfo(ErrorCategory.WEBSOCKET, 'Subscription sync complete', {
        biddingCollections: subscribedSlugs.size,
        listingCollections: subscribedListings.size,
        failed: failedSubscriptions.length,
        failedCollections: failedSubscriptions,
        totalActiveAfterSync: this.activeSubscriptions.size
      });

      // Update metrics
      this.metrics.subscriptionCount = this.activeSubscriptions.size;

      // Retry failed subscriptions after a delay
      if (failedSubscriptions.length > 0) {
        setTimeout(() => {
          this.retryFailedSubscriptions(failedSubscriptions);
        }, 5000);
      }

    } catch (error) {
      logger.logWebSocketError('Failed to sync subscriptions', error instanceof Error ? error : new Error(String(error)));
      // Don't let sync failure kill the connection
      logger.logWarning(ErrorCategory.WEBSOCKET, 'Continuing without subscription sync', {
        error: error instanceof Error ? error : new Error('Unknown error')
      });
    }
  }

  private async retryFailedSubscriptions(failedSlugs: string[]): Promise<void> {
    if (!this.isConnected()) {
      logger.logWarning(ErrorCategory.WEBSOCKET, 'Cannot retry subscriptions - not connected');
      return;
    }

    logger.logInfo(ErrorCategory.WEBSOCKET, 'Retrying failed subscriptions', {
      count: failedSlugs.length
    });

    for (const slug of failedSlugs) {
      try {
        // Find the subscription info from both active and persistent subscriptions
        let subInfo = Array.from(this.activeSubscriptions.values()).find(info => info.slug === slug);
        if (!subInfo) {
          subInfo = Array.from(this.persistentListingSubscriptions.values()).find(info => info.slug === slug);
        }

        if (subInfo) {
          const subscriptionType = subInfo.subscriptionType || 'bidding';
          const eventName = subscriptionType === 'listing' ? 'join_listings' : 'join_the_party';
          const message = {
            event: eventName,
            clientId: this.config.clientId,
            slug: subInfo.slug,
            contractAddress: subInfo.contractAddress
          };

          logger.logDebug(ErrorCategory.WEBSOCKET, 'Retrying subscription', {
            slug,
            eventName,
            subscriptionType
          });

          this.sendMessage(message);
          await new Promise(resolve => setTimeout(resolve, 200));
        } else {
          logger.logWarning(ErrorCategory.WEBSOCKET, 'Failed to find subscription info for retry', {
            slug
          });
        }
      } catch (error) {
        logger.logWarning(ErrorCategory.WEBSOCKET, 'Retry subscription failed', {
          slug,
          error: error instanceof Error ? error : new Error('Unknown error')
        });
      }
    }
  }

  private handleMessage(data: WebSocket.Data): void {
    try {
      const message = JSON.parse(data.toString());
      this.metrics.totalMessages++;
      this.recordMessage();

      // Update last message received timestamp
      this.lastMessageReceived = Date.now();

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
      this.emit('message', message);

    } catch (error) {
      logger.logGeneralError('Failed to parse WebSocket message', {
        operation: 'websocket_message_parse',
        error: error instanceof Error ? error : new Error(String(error))
      });

      // Still emit the raw message even if parsing fails
      // This helps prevent message loss
      try {
        this.emit('message', { raw: data.toString(), parseError: true });
      } catch (emitError) {
        console.log(emitError);
        logger.logWebSocketError('Failed to emit raw message after parse error');
      }
    }
  }

  private handlePong(timestamp: number): void {
    this.lastPongReceived = Date.now();
    this.missedPongs = 0;

    const maxFutureTolerance = 5000; // 5 seconds ahead max

    // Validate timestamp with future tolerance
    if (
      typeof timestamp !== 'number' ||
      timestamp <= 0 ||
      timestamp > this.lastPongReceived + maxFutureTolerance
    ) {
      logger.logWarning(ErrorCategory.WEBSOCKET, 'Invalid pong timestamp received', {
        timestamp,
        now: this.lastPongReceived
      });
      return;
    }

    const latency = this.lastPongReceived - timestamp;
    this.recordLatency(latency);

    // Clear pong timeout when we receive a pong
    this.clearTimeout('pongTimeout');

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

    // Clear intervals and timeouts
    this.clearAllIntervals();
    this.clearAllTimeouts();

    // Reset message tracking
    this.lastMessageReceived = 0;
    this.lastPingSent = 0;
    this.lastPongReceived = 0;
    this.missedPongs = 0;

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

    // Exponential backoff with jitter (per CLIENT_CONNECTION_GUIDE.md)
    const baseDelay = this.config.reconnectBaseDelay || 1000;
    const maxDelay = this.config.reconnectMaxDelay || 60000;
    const jitterRange = 0.3; // 30% jitter

    // Calculate exponential delay
    const exponentialDelay = Math.min(
      baseDelay * Math.pow(2, this.retryCount),
      maxDelay
    );

    // Add jitter to prevent thundering herd
    const jitter = exponentialDelay * jitterRange * (Math.random() - 0.5);
    const delay = Math.round(exponentialDelay + jitter);

    logger.logInfo(ErrorCategory.WEBSOCKET, 'Scheduling reconnection', {
      attempt: this.retryCount + 1,
      maxRetries: this.config.maxRetries || 'unlimited',
      delayMs: delay,
      baseDelay: exponentialDelay,
      jitter: Math.round(jitter)
    });

    this.retryCount++;

    this.setTimeout('reconnect', () => {
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
    this.lastPingSent = Date.now();

    const pingMessage = {
      event: 'ping',
      clientId: this.config.clientId,
      timestamp: this.lastPingSent // ✅ added timestamp
    };

    this.sendMessage(pingMessage);

    logger.logDebug(ErrorCategory.WEBSOCKET, 'Ping sent', {
      clientId: this.config.clientId,
      timestamp: this.lastPingSent
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

  async subscribe(slug: string, contractAddress: string, marketplace: string, subscriptionType: 'bidding' | 'listing' = 'bidding'): Promise<void> {
    const subscriptionId = `${slug}:${marketplace}:${subscriptionType}`;

    // Check if already subscribed
    if (this.activeSubscriptions.has(subscriptionId)) {
      logger.logInfo(ErrorCategory.WEBSOCKET, 'Already subscribed', {
        subscriptionId,
        totalActive: this.activeSubscriptions.size,
        totalPersistent: this.persistentListingSubscriptions.size
      });
      // Force resubscribe if we haven't received messages recently
      const now = Date.now();
      if (this.lastMessageReceived > 0 && (now - this.lastMessageReceived) > 60000) {
        logger.logWarning(ErrorCategory.WEBSOCKET, 'Forcing resubscription due to no recent messages', {
          subscriptionId,
          timeSinceLastMessage: now - this.lastMessageReceived
        });
        // Continue with subscription
      } else {
        return;
      }
    }

    // Choose appropriate event based on subscription type
    const eventName = subscriptionType === 'listing' ? 'join_listings' : 'join_the_party';
    const message = {
      event: eventName,
      clientId: this.config.clientId,
      slug: slug,
      contractAddress: contractAddress
    };

    try {
      // Send message without waiting for response
      this.sendMessage(message);

      // Store subscription locally
      const subscriptionInfo: SubscriptionInfo = {
        slug,
        contractAddress,
        marketplace,
        subscribedAt: Date.now(),
        subscriptionType
      };

      this.activeSubscriptions.set(subscriptionId, subscriptionInfo);

      // Persist listing subscriptions (per CLIENT_CONNECTION_GUIDE.md)
      if (subscriptionType === 'listing') {
        this.persistentListingSubscriptions.set(subscriptionId, subscriptionInfo);
        this.savePersistentSubscriptions();
      }

      this.metrics.subscriptionCount = this.activeSubscriptions.size;

      logger.logInfo(ErrorCategory.WEBSOCKET, 'Subscribed to collection', {
        slug,
        marketplace,
        subscriptionId,
        subscriptionType,
        persisted: subscriptionType === 'listing',
        totalActive: this.activeSubscriptions.size,
        totalPersistent: this.persistentListingSubscriptions.size,
        eventName,
        contractAddress
      });

      this.emit('subscribed', { slug, marketplace, subscriptionType });

    } catch (error) {
      logger.logWebSocketError('Failed to subscribe', error as Error);
      throw error;
    }
  }

  async unsubscribe(slug: string, contractAddress: string, marketplace?: string): Promise<void> {
    // Handle both bidding and listing unsubscriptions
    const unsubscribeBidding = {
      event: 'leave_the_party',
      clientId: this.config.clientId,
      slug: slug,
      contractAddress: contractAddress
    };
    
    const unsubscribeListing = {
      event: 'leave_listings',
      clientId: this.config.clientId,
      slug: slug,
      contractAddress: contractAddress
    };

    try {
      // Send both unsubscribe messages to ensure clean state
      this.sendMessage(unsubscribeBidding);
      this.sendMessage(unsubscribeListing);

      // Update local state
      if (marketplace) {
        // Remove specific marketplace subscription
        const biddingKey = `${slug}:${marketplace}:bidding`;
        const listingKey = `${slug}:${marketplace}:listing`;

        this.activeSubscriptions.delete(biddingKey);
        this.activeSubscriptions.delete(listingKey);

        // Remove from persistent storage if it's a listing
        if (this.persistentListingSubscriptions.has(listingKey)) {
          this.persistentListingSubscriptions.delete(listingKey);
          this.savePersistentSubscriptions();
        }
      } else {
        // Remove all marketplaces for this collection
        ['OPENSEA', 'MAGICEDEN', 'BLUR'].forEach(m => {
          const biddingKey = `${slug}:${m}:bidding`;
          const listingKey = `${slug}:${m}:listing`;

          this.activeSubscriptions.delete(biddingKey);
          this.activeSubscriptions.delete(listingKey);

          // Remove from persistent storage
          if (this.persistentListingSubscriptions.has(listingKey)) {
            this.persistentListingSubscriptions.delete(listingKey);
          }
        });
        this.savePersistentSubscriptions();
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

  // Add subscription verification method
  async verifySubscriptions(): Promise<void> {
    if (!this.isConnected()) {
      logger.logWarning(ErrorCategory.WEBSOCKET, 'Cannot verify subscriptions - not connected');
      return;
    }

    logger.logInfo(ErrorCategory.WEBSOCKET, 'Verifying subscriptions', {
      localSubscriptions: this.activeSubscriptions.size
    });

    // Request server to confirm our subscriptions
    this.requestSubscriptions();

    // Wait a bit and then check if we're receiving messages
    setTimeout(() => {
      if (this.activeSubscriptions.size > 0) {
        const timeSinceLastMessage = this.lastMessageReceived ? Date.now() - this.lastMessageReceived : 0;
        if (timeSinceLastMessage > 120000) { // 2 minutes
          logger.logWarning(ErrorCategory.WEBSOCKET, 'Subscription verification failed - no recent messages', {
            timeSinceLastMessage,
            activeSubscriptions: this.activeSubscriptions.size
          });
          // Force resubscription
          this.syncSubscriptions().catch(error => {
            logger.logWarning(ErrorCategory.WEBSOCKET, 'Forced resubscription failed', {
              error: error instanceof Error ? error : new Error('Unknown error')
            });
          });
        }
      }
    }, 30000); // Check after 30 seconds
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

  // Timeout management (per CLIENT_CONNECTION_GUIDE.md)
  private timeouts: Map<string, NodeJS.Timeout> = new Map();

  private setTimeout(name: string, callback: () => void, delay: number): NodeJS.Timeout {
    this.clearTimeout(name);
    const timeout = setTimeout(callback, delay) as NodeJS.Timeout;
    this.timeouts.set(name, timeout);
    return timeout;
  }

  private clearTimeout(name: string): void {
    const timeout = this.timeouts.get(name);
    if (timeout) {
      clearTimeout(timeout);
      this.timeouts.delete(name);
    }
  }

  private clearAllTimeouts(): void {
    this.timeouts.forEach(timeout => clearTimeout(timeout));
    this.timeouts.clear();
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
    this.clearAllTimeouts();

    if (this.ws) {
      this.ws.removeAllListeners();
      this.ws.close(1000, 'Client disconnect'); // Normal closure
      this.ws = null;
    }
  }

  // Persistent subscription management methods (per CLIENT_CONNECTION_GUIDE.md)
  private loadPersistentSubscriptions(): void {
    try {
      const fs = require('fs');
      if (fs.existsSync(this.subscriptionStateFile)) {
        const data = fs.readFileSync(this.subscriptionStateFile, 'utf8');
        const subscriptions = JSON.parse(data);
        this.persistentListingSubscriptions = new Map(subscriptions);
        logger.logInfo(ErrorCategory.WEBSOCKET, 'Loaded persistent listing subscriptions', {
          count: this.persistentListingSubscriptions.size
        });
      }
    } catch (error) {
      logger.logWarning(ErrorCategory.WEBSOCKET, 'Failed to load persistent subscriptions', {
        error: error instanceof Error ? error : new Error('Unknown error')
      });
    }
  }

  private savePersistentSubscriptions(): void {
    try {
      const fs = require('fs');
      const subscriptions = Array.from(this.persistentListingSubscriptions.entries());
      fs.writeFileSync(this.subscriptionStateFile, JSON.stringify(subscriptions, null, 2));
    } catch (error) {
      logger.logWarning(ErrorCategory.WEBSOCKET, 'Failed to save persistent subscriptions', {
        error: error instanceof Error ? error : new Error('Unknown error')
      });
    }
  }

  /**
   * Verify and log listing subscriptions for debugging
   * This helps identify if listing subscriptions are properly maintained
   */
  async verifyListingSubscriptions(): Promise<void> {
    const listingSubs = Array.from(this.activeSubscriptions.entries())
      .filter(([, info]) => info.subscriptionType === 'listing');
    
    const persistentListingSubs = Array.from(this.persistentListingSubscriptions.entries());
    
    logger.logInfo(ErrorCategory.WEBSOCKET, 'Listing subscription verification', {
      activeListingSubscriptions: listingSubs.length,
      persistentListingSubscriptions: persistentListingSubs.length,
      subscriptions: listingSubs.map(([key, info]) => ({
        key,
        slug: info.slug,
        marketplace: info.marketplace,
        subscribedAt: new Date(info.subscribedAt).toISOString()
      })),
      connectionState: this.isConnected() ? 'connected' : 'disconnected',
      lastMessageReceived: this.lastMessageReceived ? new Date(this.lastMessageReceived).toISOString() : 'never',
      messagesSinceConnect: this.metrics.totalMessages
    });
    
    // Check if we're receiving messages for these subscriptions
    if (listingSubs.length > 0 && this.lastMessageReceived) {
      const timeSinceLastMessage = Date.now() - this.lastMessageReceived;
      if (timeSinceLastMessage > 120000) { // 2 minutes
        logger.logWarning(ErrorCategory.WEBSOCKET, 'Listing subscriptions may be stale', {
          timeSinceLastMessage,
          activeListingSubscriptions: listingSubs.length
        });
      }
    }
  }

  /**
   * Get listing subscription statistics
   */
  getListingSubscriptionStats(): { active: number; persistent: number; slugs: string[] } {
    const listingSubs = Array.from(this.activeSubscriptions.entries())
      .filter(([, info]) => info.subscriptionType === 'listing');
    
    const slugs = Array.from(new Set(listingSubs.map(([, info]) => info.slug)));
    
    return {
      active: listingSubs.length,
      persistent: this.persistentListingSubscriptions.size,
      slugs
    };
  }

  /**
   * Force unsubscribe from all subscriptions before reconnecting
   * This ensures clean state on both client and server
   */
  async forceUnsubscribeAll(): Promise<void> {
    logger.logInfo(ErrorCategory.WEBSOCKET, 'Force unsubscribing from all collections', {
      activeSubscriptions: this.activeSubscriptions.size
    });

    const uniqueSlugs = new Set<string>();
    for (const [, info] of this.activeSubscriptions) {
      uniqueSlugs.add(info.slug);
    }

    // Send unsubscribe for each unique slug
    for (const slug of uniqueSlugs) {
      const sub = Array.from(this.activeSubscriptions.values()).find(s => s.slug === slug);
      if (sub) {
        // Unsubscribe from both bidding and listing events
        this.sendMessage({
          event: 'leave_the_party',
          clientId: this.config.clientId,
          slug: sub.slug,
          contractAddress: sub.contractAddress
        });
        
        this.sendMessage({
          event: 'leave_listings',
          clientId: this.config.clientId,
          slug: sub.slug,
          contractAddress: sub.contractAddress
        });
      }
    }

    // Clear local state
    this.activeSubscriptions.clear();
  }
}

export default WebSocketManager;