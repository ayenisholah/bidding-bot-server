import WebSocket from 'ws';
import { EventEmitter } from 'events';
import { warnLog, errorLog, debugLog, infoLog } from './debug-file-logger';

const DEBUG = process.env.DEBUG === 'true';

/**
 * Circular buffer for timestamp tracking
 * Avoids array.filter() on every message which creates GC pressure
 */
class TimestampCircularBuffer {
  private buffer: number[];
  private head = 0;
  private size = 0;
  private readonly capacity: number;
  private readonly windowMs: number;

  constructor(capacity: number, windowMs: number) {
    this.capacity = capacity;
    this.windowMs = windowMs;
    this.buffer = new Array(capacity).fill(0);
  }

  push(timestamp: number): void {
    this.buffer[this.head] = timestamp;
    this.head = (this.head + 1) % this.capacity;
    if (this.size < this.capacity) {
      this.size++;
    }
  }

  /**
   * Count timestamps within the time window
   * O(size) but avoids creating new arrays
   */
  countWithinWindow(now: number = Date.now()): number {
    const threshold = now - this.windowMs;
    let count = 0;
    for (let i = 0; i < this.size; i++) {
      const idx = (this.head - 1 - i + this.capacity) % this.capacity;
      if (this.buffer[idx] > threshold) {
        count++;
      }
    }
    return count;
  }

  /**
   * Get message rate per minute within window
   */
  getMessageRate(): number {
    const windowMinutes = this.windowMs / 60000;
    return this.countWithinWindow() / windowMinutes;
  }
}

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
  // Circular buffer for message timestamps (5 minute window, max 10000 messages)
  private messageTimestamps = new TimestampCircularBuffer(10000, 300000);
  private lastMessageReceived = 0;
  private connectionStaleThreshold = 3600000; // 60 minutes without any messages - increased from 10min for low-activity collections

  // Application-level watchdog: tracks bid events independently of ping/pong
  private lastBidEventReceived = 0;
  private bidActivityWatchdogInterval = 5 * 60 * 1000; // Check every 5 minutes
  private maxSilenceForActiveTasks = 10 * 60 * 1000; // 10 minutes without bid events = suspicious

  // Server status health check tracking
  private lastServerStatusReceived = 0;
  private serverStatusTimeout = 90000; // 90 seconds (server sends every 30s, allow 3x grace)

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


    // Load persistent listing subscriptions on initialization
    this.loadPersistentSubscriptions();
  }

  async connect(): Promise<void> {
    // Check if already connected
    if (this.ws?.readyState === WebSocket.OPEN) return;
    // Check if already attempting to connect
    if (this.isReconnecting) return;

    this.isReconnecting = true;

    try {
      // Clean up existing connection
      if (this.ws) {
        this.safeCloseWebSocket();
        this.ws = null;
      }

      this.ws = new WebSocket(this.config.url);

      // Connection timeout
      const connectionTimeout = setTimeout(() => {
        try {
          this.isReconnecting = false;
          if (this.ws) {
            this.ws.removeAllListeners();
            // Handle different WebSocket states appropriately
            if (this.ws.readyState === WebSocket.OPEN) {
              // Safe to close an open connection
              try {
                this.ws.close();
              } catch {
                warnLog('WebSocket', 'Error closing WebSocket connection during timeout');
              }
            } else if (this.ws.readyState === WebSocket.CONNECTING) {
              // Can't terminate a CONNECTING WebSocket - just clean up
// console.log('Connection timeout - cleaning up WebSocket in CONNECTING state');
              // Remove all listeners to prevent any callbacks
              this.ws.removeAllListeners();
              // Store reference for cleanup
              const wsToCleanup = this.ws;
              // Set to null immediately to prevent further operations
              this.ws = null;

              // Set up cleanup when the connection eventually completes or fails
              wsToCleanup.once('open', () => {
                try {
                  wsToCleanup.close();
                } catch {
                  warnLog('WebSocket', 'Error closing WebSocket during cleanup after connection timeout');
                }
              });
              wsToCleanup.once('error', () => {
                // Connection failed, nothing to do
              });
            }
            // If CLOSING or CLOSED, no action needed
          }
          this.attemptReconnect();
        } catch (error) {
          errorLog('WebSocket', 'Error during connection timeout handling', error);
          try {
            this.attemptReconnect();
          } catch (reconnectError) {
            errorLog('WebSocket', 'Error attempting reconnect after connection timeout', reconnectError);
          }
        }
      }, this.config.connectionTimeout);

      this.ws.once('open', () => {
        clearTimeout(connectionTimeout);
        this.isReconnecting = false;
        this.retryCount = 0;
        this.onConnectionEstablished();
      });

      // Only set event handlers if WebSocket still exists
      if (this.ws) {
        this.ws.on('message', this.handleMessage.bind(this));
        this.ws.on('close', this.handleClose.bind(this));
        this.ws.on('error', this.handleError.bind(this));

        // Handle any errors during connection phase
        this.ws.once('error', () => {
// console.log('WebSocket connection error');
          clearTimeout(connectionTimeout);
          this.isReconnecting = false;
          // The error handler will trigger reconnection
        });
      }

    } catch (error) {
      errorLog('WebSocket', 'Error during WebSocket connection attempt', error);
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

    // Log connection established with current state
    infoLog('WebSocket', `[CONNECTED] WebSocket connection established | connectionId=${connectionId} | totalConnections=${this.metrics.totalConnections} | activeSubscriptions=${this.activeSubscriptions.size} | persistentListings=${this.persistentListingSubscriptions.size}`);

    // Enable TCP keepalive for faster dead connection detection
    try {
      const socket = (this.ws as any)?._socket;
      if (socket && typeof socket.setKeepAlive === 'function') {
        socket.setKeepAlive(true, 10000); // 10 second keepalive
      }
    } catch {
      // TCP keepalive not available
    }

    // Send initial ping immediately after connection (per CLIENT_PROTOCOL.md)
    this.sendPing();

    // Start heartbeat
    this.startHeartbeat();

    // Start keep-alive messages to prevent proxy timeouts
    this.startKeepAlive();

    // Start health monitoring
    this.startHealthMonitoring();

    // Start application-level bid activity watchdog
    this.startBidActivityWatchdog();

    // Process queued messages
    this.processMessageQueue();

    // Sync subscriptions (don't await to prevent blocking connection)
    this.syncSubscriptions().catch(() => {
      warnLog('WebSocket', 'Error syncing subscriptions on connection');
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
        if (this.missedPongs >= 3) { // Reduced from 6 to 3 for faster dead connection detection

          try {
            this.ws?.close();
          } catch {
            warnLog('WebSocket', 'Error closing WebSocket after missed pongs');
          }
          return;
        }
      }

      // Check for stale connection (no messages received for extended period)
      // Only check if we have active subscriptions that should be sending messages
      // Use adaptive threshold based on collection activity
      const adaptiveStaleThreshold = this.getAdaptiveStaleThreshold();
      if (this.lastMessageReceived > 0 &&
        (this.activeSubscriptions.size > 0 || this.persistentListingSubscriptions.size > 0) &&
        (now - this.lastMessageReceived) > adaptiveStaleThreshold) {
// console.log('Connection appears stale (no messages received), reconnecting...');
        // Force clean disconnection to ensure server cleans up state
        try {
          this.ws?.close(1001, 'Stale connection detected');
        } catch {
          warnLog('WebSocket', 'Error closing stale connection');
        }
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

        if (this.missedPongs >= 3) { // Reduced from 6 to 3 for faster dead connection detection
          warnLog('WebSocket', 'Max missed pongs reached, forcing reconnection');
          try {
            this.ws?.close();
          } catch {
            warnLog('WebSocket', 'Error closing WebSocket after max missed pongs reached');
          }
        }
      }, 10000); // 10 second pong timeout (reduced from 15s for faster detection)

    }, this.config.heartbeatInterval);
  }

  private startKeepAlive(): void {
    this.clearInterval('keepAlive');

    // Send keep-alive every 90 seconds to prevent proxy timeouts
    this.setInterval('keepAlive', () => {
      if (this.isConnected()) {
        this.sendMessage({
          event: 'keep-alive',
          clientId: this.config.clientId,
          timestamp: Date.now()
        });

// console.log('Sent keep-alive message');
      }
    }, 90000); // 90 seconds
  }

  private startHealthMonitoring(): void {
    this.clearInterval('healthMonitor');

    this.setInterval('healthMonitor', () => {
      if (!this.isConnected()) {
        debugLog('WebSocket', '[HealthCheck] Not connected - skipping health check');
        return;
      }

      // Check for various health indicators
      const now = Date.now();
      const timeSinceLastMessage = this.lastMessageReceived ? now - this.lastMessageReceived : 0;
      const timeSinceLastPong = this.lastPongReceived ? now - this.lastPongReceived : 0;

      // ALWAYS log subscription state (important for debugging unexpected events)
      infoLog('WebSocket', `[SUBSCRIPTION_STATE] Active subscriptions dump | count=${this.activeSubscriptions.size} | persistent=${this.persistentListingSubscriptions.size} | slugs=${this.getActiveSlugsList()} | timeSinceLastMessage=${timeSinceLastMessage}ms`);

      // Log health status periodically (every minute to debug file)
      if (DEBUG) {
        const healthStatus = {
          connected: true,
          connectionId: this.metrics.currentConnectionId,
          uptime: this.metrics.connectionStartTime ? now - this.metrics.connectionStartTime : 0,
          lastMessageAge: timeSinceLastMessage,
          lastPongAge: timeSinceLastPong,
          lastBidEventAge: this.lastBidEventReceived ? now - this.lastBidEventReceived : null,
          lastServerStatusAge: this.lastServerStatusReceived ? now - this.lastServerStatusReceived : null,
          missedPongs: this.missedPongs,
          activeSubscriptions: this.activeSubscriptions.size,
          messageRate: this.metrics.messageRate
        };
        debugLog('WebSocket', `[HealthCheck] ${JSON.stringify(healthStatus)}`);
      }

      // Force reconnection if connection appears severely degraded
      if (timeSinceLastMessage > 3600000) { // 60 minutes (increased from 10min)
// console.log('Forcing reconnection due to severely stale connection');
        try {
          this.ws?.close();
        } catch {
          warnLog('WebSocket', 'Error closing severely stale connection');
        }
      }

      // Periodic subscription verification (every 5 minutes)
      if (this.activeSubscriptions.size > 0 && timeSinceLastMessage > 300000) { // 5 minutes
// console.log('Performing periodic subscription verification');
        this.verifySubscriptions().catch(() => {
          warnLog('WebSocket', 'Periodic subscription verification failed');
        });
      }

      // Add specific listing subscription health check every 2 minutes
      if (this.persistentListingSubscriptions.size > 0 && timeSinceLastMessage > 120000) { // 2 minutes
        this.verifyListingSubscriptions().catch(() => {
          warnLog('WebSocket', 'Listing subscription verification failed');
        });
      }

      // Check if server status messages are being received
      this.checkServerStatusHealth();

    }, 60000); // Check every minute
  }

  /**
   * Application-level watchdog that monitors bid activity.
   * Unlike ping/pong which tests raw connectivity, this verifies that actual
   * business data (bid events) is flowing through the connection.
   *
   * If we have active subscriptions but no bid events for 10+ minutes,
   * the connection may be silently dead even if ping/pong still works.
   */
  private startBidActivityWatchdog(): void {
    this.clearInterval('bidWatchdog');

    this.setInterval('bidWatchdog', () => {
      if (!this.isConnected()) return;

      const now = Date.now();
      const timeSinceLastBidEvent = this.lastBidEventReceived > 0
        ? now - this.lastBidEventReceived
        : 0;

      // Count active bidding subscriptions (not listing subscriptions)
      const activeBiddingSubscriptions = Array.from(this.activeSubscriptions.values())
        .filter(sub => sub.subscriptionType !== 'listing').length;

      // Only trigger if we have active bidding subscriptions AND no bid activity
      if (activeBiddingSubscriptions > 0 &&
          this.lastBidEventReceived > 0 &&
          timeSinceLastBidEvent > this.maxSilenceForActiveTasks) {

        warnLog('WebSocket', `[Watchdog] No bid events for ${Math.floor(timeSinceLastBidEvent / 60000)}+ minutes with ${activeBiddingSubscriptions} active subscriptions`);
        warnLog('WebSocket', '[Watchdog] Forcing reconnection to verify connection health');

        // Force a clean reconnection
        try {
          this.ws?.close(1000, 'Watchdog: No bid activity detected');
        } catch {
          warnLog('WebSocket', 'Error closing WebSocket during watchdog recovery');
        }
      }
    }, this.bidActivityWatchdogInterval);
  }

  /**
   * Check if server status messages are being received.
   * Server sends status every 30 seconds - if we haven't received one in 90s,
   * the connection may be half-open.
   */
  private checkServerStatusHealth(): void {
    if (!this.isConnected()) return;

    const now = Date.now();

    // Only check if we've received at least one server status
    if (this.lastServerStatusReceived > 0 &&
        (now - this.lastServerStatusReceived) > this.serverStatusTimeout) {

      warnLog('WebSocket', `[ServerStatus] No server status received for ${Math.floor((now - this.lastServerStatusReceived) / 1000)}+ seconds`);

      // Send a ping to verify connection is still alive
      this.sendPing();
    }
  }

  private async syncSubscriptions(): Promise<void> {
    try {
      infoLog('WebSocket', `[SYNC] Starting subscription sync after reconnection | persistentListings=${this.persistentListingSubscriptions.size} | activeBeforeClear=${this.activeSubscriptions.size}`);

      // First, request current subscriptions from server to verify state
      this.requestSubscriptions();

      // Wait a bit for server response
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Log what we had before clearing
      const previousSlugs = this.getActiveSlugsList();
      infoLog('WebSocket', `[SYNC] Clearing active subscriptions | previousSlugs=${previousSlugs}`);

      // Clear active subscriptions to ensure clean state
      this.activeSubscriptions.clear();

      // Restore persistent listing subscriptions to active subscriptions
      for (const [key, value] of this.persistentListingSubscriptions) {
        this.activeSubscriptions.set(key, value);
        infoLog('WebSocket', `[SYNC] Restoring persistent listing subscription | key=${key} | slug=${value.slug}`);
      }

      // Skip sync if no subscriptions
      if (this.activeSubscriptions.size === 0 && this.persistentListingSubscriptions.size === 0) {
        infoLog('WebSocket', '[SYNC] No subscriptions to sync - skipping');
        return;
      }

      infoLog('WebSocket', `[SYNC] Syncing subscriptions after reconnection | activeSubscriptions=${this.activeSubscriptions.size} | persistentListings=${this.persistentListingSubscriptions.size}`);

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

            infoLog('WebSocket', `[SYNC] Resubscribing to collection | subscriptionId=${subscriptionId} | slug=${subInfo.slug} | event=${eventName} | type=${subscriptionType}`);

            this.sendMessage(message);
            trackingSet.add(subInfo.slug);

            // Small delay between subscriptions
            await new Promise(resolve => setTimeout(resolve, 100));
          } catch (error) {
            failedSubscriptions.push(subInfo.slug);
            errorLog('WebSocket', `[SYNC] Failed to resubscribe | slug=${subInfo.slug} | type=${subscriptionType}`, error);
          }
        }
      }

      infoLog('WebSocket', `[SYNC] Subscription sync complete | biddingCollections=${subscribedSlugs.size} | listingCollections=${subscribedListings.size} | failed=${failedSubscriptions.length} | failedList=${failedSubscriptions.join(',') || 'none'} | finalActiveSlugs=${this.getActiveSlugsList()}`);

      // Update metrics
      this.metrics.subscriptionCount = this.activeSubscriptions.size;

      // Retry failed subscriptions after a delay
      if (failedSubscriptions.length > 0) {
        setTimeout(() => {
          this.retryFailedSubscriptions(failedSubscriptions);
        }, 5000);
      }

    } catch (error) {
      errorLog('WebSocket', '[SYNC] Failed to sync subscriptions - continuing without sync', error);
    }
  }

  private async retryFailedSubscriptions(failedSlugs: string[]): Promise<void> {
    if (!this.isConnected()) {
// console.log('Cannot retry subscriptions - not connected');
      return;
    }

// console.log('Retrying failed subscriptions:', {
      // count: failedSlugs.length
    // });

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

// console.log('Retrying subscription:', {
            // slug,
            // eventName,
            // subscriptionType
          // });

          this.sendMessage(message);
          await new Promise(resolve => setTimeout(resolve, 200));
        } else {
// console.log('Failed to find subscription info for retry:', slug);
        }
      } catch (error) {
        errorLog('WebSocket', `Retry subscription failed for slug: ${slug}`, error);
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
        // Log what the server thinks we're subscribed to
        const serverSubscriptions = message.subscriptions || [];
        infoLog('WebSocket', `[SYNC] Server subscription sync received | serverCount=${serverSubscriptions.length} | localActiveCount=${this.activeSubscriptions.size} | serverSubs=${JSON.stringify(serverSubscriptions).substring(0, 500)}`);
        return;
      }

      // Handle server status
      if (message.type === 'server_status') {
        this.handleServerStatus(message);
        return;
      }

      // Handle server shutdown notification - trigger immediate reconnection
      if (message.type === 'server_shutdown') {
        infoLog('WebSocket', '[SHUTDOWN] Server shutdown notification received - triggering reconnection');
        this.emit('server_shutdown', message);
        // Close connection to trigger reconnection (use 1001 to indicate going away, not 1000 which prevents reconnect)
        try {
          this.ws?.close(1001, 'Server shutdown');
        } catch {
          warnLog('WebSocket', 'Error closing WebSocket on server shutdown');
        }
        return;
      }

      // Log incoming bid/listing events with subscription validation
      if (message.eventType === 'BID_CREATED' || message.eventType === 'TRAIT_BID_CREATED' || message.eventType === 'LISTING_CREATED') {
        const msgSlug = message.payload?.payload?.collection?.slug || message.slug;
        const msgMarketplace = message.marketplace || 'unknown';

        // Check if we're actually subscribed to this collection
        const isSubscribed = this.isSubscribedToSlug(msgSlug);

        if (!isSubscribed) {
          // Only log UNEXPECTED_EVENT for valid slugs (skip undefined/malformed - server-side noise)
          if (msgSlug && msgSlug !== 'undefined') {
            warnLog('WebSocket', `[UNEXPECTED_EVENT] Unsubscribed collection event | ${message.eventType} | slug=${msgSlug} | marketplace=${msgMarketplace}`);
          }
        } else {
          // Normal case - we are subscribed (no log needed - too noisy)
        }
      }

      this.emit('message', message);

    } catch (error) {
      errorLog('WebSocket', 'Failed to parse WebSocket message', error);

      // Still emit the raw message even if parsing fails
      // This helps prevent message loss
      try {
        this.emit('message', { raw: data.toString(), parseError: true });
      } catch (emitError) {
        errorLog('WebSocket', 'Failed to emit raw message after parse error', emitError);
      }
    }
  }

  // Check if we're subscribed to a specific slug
  private isSubscribedToSlug(slug: string): boolean {
    if (!slug) return false;
    for (const [, info] of this.activeSubscriptions) {
      if (info.slug === slug) return true;
    }
    return false;
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
// console.log('Invalid pong timestamp received:', { timestamp, now: this.lastPongReceived });
      return;
    }

    const latency = this.lastPongReceived - timestamp;
    this.recordLatency(latency);

    // Clear pong timeout when we receive a pong
    this.clearTimeout('pongTimeout');

    // Pong received
  }



  private handleServerStatus(status: any): void {
    // Track server status for health monitoring
    this.lastServerStatusReceived = Date.now();

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
    const now = Date.now();
    if (this.metrics.connectionStartTime) {
      this.metrics.connectionEndTime = now;
      this.metrics.connectionDuration = this.metrics.connectionEndTime - this.metrics.connectionStartTime;
      this.metrics.totalDisconnections++;
    }

    this.metrics.connectionQuality = 'disconnected';

    // CRITICAL: Always log disconnections to help diagnose issues
    const disconnectInfo = {
      code: event.code,
      reason: event.reason || 'unknown',
      connectionDurationMinutes: this.metrics.connectionDuration ? Math.floor(this.metrics.connectionDuration / 60000) : 0,
      lastMessageAge: this.lastMessageReceived ? now - this.lastMessageReceived : 'never',
      lastPongAge: this.lastPongReceived ? now - this.lastPongReceived : 'never',
      lastBidEventAge: this.lastBidEventReceived ? now - this.lastBidEventReceived : 'never',
      lastServerStatusAge: this.lastServerStatusReceived ? now - this.lastServerStatusReceived : 'never',
      missedPongs: this.missedPongs,
      activeSubscriptions: this.activeSubscriptions.size,
      persistentListings: this.persistentListingSubscriptions.size,
      messagesReceived: this.metrics.totalMessages
    };
    errorLog('WebSocket', `DISCONNECTED: ${JSON.stringify(disconnectInfo)}`);

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
// console.log('Normal disconnection, not reconnecting');
    }
  }

  private handleError(error: Error): void {
    this.metrics.totalErrors++;
    this.metrics.lastError = error;

    this.emit('error', error);

    // If we get ECONNREFUSED or similar errors, the connection is dead
    // The close event will handle reconnection
  }

  private attemptReconnect(): void {
    // Reset reconnecting flag to allow new connection attempts
    this.isReconnecting = false;

    // Check if we should stop retrying (0 means infinite retries)
    if (this.config.maxRetries > 0 && this.retryCount >= this.config.maxRetries) {
      warnLog('WebSocket', 'Max reconnection attempts exceeded');
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

    this.retryCount++;

    this.setTimeout('reconnect', () => {
      this.connect().catch(() => {
        // Connection will retry through its own error handling
      });
    }, delay);
  }

  sendMessage(message: any): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      try {
        this.ws.send(JSON.stringify(message));
      } catch {
        warnLog('WebSocket', 'Failed to send message - queueing for retry');
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
        warnLog('WebSocket', 'Failed to send queued message - putting back in queue');
        this.messageQueue.unshift(message);
        break;
      }
    }
  }

  private getAdaptiveStaleThreshold(): number {
    // Calculate message rate over the last 5 minutes using circular buffer
    const messagesPerMinute = this.messageTimestamps.getMessageRate();

    // Define thresholds based on activity level
    if (messagesPerMinute > 10) {
      // High activity: strict threshold (15 minutes)
      return 900000;
    } else if (messagesPerMinute > 1) {
      // Medium activity: moderate threshold (30 minutes)
      return 1800000;
    } else {
      // Low activity: relaxed threshold (60 minutes)
      return this.connectionStaleThreshold;
    }
  }

  async subscribe(slug: string, contractAddress: string, marketplace: string, subscriptionType: 'bidding' | 'listing' = 'bidding'): Promise<void> {
    const subscriptionId = `${slug}:${marketplace}:${subscriptionType}`;

    // Check if already subscribed
    if (this.activeSubscriptions.has(subscriptionId)) {
      debugLog('WebSocket', `[SUBSCRIBE] Already subscribed | subscriptionId=${subscriptionId} | totalActive=${this.activeSubscriptions.size}`);
      // Force resubscribe if we haven't received messages recently
      const now = Date.now();
      if (this.lastMessageReceived > 0 && (now - this.lastMessageReceived) > 60000) {
        infoLog('WebSocket', `[SUBSCRIBE] Forcing resubscription due to no recent messages | subscriptionId=${subscriptionId} | timeSinceLastMessage=${now - this.lastMessageReceived}ms`);
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

    // Log subscription attempt
    infoLog('WebSocket', `[SUBSCRIBE] Subscribing to collection | slug=${slug} | contractAddress=${contractAddress} | marketplace=${marketplace} | type=${subscriptionType} | event=${eventName}`);
    infoLog('WebSocket', `[DEBUG-CLIENT] Sending subscribe | event=${eventName} | slug=${slug} | contract=${contractAddress}`);

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

      // Log successful subscription with full state
      infoLog('WebSocket', `[SUBSCRIBE] Successfully subscribed | subscriptionId=${subscriptionId} | totalActive=${this.activeSubscriptions.size} | totalPersistent=${this.persistentListingSubscriptions.size} | allActiveSlugs=${this.getActiveSlugsList()}`);
      infoLog('WebSocket', `[DEBUG-CLIENT] Subscribe confirmed | slug=${slug} | type=${subscriptionType}`);

      this.emit('subscribed', { slug, marketplace, subscriptionType });

    } catch (error) {
      errorLog('WebSocket', `[SUBSCRIBE] Failed to subscribe | slug=${slug} | marketplace=${marketplace}`, error);
      throw error;
    }
  }

  // Helper to get list of active slugs for debugging
  private getActiveSlugsList(): string {
    const slugs = new Set<string>();
    for (const [, info] of this.activeSubscriptions) {
      slugs.add(`${info.slug}:${info.subscriptionType}`);
    }
    return Array.from(slugs).join(',') || 'none';
  }

  async unsubscribe(slug: string, contractAddress: string, marketplace?: string): Promise<void> {
    // Log state BEFORE unsubscription
    infoLog('WebSocket', `[UNSUBSCRIBE] Starting unsubscription | slug=${slug} | contractAddress=${contractAddress} | marketplace=${marketplace || 'ALL'} | currentActiveSlugs=${this.getActiveSlugsList()} | totalActive=${this.activeSubscriptions.size}`);

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
      infoLog('WebSocket', `[UNSUBSCRIBE] Sending leave_the_party and leave_listings to proxy | slug=${slug} | clientId=${this.config.clientId}`);
      infoLog('WebSocket', `[DEBUG-CLIENT] Sending unsubscribe | event=leave_the_party | slug=${slug} | contract=${contractAddress}`);
      infoLog('WebSocket', `[DEBUG-CLIENT] Sending unsubscribe | event=leave_listings | slug=${slug} | contract=${contractAddress}`);
      this.sendMessage(unsubscribeBidding);
      this.sendMessage(unsubscribeListing);

      // Track what was removed for logging
      const removedKeys: string[] = [];

      // Update local state
      if (marketplace) {
        // Remove specific marketplace subscription
        const biddingKey = `${slug}:${marketplace}:bidding`;
        const listingKey = `${slug}:${marketplace}:listing`;

        if (this.activeSubscriptions.has(biddingKey)) {
          this.activeSubscriptions.delete(biddingKey);
          removedKeys.push(biddingKey);
        }
        if (this.activeSubscriptions.has(listingKey)) {
          this.activeSubscriptions.delete(listingKey);
          removedKeys.push(listingKey);
        }

        // Remove from persistent storage if it's a listing
        if (this.persistentListingSubscriptions.has(listingKey)) {
          this.persistentListingSubscriptions.delete(listingKey);
          this.savePersistentSubscriptions();
          infoLog('WebSocket', `[UNSUBSCRIBE] Removed from persistent storage | listingKey=${listingKey}`);
        }
      } else {
        // Remove all marketplaces for this collection
        ['OPENSEA', 'MAGICEDEN', 'BLUR'].forEach(m => {
          const biddingKey = `${slug}:${m}:bidding`;
          const listingKey = `${slug}:${m}:listing`;

          if (this.activeSubscriptions.has(biddingKey)) {
            this.activeSubscriptions.delete(biddingKey);
            removedKeys.push(biddingKey);
          }
          if (this.activeSubscriptions.has(listingKey)) {
            this.activeSubscriptions.delete(listingKey);
            removedKeys.push(listingKey);
          }

          // Remove from persistent storage
          if (this.persistentListingSubscriptions.has(listingKey)) {
            this.persistentListingSubscriptions.delete(listingKey);
          }
        });
        this.savePersistentSubscriptions();
      }

      this.metrics.subscriptionCount = this.activeSubscriptions.size;

      // Log state AFTER unsubscription
      infoLog('WebSocket', `[UNSUBSCRIBE] Completed | slug=${slug} | removedKeys=${removedKeys.join(',') || 'none'} | remainingActiveSlugs=${this.getActiveSlugsList()} | totalActive=${this.activeSubscriptions.size} | totalPersistent=${this.persistentListingSubscriptions.size}`);
      infoLog('WebSocket', `[DEBUG-CLIENT] Unsubscribe confirmed | slug=${slug}`);

      this.emit('unsubscribed', { slug, marketplace });

    } catch (error) {
      errorLog('WebSocket', `[UNSUBSCRIBE] Failed | slug=${slug} | marketplace=${marketplace}`, error);
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
// console.log('Cannot verify subscriptions - not connected');
      return;
    }

// console.log('Verifying subscriptions:', {
      // localSubscriptions: this.activeSubscriptions.size
    // });

    // Request server to confirm our subscriptions
    this.requestSubscriptions();

    // Wait a bit and then check if we're receiving messages
    setTimeout(() => {
      if (this.activeSubscriptions.size > 0) {
        const timeSinceLastMessage = this.lastMessageReceived ? Date.now() - this.lastMessageReceived : 0;
        if (timeSinceLastMessage > 120000) { // 2 minutes
// console.log('Subscription verification failed - no recent messages:', {
            // timeSinceLastMessage,
            // activeSubscriptions: this.activeSubscriptions.size
          // });
          // Force resubscription
          this.syncSubscriptions().catch(() => {
            warnLog('WebSocket', 'Forced resubscription failed during verification');
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
    // Push to circular buffer - no array.filter() needed!
    this.messageTimestamps.push(Date.now());

    // Calculate message rate (per minute) using circular buffer
    this.metrics.messageRate = this.messageTimestamps.getMessageRate();
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

  /**
   * Mark that a bid event was received.
   * Called by counterbid handlers to track bid activity for the watchdog.
   */
  markBidEventReceived(): void {
    this.lastBidEventReceived = Date.now();
  }

  private safeCloseWebSocket(): void {
    if (!this.ws) return;

    this.ws.removeAllListeners();

    try {
      switch (this.ws.readyState) {
        case WebSocket.CONNECTING:
          // Can't terminate a CONNECTING WebSocket - just clean up
// console.log('Cleaning up WebSocket in CONNECTING state');
          // Store reference for cleanup
          const wsToCleanup = this.ws;
          // Set to null to prevent further operations
          this.ws = null;

          // Set up cleanup when the connection eventually completes or fails
          wsToCleanup.once('open', () => {
            try {
              wsToCleanup.close();
            } catch {
              warnLog('WebSocket', 'Error closing WebSocket during safe cleanup');
            }
          });
          wsToCleanup.once('error', () => {
            // Connection failed, nothing to do
          });
          break;
        case WebSocket.OPEN:
          // Gracefully close if open
          try {
            this.ws.close(1000, 'Normal closure');
          } catch {
            warnLog('WebSocket', 'Error closing WebSocket - attempting force terminate');
            try {
              this.ws.terminate(); // Force terminate if close fails
            } catch {
              warnLog('WebSocket', 'Error force terminating WebSocket');
            }
          }
          break;
        case WebSocket.CLOSING:
          // Already closing, terminate to speed it up
          try {
            this.ws.terminate();
          } catch {
            warnLog('WebSocket', 'Error terminating WebSocket in CLOSING state');
          }
          break;
        // CLOSED state doesn't need any action
      }
    } catch {
      warnLog('WebSocket', 'Unexpected error in safeCloseWebSocket');
    }
  }

  disconnect(): void {
    // Prevent reconnection
    this.retryCount = this.config.maxRetries > 0 ? this.config.maxRetries : Number.MAX_SAFE_INTEGER;
    this.isReconnecting = false;

    this.clearAllIntervals();
    this.clearAllTimeouts();

    if (this.ws) {
      this.safeCloseWebSocket();
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

        // Log what was loaded from persistent storage
        const loadedSlugs = Array.from(this.persistentListingSubscriptions.values()).map(v => v.slug);
        infoLog('WebSocket', `[PERSISTENT] Loaded persistent listing subscriptions from file | count=${this.persistentListingSubscriptions.size} | slugs=${loadedSlugs.join(',') || 'none'} | file=${this.subscriptionStateFile}`);
      } else {
        infoLog('WebSocket', `[PERSISTENT] No persistent subscriptions file found | file=${this.subscriptionStateFile}`);
      }
    } catch (error) {
      warnLog('WebSocket', `[PERSISTENT] Failed to load persistent subscriptions from file | error=${error}`);
    }
  }

  private savePersistentSubscriptions(): void {
    try {
      const fs = require('fs');
      const subscriptions = Array.from(this.persistentListingSubscriptions.entries());
      fs.writeFileSync(this.subscriptionStateFile, JSON.stringify(subscriptions, null, 2));

      const savedSlugs = Array.from(this.persistentListingSubscriptions.values()).map(v => v.slug);
      infoLog('WebSocket', `[PERSISTENT] Saved persistent subscriptions to file | count=${subscriptions.length} | slugs=${savedSlugs.join(',') || 'none'}`);
    } catch (error) {
      warnLog('WebSocket', `[PERSISTENT] Failed to save persistent subscriptions to file | error=${error}`);
    }
  }

  /**
   * Verify and log listing subscriptions for debugging
   * This helps identify if listing subscriptions are properly maintained
   */
  async verifyListingSubscriptions(): Promise<void> {
    const listingSubs = Array.from(this.activeSubscriptions.entries())
      .filter(([, info]) => info.subscriptionType === 'listing');

    // const persistentListingSubs = Array.from(this.persistentListingSubscriptions.entries());

// console.log('Listing subscription verification:', {
      // activeListingSubscriptions: listingSubs.length,
      // persistentListingSubscriptions: this.persistentListingSubscriptions.size,
      // subscriptions: listingSubs.map(([key, info]) => ({
        // key,
        // slug: info.slug,
        // marketplace: info.marketplace,
        // subscribedAt: new Date(info.subscribedAt).toISOString()
      // })),
      // connectionState: this.isConnected() ? 'connected' : 'disconnected',
      // lastMessageReceived: this.lastMessageReceived ? new Date(this.lastMessageReceived).toISOString() : 'never',
      // messagesSinceConnect: this.metrics.totalMessages
    // });

    // Check if we're receiving messages for these subscriptions
    if (listingSubs.length > 0 && this.lastMessageReceived) {
      const timeSinceLastMessage = Date.now() - this.lastMessageReceived;
      if (timeSinceLastMessage > 120000) { // 2 minutes
// console.log('Listing subscriptions may be stale:', {
          // timeSinceLastMessage,
          // activeListingSubscriptions: listingSubs.length
        // });
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
// console.log('Force unsubscribing from all collections:', {
      // activeSubscriptions: this.activeSubscriptions.size
    // });

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