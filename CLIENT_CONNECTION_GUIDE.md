# Client Connection Maintenance Guide for WebSocket Marketplace Subscriptions

This guide provides comprehensive strategies and best practices for maintaining a stable, persistent connection to the WebSocket marketplace proxy server for both collection subscriptions (bids/offers) and listing subscriptions.

## Table of Contents
1. [Overview](#overview)
2. [Connection Architecture](#connection-architecture)
3. [Initial Connection Setup](#initial-connection-setup)
4. [Heartbeat Strategy](#heartbeat-strategy)
5. [Reconnection Logic](#reconnection-logic)
6. [State Persistence](#state-persistence)
7. [Error Recovery](#error-recovery)
8. [Production-Ready Implementation](#production-ready-implementation)
9. [Monitoring & Debugging](#monitoring--debugging)
10. [Performance Optimization](#performance-optimization)

## Overview

The WebSocket marketplace proxy supports two types of subscriptions:

1. **Bidding Subscriptions** (`subscribeToCollection`/`join_the_party`): Receive bid and offer events
2. **Listing Subscriptions** (`subscribeToListings`/`join_listings`): Receive new listing and ask events

Both subscription types share the same connection infrastructure. Maintaining this connection reliably is crucial for:

- **Real-time data flow**: Ensuring no marketplace events are missed
- **State persistence**: All subscriptions survive server restarts and network interruptions
- **Efficient resource usage**: Single connection for all marketplaces and subscription types
- **Data integrity**: Preventing duplicate events and ensuring ordered delivery

## Connection Architecture

### Server-Side Features
- **Redis-backed persistence**: All client state stored in Redis with 5-minute TTL
- **Message queuing**: Events queued for offline clients up to 5 minutes
- **Multiple marketplace connections**: 3 connections per marketplace for redundancy
- **Automatic recovery**: Server restores subscriptions after restart
- **Health monitoring**: Regular status updates every 30 seconds

### Client Requirements
- **Persistent client ID**: Must use same ID across reconnections
- **Regular heartbeat**: Ping every 30 seconds to maintain connection
- **Graceful reconnection**: Exponential backoff with jitter
- **Local state tracking**: Mirror subscriptions locally for recovery

## Initial Connection Setup

```javascript
class MarketplaceSubscriptionClient {
  constructor() {
    // Generate or load persistent client ID
    this.clientId = this.loadClientId() || this.generateClientId();
    this.ws = null;
    this.connectionState = 'disconnected';
    this.collectionSubscriptions = new Map(); // For bids/offers
    this.listingSubscriptions = new Map();    // For listings/asks
    this.messageBuffer = [];
    this.reconnectAttempts = 0;
  }

  generateClientId() {
    const id = `marketplace-client-${Date.now()}-${crypto.randomBytes(8).toString('hex')}`;
    this.saveClientId(id);
    return id;
  }

  async connect() {
    try {
      this.connectionState = 'connecting';
      this.ws = new WebSocket('wss://your-server.com', {
        // Connection options
        handshakeTimeout: 10000,
        perMessageDeflate: false, // Disable compression for lower latency
        headers: {
          'User-Agent': 'MarketplaceSubscriptionClient/1.0'
        }
      });

      this.setupEventHandlers();
    } catch (error) {
      console.error('Connection failed:', error);
      this.handleConnectionError(error);
    }
  }

  setupEventHandlers() {
    this.ws.on('open', () => {
      this.connectionState = 'connected';
      this.reconnectAttempts = 0;
      
      // Critical: Send initial ping immediately
      this.sendPing();
      
      // Restore subscriptions after successful ping
      setTimeout(() => this.restoreSubscriptions(), 100);
      
      // Start heartbeat
      this.startHeartbeat();
      
      console.log(`Connected with clientId: ${this.clientId}`);
    });

    // ... other handlers
  }
}
```

## Heartbeat Strategy

Maintaining an active heartbeat is critical for connection stability:

```javascript
class HeartbeatManager {
  constructor(client) {
    this.client = client;
    this.pingInterval = null;
    this.pongTimeout = null;
    this.missedPongs = 0;
    this.maxMissedPongs = 3;
  }

  start() {
    this.stop();
    
    // Send ping every 25 seconds (before 30s timeout)
    this.pingInterval = setInterval(() => {
      this.sendPing();
    }, 25000);
    
    // Send initial ping
    this.sendPing();
  }

  sendPing() {
    if (this.client.ws?.readyState === WebSocket.OPEN) {
      const pingTime = Date.now();
      
      this.client.send({
        event: 'ping',
        clientId: this.client.clientId,
        timestamp: pingTime
      });

      // Set pong timeout
      this.pongTimeout = setTimeout(() => {
        this.missedPongs++;
        console.warn(`Missed pong ${this.missedPongs}/${this.maxMissedPongs}`);
        
        if (this.missedPongs >= this.maxMissedPongs) {
          console.error('Max missed pongs reached, reconnecting...');
          this.client.handleConnectionLoss('pong_timeout');
        }
      }, 5000); // 5 second pong timeout
    }
  }

  handlePong(message) {
    clearTimeout(this.pongTimeout);
    this.missedPongs = 0;
    
    const latency = Date.now() - message.timestamp;
    console.log(`Heartbeat latency: ${latency}ms`);
    
    // Track latency for monitoring
    this.client.metrics.recordLatency(latency);
  }

  stop() {
    clearInterval(this.pingInterval);
    clearTimeout(this.pongTimeout);
    this.pingInterval = null;
    this.pongTimeout = null;
  }
}
```

## Reconnection Logic

Implement robust reconnection with exponential backoff and jitter:

```javascript
class ReconnectionManager {
  constructor(client) {
    this.client = client;
    this.baseDelay = 1000; // 1 second
    this.maxDelay = 300000; // 5 minutes
    this.maxAttempts = Infinity; // Keep trying
    this.jitterRange = 0.3; // 30% jitter
  }

  scheduleReconnect(reason) {
    if (this.client.connectionState === 'reconnecting') {
      return; // Already reconnecting
    }

    this.client.connectionState = 'reconnecting';
    
    const delay = this.calculateDelay();
    console.log(`Reconnecting in ${delay}ms (attempt ${this.client.reconnectAttempts + 1})`);
    console.log(`Reason: ${reason}`);

    setTimeout(() => {
      this.client.reconnectAttempts++;
      this.client.connect();
    }, delay);
  }

  calculateDelay() {
    // Exponential backoff with jitter
    const exponentialDelay = Math.min(
      this.baseDelay * Math.pow(2, this.client.reconnectAttempts),
      this.maxDelay
    );
    
    // Add jitter to prevent thundering herd
    const jitter = exponentialDelay * this.jitterRange * (Math.random() - 0.5);
    
    return Math.round(exponentialDelay + jitter);
  }

  reset() {
    this.client.reconnectAttempts = 0;
  }
}
```

## State Persistence

Ensure subscriptions survive across reconnections:

```javascript
class SubscriptionStateManager {
  constructor(client) {
    this.client = client;
    this.storage = this.initializeStorage();
  }

  initializeStorage() {
    // Use appropriate storage based on environment
    if (typeof window !== 'undefined') {
      // Browser environment
      return {
        save: (key, value) => localStorage.setItem(key, JSON.stringify(value)),
        load: (key) => JSON.parse(localStorage.getItem(key) || 'null'),
        remove: (key) => localStorage.removeItem(key)
      };
    } else {
      // Node.js environment
      const fs = require('fs').promises;
      const path = require('path');
      const stateFile = path.join(__dirname, '.listing-subscriptions.json');
      
      return {
        save: async (key, value) => {
          const state = await this.loadFullState();
          state[key] = value;
          await fs.writeFile(stateFile, JSON.stringify(state, null, 2));
        },
        load: async (key) => {
          const state = await this.loadFullState();
          return state[key];
        },
        remove: async (key) => {
          const state = await this.loadFullState();
          delete state[key];
          await fs.writeFile(stateFile, JSON.stringify(state, null, 2));
        },
        loadFullState: async () => {
          try {
            const content = await fs.readFile(stateFile, 'utf8');
            return JSON.parse(content);
          } catch {
            return {};
          }
        }
      };
    }
  }

  async saveSubscriptions() {
    const collectionSubs = Array.from(this.client.collectionSubscriptions.entries());
    const listingSubs = Array.from(this.client.listingSubscriptions.entries());
    await this.storage.save('collection_subscriptions', collectionSubs);
    await this.storage.save('listing_subscriptions', listingSubs);
  }

  async loadSubscriptions() {
    const collectionSubs = await this.storage.load('collection_subscriptions') || [];
    const listingSubs = await this.storage.load('listing_subscriptions') || [];
    this.client.collectionSubscriptions = new Map(collectionSubs);
    this.client.listingSubscriptions = new Map(listingSubs);
    return {
      collections: this.client.collectionSubscriptions,
      listings: this.client.listingSubscriptions
    };
  }

  async addCollectionSubscription(slug, contractAddress) {
    this.client.collectionSubscriptions.set(slug, { slug, contractAddress });
    await this.saveSubscriptions();
  }

  async addListingSubscription(slug, contractAddress) {
    this.client.listingSubscriptions.set(slug, { slug, contractAddress });
    await this.saveSubscriptions();
  }

  async removeCollectionSubscription(slug) {
    this.client.collectionSubscriptions.delete(slug);
    await this.saveSubscriptions();
  }

  async removeListingSubscription(slug) {
    this.client.listingSubscriptions.delete(slug);
    await this.saveSubscriptions();
  }
}
```

## Error Recovery

Handle various error scenarios gracefully:

```javascript
class ErrorHandler {
  constructor(client) {
    this.client = client;
    this.errorCounts = new Map();
    this.errorThresholds = {
      parse_error: 10,
      send_error: 5,
      subscription_error: 3
    };
  }

  handleError(errorType, error, context = {}) {
    // Track error frequency
    const count = (this.errorCounts.get(errorType) || 0) + 1;
    this.errorCounts.set(errorType, count);

    console.error(`[${errorType}] ${error.message}`, context);

    // Check if error threshold exceeded
    const threshold = this.errorThresholds[errorType];
    if (threshold && count >= threshold) {
      console.error(`Error threshold exceeded for ${errorType}`);
      this.handleCriticalError(errorType);
    }

    // Specific error handling
    switch (errorType) {
      case 'connection_error':
        this.client.reconnectionManager.scheduleReconnect(error.message);
        break;
        
      case 'parse_error':
        // Log but continue operation
        this.client.metrics.recordError('parse_error', context);
        break;
        
      case 'subscription_error':
        // Retry subscription after delay
        setTimeout(() => {
          this.retrySubscription(context.slug, context.contractAddress);
        }, 5000);
        break;
        
      case 'send_error':
        // Buffer message for retry
        this.client.messageBuffer.push(context.message);
        break;
    }
  }

  handleCriticalError(errorType) {
    // Implement circuit breaker pattern
    console.error(`Critical error threshold reached: ${errorType}`);
    
    // Notify monitoring system
    this.client.metrics.recordCriticalError(errorType);
    
    // Reset error count after cooldown
    setTimeout(() => {
      this.errorCounts.set(errorType, 0);
    }, 60000); // 1 minute cooldown
  }

  async retrySubscription(slug, contractAddress, type = 'listing') {
    try {
      if (type === 'collection') {
        await this.client.subscribeToCollection(slug, contractAddress);
      } else {
        await this.client.subscribeToListings(slug, contractAddress);
      }
      console.log(`Successfully retried ${type} subscription for ${slug}`);
    } catch (error) {
      console.error(`Failed to retry ${type} subscription for ${slug}:`, error);
    }
  }

  reset() {
    this.errorCounts.clear();
  }
}
```

## Production-Ready Implementation

Complete implementation with all features:

```javascript
const WebSocket = require('ws');
const EventEmitter = require('events');

class ProductionMarketplaceClient extends EventEmitter {
  constructor(config = {}) {
    super();
    
    // Configuration
    this.config = {
      url: config.url || 'wss://your-server.com',
      reconnectDelay: config.reconnectDelay || 1000,
      maxReconnectDelay: config.maxReconnectDelay || 300000,
      heartbeatInterval: config.heartbeatInterval || 25000,
      messageTimeout: config.messageTimeout || 10000,
      maxBufferSize: config.maxBufferSize || 1000,
      ...config
    };
    
    // Core properties
    this.clientId = this.loadOrGenerateClientId();
    this.ws = null;
    this.connectionState = 'disconnected';
    this.collectionSubscriptions = new Map(); // For bids/offers
    this.listingSubscriptions = new Map();    // For listings/asks
    this.messageBuffer = [];
    this.reconnectAttempts = 0;
    
    // Managers
    this.heartbeatManager = new HeartbeatManager(this);
    this.reconnectionManager = new ReconnectionManager(this);
    this.stateManager = new SubscriptionStateManager(this);
    this.errorHandler = new ErrorHandler(this);
    this.metrics = new MetricsCollector(this);
    
    // Message handling
    this.messageHandlers = new Map();
    this.setupMessageHandlers();
    
    // Initialize
    this.initialize();
  }

  async initialize() {
    // Load saved subscriptions
    await this.stateManager.loadSubscriptions();
    
    // Setup process handlers
    this.setupProcessHandlers();
    
    // Start metrics collection
    this.metrics.start();
  }

  setupProcessHandlers() {
    // Graceful shutdown
    const shutdown = async (signal) => {
      console.log(`Received ${signal}, shutting down gracefully...`);
      
      // Stop heartbeat
      this.heartbeatManager.stop();
      
      // Close connection
      if (this.ws) {
        this.ws.close(1000, 'Client shutting down');
      }
      
      // Save state
      await this.stateManager.saveSubscriptions();
      
      // Stop metrics
      this.metrics.stop();
      
      process.exit(0);
    };

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));
    
    // Handle uncaught errors
    process.on('uncaughtException', (error) => {
      console.error('Uncaught exception:', error);
      this.errorHandler.handleError('uncaught_exception', error);
    });
    
    process.on('unhandledRejection', (reason, promise) => {
      console.error('Unhandled rejection at:', promise, 'reason:', reason);
      this.errorHandler.handleError('unhandled_rejection', new Error(reason));
    });
  }

  setupMessageHandlers() {
    // Pong handler
    this.messageHandlers.set('pong', (message) => {
      this.heartbeatManager.handlePong(message);
      this.emit('pong', message);
    });
    
    // Server status handler
    this.messageHandlers.set('server_status', (message) => {
      this.metrics.recordServerStatus(message);
      this.emit('server_status', message);
      
      // Acknowledge status
      this.send({
        event: 'status_ack',
        clientId: this.clientId
      });
    });
    
    // Subscription sync handler
    this.messageHandlers.set('subscription_sync', (message) => {
      console.log('Subscription sync received:', message);
      this.emit('subscription_sync', message);
    });
    
    // Server shutdown handler
    this.messageHandlers.set('server_shutdown', (message) => {
      console.warn('Server shutdown notice:', message);
      this.emit('server_shutdown', message);
      
      // Schedule reconnection after suggested delay
      setTimeout(() => {
        this.reconnectionManager.scheduleReconnect('server_shutdown');
      }, message.reconnectIn || 5000);
    });
  }

  async connect() {
    try {
      this.connectionState = 'connecting';
      
      // Create WebSocket with options
      this.ws = new WebSocket(this.config.url, {
        handshakeTimeout: 10000,
        perMessageDeflate: false,
        headers: {
          'User-Agent': 'ProductionMarketplaceClient/1.0',
          'X-Client-Id': this.clientId
        }
      });
      
      this.setupWebSocketHandlers();
      
    } catch (error) {
      this.errorHandler.handleError('connection_error', error);
    }
  }

  setupWebSocketHandlers() {
    this.ws.on('open', async () => {
      console.log('WebSocket connected');
      this.connectionState = 'connected';
      this.reconnectAttempts = 0;
      this.errorHandler.reset();
      
      // Send initial ping
      this.sendPing();
      
      // Process buffered messages
      await this.processMessageBuffer();
      
      // Restore subscriptions
      await this.restoreAllSubscriptions();
      
      // Start heartbeat
      this.heartbeatManager.start();
      
      // Emit connected event
      this.emit('connected');
    });

    this.ws.on('message', (data) => {
      try {
        const message = JSON.parse(data.toString());
        this.handleMessage(message);
      } catch (error) {
        this.errorHandler.handleError('parse_error', error, { data });
      }
    });

    this.ws.on('error', (error) => {
      this.errorHandler.handleError('websocket_error', error);
      this.emit('error', error);
    });

    this.ws.on('close', (code, reason) => {
      console.log(`WebSocket closed: ${code} - ${reason}`);
      this.connectionState = 'disconnected';
      this.heartbeatManager.stop();
      
      // Emit disconnected event
      this.emit('disconnected', { code, reason });
      
      // Determine if we should reconnect
      if (code !== 1000) { // Not a normal closure
        this.reconnectionManager.scheduleReconnect(`close_${code}`);
      }
    });

    this.ws.on('ping', () => {
      // Respond to server pings
      if (this.ws.readyState === WebSocket.OPEN) {
        this.ws.pong();
      }
    });
  }

  handleMessage(message) {
    // Check for registered handler
    const handler = this.messageHandlers.get(message.type);
    if (handler) {
      handler(message);
      return;
    }

    // Handle marketplace events
    if (message.marketplace) {
      this.handleMarketplaceEvent(message);
      return;
    }

    // Handle listing-specific events
    if (message.eventType === 'ORDER_CREATED' && message.subscriptionType?.includes('listings')) {
      this.handleListingEvent(message);
      return;
    }

    // Handle bid/offer events
    if (message.subscriptionType?.includes('bids')) {
      this.handleCollectionEvent(message);
      return;
    }

    // Unknown message type
    console.warn('Unknown message type:', message);
    this.emit('unknown_message', message);
  }

  handleListingEvent(event) {
    // Emit specific listing event
    this.emit('listing', event);
    
    // Emit marketplace-specific event
    this.emit(`listing:${event.marketplace?.toLowerCase()}`, event);
    
    // Emit collection-specific event
    if (event.contractAddress) {
      this.emit(`listing:${event.contractAddress.toLowerCase()}`, event);
    }
    
    // Record metrics
    this.metrics.recordListingEvent(event);
  }

  handleMarketplaceEvent(event) {
    // Emit general marketplace event
    this.emit('marketplace_event', event);
    
    // Emit specific event types
    this.emit(event.event, event);
  }

  handleCollectionEvent(event) {
    // Emit specific collection event
    this.emit('collection', event);
    
    // Emit marketplace-specific event
    this.emit(`collection:${event.marketplace?.toLowerCase()}`, event);
    
    // Emit collection-specific event
    if (event.contractAddress || event.payload?.payload?.asset_contract_criteria?.address) {
      const address = (event.contractAddress || event.payload?.payload?.asset_contract_criteria?.address).toLowerCase();
      this.emit(`collection:${address}`, event);
    }
    
    // Record metrics
    this.metrics.recordCollectionEvent(event);
  }

  async subscribeToCollection(slug, contractAddress) {
    try {
      // Validate inputs
      if (!slug || !contractAddress) {
        throw new Error('Both slug and contractAddress are required');
      }
      
      // Check if already subscribed
      if (this.collectionSubscriptions.has(slug)) {
        console.log(`Already subscribed to collection ${slug}`);
        return;
      }
      
      // Store subscription locally
      await this.stateManager.addCollectionSubscription(slug, contractAddress);
      
      // Send subscription message
      const message = {
        event: 'join_the_party',
        clientId: this.clientId,
        slug,
        contractAddress
      };
      
      this.send(message);
      
      console.log(`Subscribed to collection ${slug} for bids/offers`);
      this.emit('subscription_added', { slug, contractAddress, type: 'collection' });
      
    } catch (error) {
      this.errorHandler.handleError('subscription_error', error, { slug, contractAddress, type: 'collection' });
      throw error;
    }
  }

  async subscribeToListings(slug, contractAddress) {
    try {
      // Validate inputs
      if (!slug || !contractAddress) {
        throw new Error('Both slug and contractAddress are required');
      }
      
      // Check if already subscribed
      if (this.listingSubscriptions.has(slug)) {
        console.log(`Already subscribed to listings for ${slug}`);
        return;
      }
      
      // Store subscription locally
      await this.stateManager.addListingSubscription(slug, contractAddress);
      
      // Send subscription message
      const message = {
        event: 'join_listings',
        clientId: this.clientId,
        slug,
        contractAddress
      };
      
      this.send(message);
      
      console.log(`Subscribed to listings for ${slug}`);
      this.emit('subscription_added', { slug, contractAddress, type: 'listings' });
      
    } catch (error) {
      this.errorHandler.handleError('subscription_error', error, { slug, contractAddress, type: 'listings' });
      throw error;
    }
  }

  async unsubscribeFromCollection(slug, contractAddress) {
    try {
      // Check if subscribed
      if (!this.collectionSubscriptions.has(slug)) {
        console.log(`Not subscribed to collection ${slug}`);
        return;
      }
      
      // Remove subscription locally
      await this.stateManager.removeCollectionSubscription(slug);
      
      // Send unsubscribe message
      const message = {
        event: 'leave_the_party',
        clientId: this.clientId,
        slug,
        contractAddress
      };
      
      this.send(message);
      
      console.log(`Unsubscribed from collection ${slug}`);
      this.emit('subscription_removed', { slug, contractAddress, type: 'collection' });
      
    } catch (error) {
      this.errorHandler.handleError('unsubscription_error', error, { slug, contractAddress, type: 'collection' });
      throw error;
    }
  }

  async unsubscribeFromListings(slug, contractAddress) {
    try {
      // Check if subscribed
      if (!this.listingSubscriptions.has(slug)) {
        console.log(`Not subscribed to listings for ${slug}`);
        return;
      }
      
      // Remove subscription locally
      await this.stateManager.removeListingSubscription(slug);
      
      // Send unsubscribe message
      const message = {
        event: 'leave_listings',
        clientId: this.clientId,
        slug,
        contractAddress
      };
      
      this.send(message);
      
      console.log(`Unsubscribed from listings for ${slug}`);
      this.emit('subscription_removed', { slug, contractAddress, type: 'listings' });
      
    } catch (error) {
      this.errorHandler.handleError('unsubscription_error', error, { slug, contractAddress, type: 'listings' });
      throw error;
    }
  }

  async restoreAllSubscriptions() {
    const totalSubs = this.collectionSubscriptions.size + this.listingSubscriptions.size;
    console.log(`Restoring ${totalSubs} subscriptions (${this.collectionSubscriptions.size} collections, ${this.listingSubscriptions.size} listings)`);
    
    // Restore collection subscriptions
    for (const [slug, data] of this.collectionSubscriptions) {
      try {
        const message = {
          event: 'join_the_party',
          clientId: this.clientId,
          slug: data.slug,
          contractAddress: data.contractAddress
        };
        
        this.send(message);
        
        // Small delay between subscriptions
        await new Promise(resolve => setTimeout(resolve, 100));
        
      } catch (error) {
        console.error(`Failed to restore collection subscription for ${slug}:`, error);
      }
    }
    
    // Restore listing subscriptions
    for (const [slug, data] of this.listingSubscriptions) {
      try {
        const message = {
          event: 'join_listings',
          clientId: this.clientId,
          slug: data.slug,
          contractAddress: data.contractAddress
        };
        
        this.send(message);
        
        // Small delay between subscriptions
        await new Promise(resolve => setTimeout(resolve, 100));
        
      } catch (error) {
        console.error(`Failed to restore listing subscription for ${slug}:`, error);
      }
    }
    
    console.log('All subscriptions restored');
    this.emit('subscriptions_restored');
  }

  send(message) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      try {
        this.ws.send(JSON.stringify(message));
        this.metrics.recordMessageSent(message.event);
      } catch (error) {
        this.errorHandler.handleError('send_error', error, { message });
        this.bufferMessage(message);
      }
    } else {
      // Buffer message for later
      this.bufferMessage(message);
    }
  }

  sendPing() {
    this.send({
      event: 'ping',
      clientId: this.clientId,
      timestamp: Date.now()
    });
  }

  bufferMessage(message) {
    if (this.messageBuffer.length >= this.config.maxBufferSize) {
      // Remove oldest message
      this.messageBuffer.shift();
    }
    
    this.messageBuffer.push({
      message,
      timestamp: Date.now()
    });
  }

  async processMessageBuffer() {
    if (this.messageBuffer.length === 0) return;
    
    console.log(`Processing ${this.messageBuffer.length} buffered messages`);
    
    const messages = [...this.messageBuffer];
    this.messageBuffer = [];
    
    for (const { message, timestamp } of messages) {
      // Skip expired messages
      if (Date.now() - timestamp > this.config.messageTimeout) {
        continue;
      }
      
      this.send(message);
      
      // Small delay between messages
      await new Promise(resolve => setTimeout(resolve, 50));
    }
  }

  async getSubscriptions() {
    this.send({
      event: 'get_subscriptions',
      clientId: this.clientId
    });
  }

  loadOrGenerateClientId() {
    // Try to load from persistent storage
    const stored = this.loadClientId();
    if (stored) return stored;
    
    // Generate new ID
    const id = `prod-marketplace-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    this.saveClientId(id);
    return id;
  }

  loadClientId() {
    if (typeof window !== 'undefined') {
      return localStorage.getItem('marketplace_client_id');
    } else {
      try {
        const fs = require('fs');
        return fs.readFileSync('.marketplace-client-id', 'utf8').trim();
      } catch {
        return null;
      }
    }
  }

  saveClientId(id) {
    if (typeof window !== 'undefined') {
      localStorage.setItem('marketplace_client_id', id);
    } else {
      const fs = require('fs');
      fs.writeFileSync('.marketplace-client-id', id);
    }
  }

  async disconnect() {
    console.log('Disconnecting client...');
    
    // Stop heartbeat
    this.heartbeatManager.stop();
    
    // Save state
    await this.stateManager.saveSubscriptions();
    
    // Close WebSocket
    if (this.ws) {
      this.ws.close(1000, 'Client disconnect');
      this.ws = null;
    }
    
    this.connectionState = 'disconnected';
    this.emit('disconnected', { code: 1000, reason: 'Client disconnect' });
  }
}

// Metrics collector for monitoring
class MetricsCollector {
  constructor(client) {
    this.client = client;
    this.metrics = {
      messagesSent: 0,
      messagesReceived: 0,
      collectionEvents: 0,
      listingEvents: 0,
      errors: new Map(),
      latencies: [],
      serverStatuses: []
    };
    this.interval = null;
  }

  start() {
    // Report metrics every minute
    this.interval = setInterval(() => {
      this.report();
    }, 60000);
  }

  stop() {
    clearInterval(this.interval);
  }

  recordMessageSent(eventType) {
    this.metrics.messagesSent++;
  }

  recordCollectionEvent(event) {
    this.metrics.collectionEvents++;
    this.metrics.messagesReceived++;
  }

  recordListingEvent(event) {
    this.metrics.listingEvents++;
    this.metrics.messagesReceived++;
  }

  recordError(errorType, context) {
    const count = this.metrics.errors.get(errorType) || 0;
    this.metrics.errors.set(errorType, count + 1);
  }

  recordCriticalError(errorType) {
    // Send to monitoring service
    console.error(`CRITICAL ERROR: ${errorType}`);
  }

  recordLatency(latency) {
    this.metrics.latencies.push(latency);
    
    // Keep only last 100 latencies
    if (this.metrics.latencies.length > 100) {
      this.metrics.latencies.shift();
    }
  }

  recordServerStatus(status) {
    this.metrics.serverStatuses.push({
      timestamp: Date.now(),
      status
    });
    
    // Keep only last 10 statuses
    if (this.metrics.serverStatuses.length > 10) {
      this.metrics.serverStatuses.shift();
    }
  }

  report() {
    const avgLatency = this.metrics.latencies.length > 0
      ? this.metrics.latencies.reduce((a, b) => a + b, 0) / this.metrics.latencies.length
      : 0;

    const report = {
      timestamp: new Date().toISOString(),
      connectionState: this.client.connectionState,
      collectionSubscriptions: this.client.collectionSubscriptions.size,
      listingSubscriptions: this.client.listingSubscriptions.size,
      messagesSent: this.metrics.messagesSent,
      messagesReceived: this.metrics.messagesReceived,
      collectionEvents: this.metrics.collectionEvents,
      listingEvents: this.metrics.listingEvents,
      averageLatency: avgLatency,
      errors: Object.fromEntries(this.metrics.errors),
      bufferedMessages: this.client.messageBuffer.length
    };

    console.log('Metrics Report:', report);
    
    // Send to monitoring service
    this.client.emit('metrics', report);
  }
}

// Export the client
module.exports = ProductionMarketplaceClient;

// Usage example
if (require.main === module) {
  const client = new ProductionMarketplaceClient({
    url: 'ws://localhost:8080',
    // url: 'wss://your-production-server.com',
  });

  // Event listeners
  client.on('connected', () => {
    console.log('Client connected successfully');
  });

  // Collection events (bids/offers)
  client.on('collection', (event) => {
    console.log(`Collection event: ${event.event} on ${event.marketplace}`);
  });

  client.on('item_received_bid', (event) => {
    console.log(`New bid on ${event.payload?.payload?.collection?.slug}`);
  });

  // Listing events
  client.on('listing', (event) => {
    console.log(`New listing: ${event.tokenId} at ${event.price} ${event.priceUnit}`);
  });

  client.on('error', (error) => {
    console.error('Client error:', error);
  });

  client.on('metrics', (report) => {
    // Send to your monitoring service
    console.log('Metrics:', report);
  });

  // Connect and subscribe
  client.connect().then(() => {
    // Subscribe to collections for bids/offers
    client.subscribeToCollection('boredapeyachtclub', '0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D');
    client.subscribeToCollection('mutant-ape-yacht-club', '0x60E4d786628Fea6478F785A6d7e704777c86a7c6');
    
    // Subscribe to listings
    client.subscribeToListings('boredapeyachtclub', '0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D');
    client.subscribeToListings('azuki', '0xED5AF388653567Af2F388E6224dC7C4b3241C544');
  });

  // Graceful shutdown
  process.on('SIGINT', async () => {
    await client.disconnect();
    process.exit(0);
  });
}
```

## Monitoring & Debugging

### Connection Health Indicators

```javascript
class ConnectionHealthMonitor {
  constructor(client) {
    this.client = client;
    this.healthChecks = {
      lastPong: null,
      lastEvent: null,
      connectionUptime: null,
      reconnectCount: 0,
      errorRate: 0
    };
  }

  getHealthStatus() {
    const now = Date.now();
    const health = {
      status: this.client.connectionState,
      uptime: this.healthChecks.connectionUptime ? now - this.healthChecks.connectionUptime : 0,
      lastPong: this.healthChecks.lastPong ? now - this.healthChecks.lastPong : null,
      lastEvent: this.healthChecks.lastEvent ? now - this.healthChecks.lastEvent : null,
      reconnectCount: this.healthChecks.reconnectCount,
      errorRate: this.healthChecks.errorRate,
      collectionSubscriptions: this.client.collectionSubscriptions.size,
      listingSubscriptions: this.client.listingSubscriptions.size,
      bufferedMessages: this.client.messageBuffer.length
    };

    // Determine overall health
    if (health.status !== 'connected') {
      health.overall = 'unhealthy';
    } else if (health.lastPong && health.lastPong > 60000) {
      health.overall = 'degraded';
    } else if (health.errorRate > 0.1) {
      health.overall = 'degraded';
    } else {
      health.overall = 'healthy';
    }

    return health;
  }

  recordPong() {
    this.healthChecks.lastPong = Date.now();
  }

  recordEvent() {
    this.healthChecks.lastEvent = Date.now();
  }

  recordConnection() {
    this.healthChecks.connectionUptime = Date.now();
  }

  recordReconnect() {
    this.healthChecks.reconnectCount++;
  }

  updateErrorRate(rate) {
    this.healthChecks.errorRate = rate;
  }
}
```

### Debug Logging

```javascript
class DebugLogger {
  constructor(enabled = false) {
    this.enabled = enabled;
    this.logFile = null;
  }

  enable(logFile = null) {
    this.enabled = true;
    this.logFile = logFile;
  }

  disable() {
    this.enabled = false;
  }

  log(level, message, data = {}) {
    if (!this.enabled) return;

    const entry = {
      timestamp: new Date().toISOString(),
      level,
      message,
      data
    };

    // Console output
    console.log(`[${level}] ${message}`, data);

    // File output
    if (this.logFile) {
      const fs = require('fs').promises;
      fs.appendFile(this.logFile, JSON.stringify(entry) + '\n').catch(console.error);
    }
  }

  debug(message, data) {
    this.log('DEBUG', message, data);
  }

  info(message, data) {
    this.log('INFO', message, data);
  }

  warn(message, data) {
    this.log('WARN', message, data);
  }

  error(message, data) {
    this.log('ERROR', message, data);
  }
}
```

## Performance Optimization

### Message Batching

```javascript
class MessageBatcher {
  constructor(client, options = {}) {
    this.client = client;
    this.batchSize = options.batchSize || 10;
    this.batchDelay = options.batchDelay || 100;
    this.queue = [];
    this.timeout = null;
  }

  add(message) {
    this.queue.push(message);

    if (this.queue.length >= this.batchSize) {
      this.flush();
    } else if (!this.timeout) {
      this.timeout = setTimeout(() => this.flush(), this.batchDelay);
    }
  }

  flush() {
    if (this.queue.length === 0) return;

    const messages = [...this.queue];
    this.queue = [];
    clearTimeout(this.timeout);
    this.timeout = null;

    // Send messages
    messages.forEach(msg => this.client.send(msg));
  }
}
```

### Connection Pooling

```javascript
class ConnectionPool {
  constructor(options = {}) {
    this.maxConnections = options.maxConnections || 3;
    this.connections = [];
    this.currentIndex = 0;
  }

  async addConnection(clientConfig) {
    if (this.connections.length >= this.maxConnections) {
      throw new Error('Maximum connections reached');
    }

    const client = new ProductionMarketplaceClient(clientConfig);
    await client.connect();
    this.connections.push(client);
    
    return client;
  }

  getConnection() {
    if (this.connections.length === 0) {
      throw new Error('No connections available');
    }

    // Round-robin selection
    const client = this.connections[this.currentIndex];
    this.currentIndex = (this.currentIndex + 1) % this.connections.length;
    
    return client;
  }

  async closeAll() {
    await Promise.all(
      this.connections.map(client => client.disconnect())
    );
    this.connections = [];
  }
}
```

## Best Practices Summary

1. **Always use persistent client IDs** - Essential for state recovery
2. **Implement exponential backoff with jitter** - Prevents connection storms
3. **Maintain local state** - Mirror both subscription types for quick recovery
4. **Send heartbeats proactively** - Every 25-30 seconds
5. **Handle all error scenarios** - Connection, parsing, and application errors
6. **Monitor connection health** - Track latency, uptime, and error rates
7. **Buffer messages during disconnection** - Prevent data loss
8. **Use event-driven architecture** - Makes integration easier
9. **Implement graceful shutdown** - Save state before exit
10. **Test reconnection scenarios** - Network issues, server restarts, etc.
11. **Subscribe separately** - Collection and listing subscriptions are independent
12. **Handle both event types** - Different event structures for bids/offers vs listings

## Troubleshooting Guide

### Issue: Connection drops after ~2 minutes
**Solution**: Ensure heartbeat is sent every 25-30 seconds

### Issue: Subscriptions lost after reconnection
**Solution**: Use same clientId and restore both collection and listing subscriptions after ping

### Issue: Missing events
**Solution**: 
- For collection events: Check you used `join_the_party` with both slug and contractAddress
- For listing events: Check you used `join_listings` with both slug and contractAddress
- Verify subscriptionType in events to ensure correct routing

### Issue: High latency or delayed events
**Solution**: Disable WebSocket compression, check network path

### Issue: Memory leak with long-running connections
**Solution**: Limit buffer sizes, clear old metrics data

### Issue: Events arriving out of order
**Solution**: Add sequence numbers or timestamps for ordering

### Issue: Not receiving bid events but getting listings
**Solution**: Ensure you've subscribed to both types independently - they use different event names

This guide provides a comprehensive approach to maintaining stable connections for both collection (bid/offer) and listing subscriptions. Implement these patterns based on your specific requirements and scale.