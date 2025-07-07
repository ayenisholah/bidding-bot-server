# WebSocket Client Protocol & Usage Guide

This document contains everything you need to know to connect and interact with the WebSocket marketplace proxy server.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Connection Details](#connection-details)
3. [Message Protocol](#message-protocol)
4. [Marketplace Events](#marketplace-events)
5. [Error Handling](#error-handling)
6. [Complete Client Implementation](#complete-client-implementation)
7. [Testing Your Connection](#testing-your-connection)
8. [Common Issues & Solutions](#common-issues--solutions)

## Quick Start

```javascript
const WebSocket = require("ws");

// Generate persistent client ID
const clientId = `client-${Date.now()}-${Math.random()
	.toString(36)
	.substr(2, 9)}`;

// Connect
const ws = new WebSocket("ws://localhost:8080");

ws.on("open", () => {
	// 1. Send ping to register
	ws.send(
		JSON.stringify({
			event: "ping",
			clientId: clientId,
		})
	);

	// 2. Subscribe to bid events
	ws.send(
		JSON.stringify({
			event: "join_the_party",
			clientId: clientId,
			slug: "boredapeyachtclub",
			contractAddress: "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
		})
	);

	// 3. Subscribe to listing events
	ws.send(
		JSON.stringify({
			event: "join_listings",
			clientId: clientId,
			slug: "boredapeyachtclub",
			contractAddress: "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
		})
	);

	// 4. Set up heartbeat
	setInterval(() => {
		ws.send(JSON.stringify({ event: "ping", clientId }));
	}, 30000);
});

ws.on("message", (data) => {
	const message = JSON.parse(data);
	console.log("Received:", message);
});
```

## Connection Details

### Server URL

- **Development**: `ws://localhost:8080`
- **Production**: Replace with your deployed server URL

### Connection Requirements

- WebSocket protocol (ws:// or wss:// for SSL)
- Keep connection alive with ping every 30 seconds
- Persistent client ID across reconnections
- Automatic reconnection with exponential backoff

### Server Features

- **Automatic cleanup**: Orphaned subscriptions removed every minute
- **Error tracking**: Processing statistics stored in Redis
- **Connection monitoring**: Stale connections detected after 5 minutes
- **Enhanced health endpoint**: Detailed metrics for monitoring

### Connection Lifecycle

1. **Connect** → Send initial ping → Register client
2. **Subscribe** → Server subscribes to ALL marketplaces automatically
3. **Receive Events** → Filtered by your subscriptions
4. **Disconnect** → Subscriptions persist for 5 minutes
5. **Reconnect** → Send ping → Subscriptions auto-restore

### Server Restart Behavior

- **Client state persists** - Client registration stored in Redis survives server restarts
- **Subscriptions persist** - All subscriptions maintained in Redis
- **Message queue persists** - Queued messages preserved during restart
- **Reconnect required** - Clients must reconnect after server restart
- **Use same clientId** - Essential for recovering state after restart

## Message Protocol

### Client to Server Messages

#### 1. Ping (Heartbeat) - **REQUIRED**

Keep your connection alive and register your client.

```json
{
	"event": "ping",
	"clientId": "your-unique-client-id"
}
```

**When to send**:

- Immediately after connection
- Every 30 seconds to maintain connection
- After reconnection to restore state

**Response**:

```json
{
	"type": "pong",
	"timestamp": 1234567890,
	"message": "pong 🏓 — you rang, mortal?"
}
```

#### 2. Subscribe to Collection

Subscribe to receive events for a specific NFT collection.

```json
{
	"event": "join_the_party",
	"clientId": "your-unique-client-id",
	"slug": "collection-slug",
	"contractAddress": "0x..."
}
```

**Important**:

- Both `slug` and `contractAddress` are required
- Server subscribes to ALL marketplaces (OpenSea, MagicEden, Blur)
- No response sent - subscription is processed asynchronously
- Send one subscription per message

#### 3. Unsubscribe from Collection

Stop receiving events for a specific collection.

```json
{
	"event": "leave_the_party",
	"clientId": "your-unique-client-id",
	"slug": "collection-slug",
	"contractAddress": "0x..."
}
```

#### 4. Subscribe to Listings

Subscribe to receive listing events (asks/sales) for a specific NFT collection.

```json
{
	"event": "join_listings",
	"clientId": "your-unique-client-id",
	"slug": "collection-slug",
	"contractAddress": "0x..."
}
```

**Important**:

- Both `slug` and `contractAddress` are required
- Server subscribes to ALL marketplaces (OpenSea, MagicEden, Blur)
- No response sent - subscription is processed asynchronously
- Separate from bid subscriptions - you can subscribe to both independently
- Send one subscription per message

#### 5. Unsubscribe from Listings

Stop receiving listing events for a specific collection.

```json
{
	"event": "leave_listings",
	"clientId": "your-unique-client-id",
	"slug": "collection-slug",
	"contractAddress": "0x..."
}
```

#### 6. Get Current Subscriptions

Retrieve your active subscriptions.

```json
{
	"event": "get_subscriptions",
	"clientId": "your-unique-client-id"
}
```

**Response**:

```json
{
	"type": "subscription_sync",
	"clientId": "your-unique-client-id",
	"subscriptions": [
		"{\"slug\":\"boredapeyachtclub\",\"contractAddress\":\"0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D\"}"
	],
	"listingSubscriptions": [
		"{\"slug\":\"mutant-ape-yacht-club\",\"contractAddress\":\"0x60E4d786628Fea6478F785A6d7e704777c86a7c6\"}"
	]
}
```

**Notes**:

- `subscriptions`: Bid/offer events (join_the_party)
- `listingSubscriptions`: Listing/ask events (join_listings)
- Both arrays contain JSON-stringified subscription objects

#### 5. Status Acknowledgment

Acknowledge server status messages (optional but recommended).

```json
{
	"event": "status_ack",
	"clientId": "your-unique-client-id"
}
```

### Server to Client Messages

#### 1. Pong Response

Heartbeat acknowledgment.

```json
{
	"type": "pong",
	"timestamp": 1234567890,
	"message": "pong 🏓 — you rang, mortal?"
}
```

#### 2. Server Status

Periodic health updates (every 30 seconds).

```json
{
	"type": "server_status",
	"timestamp": 1234567890,
	"marketplaces": [
		{ "name": "OPENSEA", "connections": 3 },
		{ "name": "MAGICEDEN", "connections": 3 },
		{ "name": "BLUR", "connections": 3 }
	],
	"subscriptions": 15
}
```

#### 3. Server Shutdown

Graceful shutdown notification.

```json
{
	"type": "server_shutdown",
	"reconnectIn": 5000
}
```

## Marketplace Events

All marketplace events include a `marketplace` field indicating the source.

### Event Structure

```json
{
	"event": "event_type",
	"marketplace": "OPENSEA|MAGICEDEN|BLUR",
	"subscriptionType": "bids|listings|both",
	"payload": {
		// Event-specific data
	}
}
```

**Fields**:

- `event`: The specific event type (e.g., "item_received_bid", "item_listed")
- `marketplace`: Source marketplace (OPENSEA, MAGICEDEN, or BLUR)
- `subscriptionType`: Indicates which subscription type triggered this event
  - `"bids"`: Event sent to bid subscribers only
  - `"listings"`: Event sent to listing subscribers only
  - `"both"`: Event sent to clients subscribed to both types
- `payload`: Event-specific data from the marketplace

### Normalized Listing Events

When subscribed to listings using `join_listings`, all marketplaces deliver listing events in a normalized format with `eventType: "ORDER_CREATED"`:

```json
{
	"id": "unique_order_id",
	"marketplace": "OPENSEA|MAGICEDEN|BLUR",
	"eventType": "ORDER_CREATED",
	"fromAddress": "0x...",
	"priceUnit": "ETH",
	"createdAt": "2024-01-15T10:30:00.000Z",
	"transactionHash": null,
	"tokenId": "1234",
	"imageUrl": "https://...",
	"price": "1.5",
	"contractAddress": "0x...",
	"subscriptionType": "listings"
}
```

This normalized format ensures consistent handling across all marketplaces, making it easier to process listing events regardless of their source.

### OpenSea Events

#### item_received_bid

```json
{
	"event": "item_received_bid",
	"marketplace": "OPENSEA",
	"payload": {
		"payload": {
			"collection": { "slug": "collection-name" },
			"asset_contract_criteria": { "address": "0x..." },
			"base_price": "50000000000000000000",
			"quantity": 1,
			"maker": { "address": "0x..." }
		}
	}
}
```

#### collection_offer

```json
{
	"event": "collection_offer",
	"marketplace": "OPENSEA",
	"payload": {
		"payload": {
			"collection": { "slug": "collection-name" },
			"base_price": "1000000000000000000",
			"quantity": 1
		}
	}
}
```

#### item_sold

```json
{
	"event": "item_sold",
	"marketplace": "OPENSEA",
	"payload": {
		"payload": {
			"collection": { "slug": "collection-name" },
			"sale_price": "2000000000000000000",
			"item": { "token_id": "1234" }
		}
	}
}
```

#### item_listed

When subscribed to listings, OpenSea listing events are delivered in a normalized format:

```json
{
	"id": "order_hash_123",
	"marketplace": "OPENSEA",
	"eventType": "ORDER_CREATED",
	"fromAddress": "0x...",
	"priceUnit": "ETH",
	"createdAt": "2024-01-15T10:30:00.000Z",
	"transactionHash": null,
	"tokenId": "5678",
	"imageUrl": "https://...",
	"price": "1.5",
	"contractAddress": "0x...",
	"subscriptionType": "listings"
}
```

### MagicEden Events

#### bid.created

```json
{
	"event": "bid.created",
	"marketplace": "MAGICEDEN",
	"payload": {
		"tags": { "contract": "0x..." },
		"payload": {
			"collection": { "slug": "collection-name" },
			"price": { "amount": "1.5" }
		}
	}
}
```

#### ask.created

When subscribed to listings, MagicEden ask events are delivered in a normalized format:

```json
{
	"id": "ask_id_456",
	"marketplace": "MAGICEDEN",
	"eventType": "ORDER_CREATED",
	"fromAddress": "0x...",
	"priceUnit": "ETH",
	"createdAt": "2024-01-15T10:30:00.000Z",
	"transactionHash": null,
	"tokenId": "9876",
	"imageUrl": "https://...",
	"price": "2.5",
	"contractAddress": "0x...",
	"subscriptionType": "listings"
}
```

### Blur Events

Blur events come in array format:

#### Bid Events

```json
{
  "marketplace": "BLUR",
  "0": "bidLevels",
  "1": {
    "contractAddress": "0x...",
    "levels": [...]
  }
}
```

#### Listing Events (ORDER_CREATED)

When subscribed to listings, Blur ORDER_CREATED events are delivered in a normalized format as individual events:

```json
{
	"id": "924756826",
	"marketplace": "BLUR",
	"eventType": "ORDER_CREATED",
	"fromAddress": "0x5bc136f64226648b41d3964c2cdd63193663225e",
	"priceUnit": "ETH",
	"createdAt": "2025-07-02T12:07:39.452Z",
	"transactionHash": null,
	"tokenId": "6310",
	"imageUrl": "https://images.blur.io/_blur-prod/0x8c186802b1992f7650ac865d4ca94d55ff3c0d17/6310-c650818984b6a288",
	"price": "0.0973722",
	"contractAddress": "0x8c186802b1992f7650ac865d4ca94d55ff3c0d17",
	"subscriptionType": "listings"
}
```

**Note**: Blur sends multiple events in batches, but the server delivers each event individually to clients for consistent handling across all marketplaces.

## Error Handling

### Connection Errors

```javascript
ws.on("error", (error) => {
	console.error("WebSocket error:", error);
	// Don't try to reconnect here - wait for 'close' event
});

ws.on("close", (code, reason) => {
	console.log(`Connection closed: ${code} - ${reason}`);
	// Implement reconnection logic here
	setTimeout(reconnect, 5000);
});
```

### WebSocket Close Codes

- `1000`: Normal closure
- `1001`: Server going away
- `1006`: Abnormal closure (network issue)
- `1011`: Server error

### Reconnection Strategy

```javascript
let retryCount = 0;
const MAX_RETRIES = 10;

function reconnect() {
	if (retryCount >= MAX_RETRIES) {
		console.error("Max reconnection attempts reached");
		return;
	}

	const delay = Math.min(1000 * Math.pow(2, retryCount), 60000);
	retryCount++;

	setTimeout(() => {
		console.log(`Reconnection attempt ${retryCount}`);
		connect();
	}, delay);
}
```

## Complete Client Implementation

```javascript
const WebSocket = require("ws");

class MarketplaceClient {
	constructor() {
		this.clientId = this.loadOrGenerateClientId();
		this.ws = null;
		this.subscriptions = new Map();
		this.listingSubscriptions = new Map();
		this.retryCount = 0;
		this.maxRetries = 10;
		this.pingInterval = null;
		this.reconnectTimeout = null;
	}

	loadOrGenerateClientId() {
		// In browser: use localStorage
		// In Node.js: use file system or database
		return `client-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
	}

	connect() {
		try {
			this.ws = new WebSocket("ws://localhost:8080");
			this.setupEventHandlers();
		} catch (error) {
			console.error("Connection failed:", error);
			this.scheduleReconnect();
		}
	}

	setupEventHandlers() {
		this.ws.on("open", () => {
			console.log("Connected to marketplace proxy");
			this.retryCount = 0;

			// Send initial ping
			this.sendPing();

			// Restore subscriptions
			this.restoreSubscriptions();

			// Start heartbeat
			this.startHeartbeat();
		});

		this.ws.on("message", (data) => {
			try {
				const message = JSON.parse(data.toString());
				this.handleMessage(message);
			} catch (error) {
				console.error("Failed to parse message:", error);
			}
		});

		this.ws.on("error", (error) => {
			console.error("WebSocket error:", error);
		});

		this.ws.on("close", (code, reason) => {
			console.log(`Disconnected: ${code} - ${reason}`);
			this.stopHeartbeat();
			this.scheduleReconnect();
		});
	}

	handleMessage(message) {
		// Handle different message types
		if (message.type === "pong") {
			console.log("Heartbeat acknowledged");
		} else if (message.type === "server_status") {
			console.log("Server status:", message);
			// Optionally acknowledge
			this.send({
				event: "status_ack",
				clientId: this.clientId,
			});
		} else if (message.type === "subscription_sync") {
			console.log("Current bid subscriptions:", message.subscriptions);
			console.log(
				"Current listing subscriptions:",
				message.listingSubscriptions
			);
		} else if (message.type === "server_shutdown") {
			console.log(
				"Server shutting down, will reconnect in",
				message.reconnectIn,
				"ms"
			);
		} else if (message.marketplace) {
			// Marketplace event
			this.handleMarketplaceEvent(message);
		}
	}

	handleMarketplaceEvent(event) {
		const subscriptionType = event.subscriptionType || "unknown";
		console.log(
			`[${event.marketplace}] ${event.event} (${subscriptionType}):`,
			event
		);

		// Process based on event type
		switch (event.event) {
			case "item_received_bid":
				// Handle new bid
				break;
			case "item_sold":
				// Handle sale
				break;
			case "item_listed":
				// Handle new listing
				break;
			case "ask.created":
				// Handle new ask/listing
				break;
			// Add more cases as needed
		}
	}

	subscribe(slug, contractAddress) {
		// Store locally
		this.subscriptions.set(slug, { slug, contractAddress });

		// Send to server
		this.send({
			event: "join_the_party",
			clientId: this.clientId,
			slug,
			contractAddress,
		});

		console.log(`Subscribed to ${slug}`);
	}

	unsubscribe(slug, contractAddress) {
		// Remove locally
		this.subscriptions.delete(slug);

		// Send to server
		this.send({
			event: "leave_the_party",
			clientId: this.clientId,
			slug,
			contractAddress,
		});

		console.log(`Unsubscribed from ${slug}`);
	}

	subscribeToListings(slug, contractAddress) {
		// Store locally
		this.listingSubscriptions.set(slug, { slug, contractAddress });

		// Send to server
		this.send({
			event: "join_listings",
			clientId: this.clientId,
			slug,
			contractAddress,
		});

		console.log(`Subscribed to listings for ${slug}`);
	}

	unsubscribeFromListings(slug, contractAddress) {
		// Remove locally
		this.listingSubscriptions.delete(slug);

		// Send to server
		this.send({
			event: "leave_listings",
			clientId: this.clientId,
			slug,
			contractAddress,
		});

		console.log(`Unsubscribed from listings for ${slug}`);
	}

	getSubscriptions() {
		this.send({
			event: "get_subscriptions",
			clientId: this.clientId,
		});
	}

	restoreSubscriptions() {
		// Re-subscribe to all bid collections
		for (const [slug, data] of this.subscriptions) {
			this.send({
				event: "join_the_party",
				clientId: this.clientId,
				slug: data.slug,
				contractAddress: data.contractAddress,
			});
		}

		// Re-subscribe to all listing collections
		for (const [slug, data] of this.listingSubscriptions) {
			this.send({
				event: "join_listings",
				clientId: this.clientId,
				slug: data.slug,
				contractAddress: data.contractAddress,
			});
		}
	}

	send(message) {
		if (this.ws && this.ws.readyState === WebSocket.OPEN) {
			this.ws.send(JSON.stringify(message));
		} else {
			console.error("WebSocket not connected");
		}
	}

	sendPing() {
		this.send({
			event: "ping",
			clientId: this.clientId,
		});
	}

	startHeartbeat() {
		this.stopHeartbeat();
		this.pingInterval = setInterval(() => {
			this.sendPing();
		}, 30000);
	}

	stopHeartbeat() {
		if (this.pingInterval) {
			clearInterval(this.pingInterval);
			this.pingInterval = null;
		}
	}

	scheduleReconnect() {
		if (this.reconnectTimeout) {
			clearTimeout(this.reconnectTimeout);
		}

		if (this.retryCount >= this.maxRetries) {
			console.error("Max reconnection attempts reached");
			return;
		}

		const delay = Math.min(1000 * Math.pow(2, this.retryCount), 60000);
		this.retryCount++;

		console.log(`Reconnecting in ${delay}ms (attempt ${this.retryCount})`);

		this.reconnectTimeout = setTimeout(() => {
			this.connect();
		}, delay);
	}

	disconnect() {
		this.stopHeartbeat();
		if (this.reconnectTimeout) {
			clearTimeout(this.reconnectTimeout);
		}
		if (this.ws) {
			this.ws.close();
			this.ws = null;
		}
	}
}

// Usage
const client = new MarketplaceClient();
client.connect();

// Subscribe to bid events
client.subscribe(
	"boredapeyachtclub",
	"0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
);
client.subscribe(
	"mutant-ape-yacht-club",
	"0x60E4d786628Fea6478F785A6d7e704777c86a7c6"
);

// Subscribe to listing events
client.subscribeToListings(
	"boredapeyachtclub",
	"0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
);
client.subscribeToListings(
	"azuki",
	"0xED5AF388653567Af2F388E6224dC7C4b3241C544"
);
```

## Testing Your Connection

### 1. Basic Connection Test

```bash
# Using wscat
npm install -g wscat
wscat -c ws://localhost:8080

# Send a ping
{"event":"ping","clientId":"test-client-123"}
```

### 2. Test Subscription

```javascript
// test-client.js
const WebSocket = require("ws");
const ws = new WebSocket("ws://localhost:8080");

ws.on("open", () => {
	// Test ping
	ws.send(
		JSON.stringify({
			event: "ping",
			clientId: "test-123",
		})
	);

	// Test subscription
	setTimeout(() => {
		ws.send(
			JSON.stringify({
				event: "join_the_party",
				clientId: "test-123",
				slug: "boredapeyachtclub",
				contractAddress: "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
			})
		);
	}, 1000);
});

ws.on("message", (data) => {
	console.log("Received:", JSON.parse(data));
});
```

## Common Issues & Solutions

### Issue: Not receiving events

**Solution**:

- Ensure you're sending pings every 30 seconds
- Verify both slug and contractAddress are correct
- Check that you're subscribed (use get_subscriptions)
- Look for subscription confirmation in server logs

### Issue: Connection drops frequently

**Solution**:

- Implement proper heartbeat (ping every 30 seconds)
- Use exponential backoff for reconnection
- Check network stability
- Increase client timeout if needed

### Issue: Subscriptions lost after reconnect

**Solution**:

- Use persistent clientId
- Send ping immediately after reconnection
- Subscriptions persist for 5 minutes - reconnect within this time
- Store subscriptions locally and resubscribe if needed

### Issue: Receiving duplicate events

**Solution**:

- Server deduplicates for 60 seconds
- Check you're not subscribed multiple times
- Ensure only one client instance per clientId

### Issue: Missing events during disconnect

**Solution**:

- Events are queued for 5 minutes
- Send ping after reconnection to receive queued messages
- Implement local event tracking if critical

## Important Notes

1. **Client ID Persistence**: Always use the same clientId across reconnections
2. **Subscription Scope**: One subscription covers ALL marketplaces (OpenSea, MagicEden, Blur)
3. **Message Format**: All messages must be valid JSON
4. **Heartbeat Required**: Send ping every 30 seconds or connection will timeout
5. **Reconnection**: Implement exponential backoff to avoid overwhelming the server
6. **Event Handling**: Always check for `marketplace` field to identify event source
7. **Queue Duration**: Offline messages queued for 5 minutes maximum
8. **No Bulk Operations**: Subscribe to collections one at a time

## Support

For issues or questions:

- Check server logs for detailed error messages
- Verify your client implementation matches this protocol
- Test with the provided example client
- Monitor the health endpoint: `GET http://localhost:8080/health`

## Enhanced Server Monitoring

The server now includes advanced monitoring capabilities to ensure reliability and observability:

### Health Endpoint Response

The `/health` endpoint provides comprehensive monitoring data:

```json
{
	"status": "ok",
	"timestamp": "2024-01-15T10:30:00.000Z",
	"connectedClients": 5,
	"marketplaces": [
		{
			"name": "OPENSEA",
			"connections": [
				{ "index": 0, "connected": true },
				{ "index": 1, "connected": true },
				{ "index": 2, "connected": true }
			]
		},
		{
			"name": "MAGICEDEN",
			"connections": [
				{ "index": 0, "connected": true },
				{ "index": 1, "connected": true },
				{ "index": 2, "connected": true }
			]
		},
		{
			"name": "BLUR",
			"connections": [
				{ "index": 0, "connected": true },
				{ "index": 1, "connected": true },
				{ "index": 2, "connected": false }
			]
		}
	],
	"activeClientIds": ["client-123", "client-456", "client-789"],
	"processingStats": {
		"OPENSEA": {
			"processed": 1500,
			"errors": 3,
			"errorRate": "0.20%"
		},
		"MAGICEDEN": {
			"processed": 1200,
			"errors": 1,
			"errorRate": "0.08%"
		},
		"BLUR": {
			"processed": 2000,
			"errors": 5,
			"errorRate": "0.25%"
		}
	},
	"connectionHealth": {
		"OPENSEA": {
			"messages": 1500,
			"errors": 3,
			"lastMessage": 1705318200000,
			"timeSinceLastMessage": 5000,
			"status": "HEALTHY"
		},
		"MAGICEDEN": {
			"messages": 1200,
			"errors": 1,
			"lastMessage": 1705318195000,
			"timeSinceLastMessage": 10000,
			"status": "HEALTHY"
		},
		"BLUR": {
			"messages": 2000,
			"errors": 5,
			"lastMessage": 1705317900000,
			"timeSinceLastMessage": 305000,
			"status": "STALE"
		}
	}
}
```

### Monitoring Features

1. **Subscription Health Checks** (every minute):

   - Automatically detects and removes orphaned subscriptions
   - Logs subscription statistics: active clients, total bid/listing subscriptions
   - Cleans up data for disconnected clients without proper cleanup

2. **Enhanced Error Tracking**:

   - Message processing counts stored in Redis (key: `websocket:stats`)
   - Error counts with detailed logging (error message, data preview, timestamp)
   - Real-time error rate calculation per marketplace
   - Metrics persist across server restarts

3. **Connection Quality Monitoring** (every 5 minutes):
   - Tracks message flow from each marketplace
   - Detects stale connections (no messages for > 5 minutes)
   - Reports connection health status: HEALTHY or STALE
   - Helps identify marketplace connectivity issues

### Using the Monitoring Data

- **processingStats**: Monitor error rates to detect issues with specific marketplaces
- **connectionHealth**: Check if marketplaces are actively sending data
- **activeClientIds**: Verify your client is properly registered
- **status**: Quick health check - "STALE" indicates potential connection issues

### Redis Keys for Monitoring

The server stores metrics in Redis:

- `websocket:stats` - Hash containing processing counts (`OPENSEA:processed`, `OPENSEA:errors`, etc.)
- `client:*` - Client registration and last ping times
- `subscriptions:*` - Bid/offer subscriptions per client
- `listing_subscriptions:*` - Listing subscriptions per client
