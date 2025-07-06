# WebSocket Server Fixes for Persistent Connection Issues

## Issues Identified

### 1. Client ID Assignment on Reconnection
The WebSocket instance doesn't get its `clientId` assigned immediately on reconnection.

**Fix in `wss.on("message")` handler:**
```typescript
if (data?.event?.toLowerCase() === "ping") {
  // Assign clientId immediately
  ws.clientId = clientId;
  
  await registerClient(ws, clientId, Date.now().toString());
  clientLastResponse.set(clientId, Date.now());
  // ... rest of ping handling
}
```

### 2. OpenSea Listing Subscription Topic
The listing subscription uses the wrong topic format.

**Fix in `subscribeToListings`:**
```typescript
if (marketplace.name === OPENSEA) {
  const openseaListingMessage = {
    topic: `collection:${data.slug}:listings`, // Add :listings suffix
    event: "phx_join",
    payload: {},
    ref: 0,
  };
  ws.send(JSON.stringify(openseaListingMessage));
}
```

### 3. Listing Message Type Consistency
Ensure listing messages are properly tagged.

**Fix in `handleOpenSeaMessages`:**
```typescript
else if (message.event === "item_listed") {
  const transformedMessage = {
    // ... existing transformation
    subscriptionType: "listings", // Add this field
  };
  return { message: transformedMessage, contractAddress, slug };
}
```

### 4. WebSocket Instance Client ID Persistence
Store client ID mapping separately to handle reconnections.

**Add global map:**
```typescript
const wsClientMap = new Map<WebSocket, string>();
```

**Update `registerClient`:**
```typescript
async function registerClient(ws: WebSocketWithClient, clientId: string, lastPing: string = Date.now().toString()) {
  ws.clientId = clientId;
  wsClientMap.set(ws, clientId);
  // ... rest of function
}
```

**Update `findWebSocketByClientId`:**
```typescript
function findWebSocketByClientId(clientId: string): WebSocket | undefined {
  for (const [ws, id] of wsClientMap.entries()) {
    if (id === clientId && ws.readyState === WebSocket.OPEN) {
      return ws;
    }
  }
  return undefined;
}
```

**Clean up on disconnect:**
```typescript
ws.on("close", (code: number, reason: Buffer) => {
  // ... existing code
  wsClientMap.delete(ws);
  // ... rest of cleanup
});
```

### 5. Message Queue Processing on All Events
Process queued messages on any client activity, not just ping.

**Add to all event handlers:**
```typescript
// After each event handler (join_the_party, join_listings, etc.)
if (ws.readyState === WebSocket.OPEN) {
  await messageQueue.processQueue(clientId, ws);
}
```

### 6. Subscription Restoration Enhancement
Improve subscription restoration to handle both types properly.

**Update `verifyAndRestoreSubscriptions`:**
```typescript
// When restoring OpenSea subscriptions
if (marketplaceName === OPENSEA) {
  // For bid subscriptions
  const openseaBidMessage = {
    topic: `collection:${sub.slug}`,
    event: "phx_join",
    payload: {},
    ref: 0,
  };
  ws.send(JSON.stringify(openseaBidMessage));
  
  // For listing subscriptions - separate message
  const openseaListingMessage = {
    topic: `collection:${sub.slug}:listings`,
    event: "phx_join",
    payload: {},
    ref: 0,
  };
  ws.send(JSON.stringify(openseaListingMessage));
}
```

### 7. Enhanced Logging for Debugging
Add more detailed logging to track subscription lifecycle.

**In `forwardMessageToClients`:**
```typescript
logger.debug(`Forwarding message to client ${clientId}: ${JSON.stringify({
  messageType: Array.isArray(message) ? 'array' : 'single',
  marketplace: name,
  contractAddress,
  slug,
  isBidSubscribed,
  isListingSubscribed,
  subscriptionType: messageType
})}`);
```

## Testing the Fixes

1. Monitor that listing subscriptions persist across reconnections
2. Verify that `handleListingPriceUpdate` continues receiving messages after reconnection
3. Check that the WebSocket server properly maintains client-subscription mappings
4. Ensure message queuing works for offline clients

## Additional Recommendations

1. **Implement heartbeat from server to client**: Currently only client pings server
2. **Add subscription acknowledgment**: Confirm when subscriptions are successfully established
3. **Implement subscription state sync**: Periodically verify client and server agree on active subscriptions
4. **Add metrics collection**: Track message delivery success rates per client