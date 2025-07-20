# WebSocket Disconnection Analysis

## Overview
This document analyzes why WebSocket disconnections are occurring between the client (`websocket-manager.ts`) and the WSS marketplace server, based on production logs showing disconnections every 2-3 minutes.

## Key Findings

### 1. **Heartbeat/Ping-Pong Mechanism Issues**

#### Client Configuration (websocket-manager.ts)
- **Heartbeat Interval**: 25 seconds (line 85)
- **Pong Timeout**: 5 seconds (line 317)
- **Missed Pongs Threshold**: 3 (lines 255, 307)
- **Connection Stale Threshold**: 10 minutes (line 73)

#### Server Requirements (from CLAUDE.md)
- Server expects ping every 30 seconds
- Server will disconnect clients that don't maintain heartbeat

**Issue**: Client sends pings every 25 seconds, which should be sufficient, but the aggressive 3-missed-pongs policy combined with a 5-second timeout means the connection can be terminated after just 15 seconds of network instability.

### 2. **Connection Quality Monitoring**

The client implements aggressive health monitoring that may be too sensitive:

```typescript
// Line 361-373: Forces reconnection after 10 minutes without messages
if (timeSinceLastMessage > 600000) { // 10 minutes
    logger.logWarning(ErrorCategory.WEBSOCKET, 'Forcing reconnection due to severely stale connection');
    this.ws?.close();
}
```

**Issue**: For collections with low activity, this 10-minute threshold might be too aggressive, causing unnecessary reconnections.

### 3. **Stale Connection Detection**

Lines 270-291 show stale connection detection logic:
```typescript
if (this.lastMessageReceived > 0 &&
    (this.activeSubscriptions.size > 0 || this.persistentListingSubscriptions.size > 0) &&
    (now - this.lastMessageReceived) > this.connectionStaleThreshold) {
    logger.logWarning(ErrorCategory.WEBSOCKET, 'Connection appears stale (no messages received), reconnecting...');
    this.ws?.close(1001, 'Stale connection detected');
}
```

**Issue**: This disconnects after 10 minutes without messages, even if the connection is healthy and collections simply have no activity.

### 4. **Network Error Handling**

From the production logs pattern showing "unknown" disconnection reasons and 2-3 minute connection durations, combined with the code analysis:

- Average connection duration: 2m 23s
- Disconnection reasons: `{ unknown: 4 }`
- Pattern suggests network-level issues or timeouts

### 5. **Potential Root Causes**

1. **Proxy/Load Balancer Timeouts**
   - Many cloud providers and proxies have 2-3 minute idle timeouts
   - The WebSocket might be going through a proxy that disconnects idle connections

2. **Server-Side Connection Limits**
   - Server might be enforcing connection duration limits
   - Server might have different heartbeat expectations than documented

3. **Network Instability**
   - Brief network interruptions causing missed pongs
   - The 3-strikes policy is too aggressive for real-world network conditions

4. **Message Queue Backpressure**
   - High message volume causing delayed pong responses
   - Client interpreting delays as connection failures

## Recommendations

### 1. **Increase Heartbeat Tolerance**
```typescript
// Increase missed pongs threshold from 3 to 5
if (this.missedPongs >= 5) { // Was 3
    logger.logWarning(ErrorCategory.WEBSOCKET, 'Connection appears dead, reconnecting...');
}

// Increase pong timeout from 5s to 10s
this.setTimeout('pongTimeout', () => {
    this.missedPongs++;
}, 10000); // Was 5000
```

### 2. **Adjust Stale Connection Threshold**
```typescript
// Increase from 10 minutes to 30 minutes for low-activity collections
private connectionStaleThreshold = 1800000; // Was 600000 (10 minutes)
```

### 3. **Implement Adaptive Health Monitoring**
Instead of fixed thresholds, adapt based on historical message rates:
- High-activity collections: stricter timeouts
- Low-activity collections: relaxed timeouts

### 4. **Add Connection Duration Tracking**
The client already tracks connection metrics but should log more details about why connections are closing:
```typescript
private handleClose(event: { code: number; reason: string }): void {
    // Add more detailed logging
    logger.logWarning(ErrorCategory.WEBSOCKET, 'WebSocket disconnected', {
        code: event.code,
        reason: event.reason || 'unknown',
        connectionDuration: this.metrics.connectionDuration,
        lastMessageAge: Date.now() - this.lastMessageReceived,
        missedPongs: this.missedPongs,
        activeSubscriptions: this.activeSubscriptions.size
    });
}
```

### 5. **Implement Keep-Alive Messages**
Send periodic no-op messages to prevent proxy timeouts:
```typescript
// Send keep-alive every 90 seconds
setInterval(() => {
    if (this.isConnected()) {
        this.sendMessage({ event: 'keep-alive', clientId: this.config.clientId });
    }
}, 90000);
```

### 6. **Check Server-Side Logs**
The "unknown" disconnection reason suggests the server isn't providing clear close codes. Server logs might reveal:
- Rate limiting
- Memory pressure
- Connection pool limits
- Heartbeat validation failures

## Connection Lifecycle Summary

1. **Connection Established** → Immediate ping sent
2. **Heartbeat Loop** → Ping every 25s, expect pong within 5s
3. **Health Monitoring** → Check connection quality every 60s
4. **Stale Detection** → Disconnect if no messages for 10 minutes
5. **Disconnection** → Exponential backoff reconnection

## Nginx Configuration Analysis

After reviewing the nginx configuration, the server-side settings are actually well-configured for WebSocket connections:

### Nginx Settings (Good)
- **proxy_read_timeout**: 86400s (24 hours) ✅
- **proxy_send_timeout**: 86400s (24 hours) ✅
- **keepalive_timeout**: 70s ✅
- **proxy_buffering**: off (critical for real-time) ✅
- **proxy_socket_keepalive**: on ✅
- **tcp_nodelay**: on (reduces latency) ✅

The nginx configuration is NOT the cause of the 2-3 minute disconnections.

## Root Cause: Client-Side Stale Connection Detection

The issue is in the client's aggressive stale connection detection at line 270-291 of websocket-manager.ts:

```typescript
private connectionStaleThreshold = 600000; // 10 minutes without any messages
```

However, from the logs showing 2-3 minute disconnections, the actual issue might be:
1. **Health monitor forced disconnection** (line 361) checking every minute
2. **Heartbeat missed pongs** accumulating too quickly
3. **Network instability** combined with low tolerance thresholds

## Revised Conclusion

The disconnections are NOT due to nginx or proxy timeouts, but rather the client's own health monitoring being too aggressive. The 2-3 minute pattern suggests:

1. **Missed Pongs Accumulation**: With heartbeat every 25s and 3-strike policy, disconnection can occur in ~75 seconds
2. **Health Monitor Checks**: Running every 60 seconds, could trigger disconnection logic
3. **Network Jitter**: Brief network issues cause pong delays, triggering disconnection

**Immediate Actions**:
1. **Increase missed pong tolerance** from 3 to 5-6
2. **Increase pong timeout** from 5s to 15s 
3. **Add jitter tolerance** to latency calculations
4. **Log more details** about why disconnections occur
5. **Disable aggressive stale detection** for low-activity collections

**Configuration Changes Needed**:
```typescript
// websocket-manager.ts adjustments
heartbeatInterval: 25000,        // Keep at 25s
pongTimeout: 15000,              // Increase from 5s to 15s
maxMissedPongs: 6,               // Increase from 3 to 6
connectionStaleThreshold: 3600000 // Increase from 10min to 60min
```

This would allow the connection to survive up to 90 seconds of network instability (6 missed pongs × 15s timeout) instead of the current 15 seconds (3 × 5s).