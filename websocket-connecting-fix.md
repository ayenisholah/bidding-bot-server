# WebSocket CONNECTING State Fix

## Problem
The WebSocket library's `terminate()` method throws an error when called on a WebSocket in CONNECTING state:
```
Error: WebSocket was closed before the connection was established
    at WebSocket.terminate (/node_modules/ws/lib/websocket.js:489:7)
```

## Root Cause
The `ws` library's `terminate()` method internally calls `abortHandshake()` when the WebSocket is in CONNECTING state, which throws an error. This is by design in the library - you cannot forcefully terminate a WebSocket that hasn't established a connection yet.

## Solution
Instead of trying to terminate a CONNECTING WebSocket, we:

1. **Remove all event listeners** - Prevents any callbacks from firing
2. **Set internal reference to null** - Prevents our code from using it
3. **Keep a cleanup reference** - Let the WebSocket complete its connection attempt
4. **Clean up after connection** - If it connects, close it; if it errors, ignore

## Implementation

### Connection Timeout Handler
```javascript
if (this.ws.readyState === WebSocket.CONNECTING) {
  // Can't terminate - just clean up
  console.log('Connection timeout - cleaning up WebSocket in CONNECTING state');
  this.ws.removeAllListeners();
  const wsToCleanup = this.ws;
  this.ws = null;
  
  // Clean up when connection completes
  wsToCleanup.once('open', () => {
    try {
      wsToCleanup.close();
    } catch (e) {
      // Ignore
    }
  });
  wsToCleanup.once('error', () => {
    // Connection failed, nothing to do
  });
}
```

## Why This Works
1. **No forced termination** - We don't call terminate() on CONNECTING state
2. **Clean separation** - Our code immediately stops using the old WebSocket
3. **Graceful cleanup** - The WebSocket cleans itself up when ready
4. **No memory leaks** - Event listeners ensure proper cleanup

## Benefits
- No more crashes from terminate() errors
- Clean handling of connection timeouts
- Proper resource cleanup
- Works with the ws library's design