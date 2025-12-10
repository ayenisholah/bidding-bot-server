# How to Revert Bulletproof Task Update Changes

This document provides instructions to revert the bulletproof task update system changes if needed.

## Overview

The changes implemented a comprehensive reliability system for bid price updates across:
- **Server**: `bidding-bot-server/src/index.ts`
- **Client**: `bidding-bot-client/src/app/context/WebSocketContext.tsx`, `bidding-bot-client/src/hooks/useTaskForm.tsx`
- **Changelogs**: Both server and client CHANGELOG.md files

---

## Quick Revert (Git)

If you have the changes in a git commit, simply revert the commit:

```bash
# Find the commit hash
git log --oneline

# Revert the commit (replace COMMIT_HASH with actual hash)
git revert COMMIT_HASH

# Or reset to before the changes (WARNING: loses all uncommitted work)
git reset --hard HEAD~1
```

---

## Manual Revert Instructions

### 1. Server File: `bidding-bot-server/src/index.ts`

#### **Revert Lines 115-117** (Remove idempotency tracking)
**REMOVE:**
```typescript
// Message idempotency tracking
const processedMessages = new Map<string, number>();
const MESSAGE_TTL = 5 * 60 * 1000; // 5 minutes
```

#### **Revert Lines 5163-5189** (Remove helper functions)
**REMOVE the entire block:**
```typescript
  // Helper functions for message idempotency
  function isDuplicateMessage(messageId: string): boolean {
    // ... entire function
  }

  function markMessageProcessed(messageId: string): void {
    // ... entire function
  }
```

#### **Revert WebSocket onmessage handler** (Lines 5191-5318)
**REPLACE** the enhanced version with the original simple version:

```typescript
  ws.onmessage = async (event: WebSocket.MessageEvent) => {
    try {
      const message = JSON.parse(event.data as string);
      switch (message.endpoint) {
        case 'new-task':
          await processNewTask(message.data);
          break;
        case "import-tasks":
          await processImportedTasks(message.data);
          break;
        case 'updated-task':
          await updateTaskData(message.data);
          break;
        case 'toggle-status':
          await updateStatus(message.data);
          break;
        case 'stop-task':
          await stopTask(message.data, false);
          break;
        case 'delete-task':
          await deleteTask(message.data)
          break;
        case 'update-multiple-tasks-status':
          await updateMultipleTasksStatus(message.data);
          break;
        case 'globalMarketplaces':
          processGlobalMarketplaces(message.data)
          break
        case 'update-marketplace':
          await updateMarketplace(message.data)
          break

        case 'create-listing':
          await handleStartListing(message.data, ws)
          break
        case 'stop-listing':
          await handleStopListing(message.data, ws)
          break

        case 'update-listing':
          await handleUpdateListing(message.data, ws)
          break

        default:
      }
    } catch (error) {
      logger.logApiError("WebSocket message handler failed", {
        error: error as Error,
        endpoint: 'unknown',
        messageType: typeof event?.data,
        rawData: event?.data?.toString()?.substring(0, 100)
      });
    }
  };
```

#### **Revert updateTaskData function** (Lines 5814-5868)
**REPLACE** the beginning of the function with the original:

```typescript
async function updateTaskData(task: ITask) {
  try {
    const existingTaskIndex = currentTasks.findIndex(t => t._id === task._id);
    const existingTask = getActiveTaskWithIdByIndex(existingTaskIndex)?.task

    // Clear both warningBids and warningMessage when task is updated
    warningBids[task._id] = {
      opensea: false,
      magiceden: false,
      blur: false
    };

    if (warningMessage[task._id]) {
      delete warningMessage[task._id];
    }

    if (existingTaskIndex === -1 || !existingTask) return

    // Check if loopInterval has changed
    const loopIntervalChanged = existingTask.loopInterval.value !== task.loopInterval.value ||
      existingTask.loopInterval.unit !== task.loopInterval.unit;

    const wallet = await walletCache.getWalletById(task.walletId)
    currentTasks.splice(existingTaskIndex, 1, task);
    activeTasks.set(task._id, task);
    updateTaskIndexes(currentTasks);
```

**And revert the end of the function** (Lines 6253-6267):

```typescript
    if (stop) {
      await stopTask(task, false)
    }
  } catch (error) {
    logSystemError('TaskUpdate', `Error processing updated task: ${task.contract.slug}`, error, { contractSlug: task.contract.slug });
  }
}
```

---

### 2. Client File: `bidding-bot-client/src/app/context/WebSocketContext.tsx`

#### **Revert Interface** (Lines 14-23)
**REMOVE** `sendMessageWithAck` from interface:
```typescript
interface WebSocketContextType {
  sendMessage: (message: any) => void;
  // REMOVE: sendMessageWithAck: (message: any) => Promise<boolean>;
  isConnected: boolean;
  taskLockData: any;
  bidStats: BidStats | null;
  wsConnectionStatus: string | null;
  lastHeartbeat: number | null;
  connectionHealth: "healthy" | "stale" | "disconnected";
}
```

**And in context default** (Lines 25-34):
```typescript
const WebSocketContext = createContext<WebSocketContextType>({
  sendMessage: () => {},
  // REMOVE: sendMessageWithAck: async () => false,
  isConnected: false,
  taskLockData: null,
  bidStats: null,
  wsConnectionStatus: null,
  lastHeartbeat: null,
  connectionHealth: "disconnected",
});
```

#### **Remove queue types and constants** (Lines 39-54)
**REMOVE:**
```typescript
// Message queue types
interface QueuedMessage {
  message: any;
  attempts: number;
  timestamp: number;
}

interface PendingAck {
  message: any;
  timeout: NodeJS.Timeout;
  resolve: (value: boolean) => void;
}

const MAX_QUEUE_SIZE = 100;
const MAX_RETRIES = 3;
const ACK_TIMEOUT = 5000;
```

#### **Remove refs** (Lines 56-60)
**REMOVE:**
```typescript
  const messageQueue = useRef<QueuedMessage[]>([]);
  const pendingAcks = useRef<Map<string, PendingAck>>(new Map());
```

#### **Remove helper functions** (Lines 74-85)
**REMOVE:**
```typescript
  // Helper function to queue messages when WebSocket is unavailable
  const queueMessage = useCallback((message: any, attempts: number = 0) => {
    // ... entire function
  }, []);
```

#### **Simplify ws.current.onopen** (Lines 126-149)
**REPLACE** with original:
```typescript
    ws.current.onopen = () => {
      logger.logInfo(
        ErrorCategory.WEBSOCKET,
        "WebSocket connected successfully",
        {
          url: NEXT_PUBLIC_SERVER_WEBSOCKET,
          operation: "websocket_connected",
        }
      );
      setIsConnected(true);
    };
```

#### **Simplify ws.current.onmessage** (Lines 151-192)
**REPLACE** with original:
```typescript
    ws.current.onmessage = (event) => {
      try {
        const message = JSON.parse(event.data);

        if (message.type === "taskLockUpdate") {
          setTaskLockData(message.data);
        } else if (message.type === "bidRatesUpdate") {
          setBidStats(message.data);
          setConnectionHealth("healthy");
        } else if (message.type === "heartbeat") {
          const now = Date.now();
          setLastHeartbeat(now);
          setConnectionHealth("healthy");
        }
      } catch (error) {
        logger.logWebSocketError("Error parsing WebSocket message", {
          error: error as Error,
          messageData: event.data,
          operation: "websocket_message_parse",
        });
      }
    };
```

#### **Simplify sendMessage** (Lines 218-305)
**REPLACE** all the sendMessage logic with the original:
```typescript
  const sendMessage = useCallback((message: any) => {
    if (ws.current?.readyState === WebSocket.OPEN) {
      ws.current.send(JSON.stringify(message));
    } else {
      logger.logWarning(
        ErrorCategory.WEBSOCKET,
        "Cannot send message - WebSocket is not connected",
        {
          readyState: ws.current?.readyState,
          message,
          operation: "websocket_send_message",
        }
      );
    }
  }, []);
```

#### **Remove from Provider** (Lines 387-402)
**REMOVE** `sendMessageWithAck` from the provider value:
```typescript
  return (
    <WebSocketContext.Provider
      value={{
        sendMessage,
        // REMOVE: sendMessageWithAck,
        isConnected,
        taskLockData,
        bidStats,
        wsConnectionStatus,
        lastHeartbeat,
        connectionHealth,
      }}
    >
      {children}
    </WebSocketContext.Provider>
  );
```

---

### 3. Client File: `bidding-bot-client/src/hooks/useTaskForm.tsx`

#### **Revert useWebSocket destructuring** (Line 19)
**REPLACE:**
```typescript
  const { sendMessage, sendMessageWithAck, isConnected } = useWebSocket();
```

**WITH:**
```typescript
  const { sendMessage } = useWebSocket();
```

#### **Revert task update logic** (Lines 761-815)
**REPLACE** the entire try-catch block with the original:

```typescript
          } else {
            // Original API logic for regular tasks
            const response = await fetch(`/api/task/${taskId}`, {
              method: "PUT",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(taskData),
              credentials: "include",
            });

            if (!response.ok) throw new Error("Failed to update task");
            const updatedTask = await response.json();
            const message = { endpoint: "updated-task", data: updatedTask };
            editTask(taskId, taskData);
            sendMessage(message);
            await refetchTasks();
          }
```

---

### 4. Revert Changelogs

#### **Server CHANGELOG** (`bidding-bot-server/CHANGELOG.md`)
**REMOVE** lines 10-21 (the entire "### Added" section for Bulletproof Task Update System)

#### **Client CHANGELOG** (`bidding-bot-client/CHANGELOG.md`)
**REMOVE** lines 10-24 (the entire "### Added" section for Bulletproof WebSocket Reliability System)

---

## Testing After Revert

1. **Restart both server and client:**
   ```bash
   # Server
   cd bidding-bot-server
   npm run dev

   # Client
   cd bidding-bot-client
   npm run dev
   ```

2. **Test basic task update:**
   - Edit a task's bid price
   - Verify it updates in the UI
   - Check server logs for any errors

3. **Verify WebSocket connection:**
   - Check browser console for WebSocket connection status
   - Verify real-time updates still work

---

## Notes

- If you reverted via git, the manual steps are not needed
- After manual revert, search for any remaining references to `sendMessageWithAck`, `messageQueue`, `pendingAcks`, `processedMessages`, or `isDuplicateMessage`
- Consider keeping this revert guide until the changes are proven stable in production

---

## Support

If you encounter issues during revert:
1. Check git diff to see what changed
2. Use `git status` to see modified files
3. Use `git diff <filename>` to see specific changes
4. Consider using `git stash` to temporarily save changes while testing
