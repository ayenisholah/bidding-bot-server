# Seen Order Hash Fix

## Issue
When a trait offer event arrives, the system finds multiple tasks for the same collection (e.g., one with bidType: "token" and another with bidType: "collection"). The first task would process the order and delete it from `seenOrderHashes`, but then add it back, preventing the second task from processing the same order.

## Root Cause
The `seenOrderHashes` tracking was using the same `orderHash` for all tasks, causing conflicts when multiple tasks tried to process the same incoming bid event.

## Solution
Made the hash truly task-specific by changing:
```typescript
const taskSpecificHash = orderHash
```
to:
```typescript
const taskSpecificHash = `${orderHash}-${task._id}`
```

## Changes Made
1. Updated OpenSea handler to use task-specific hashes
2. Updated all `seenOrderHashes.delete(orderHash)` calls to use `taskSpecificHash`
3. Applied the same fix to MagicEden handler

## Result
Now each task independently tracks whether it has seen and processed an order, allowing:
- Token bidType tasks to place individual token bids for matching traits
- Collection bidType tasks to place trait offers
- Both to process the same incoming trait offer event without interference