# StartTask Function and Task Execution Flow Analysis

## Overview

The `startTask` function is the central orchestrator for initiating bidding tasks in the NFT bidding bot. It manages the lifecycle of a task from initialization through execution across multiple marketplaces.

## 1. Functions That Call startTask

### Direct Callers:
- **updateStatus()** - Called when toggling task status via WebSocket message 'toggle-status'
- **updateMultipleTasksStatus()** - Batch operation for starting/stopping multiple tasks
- **runScheduledLoop()** - Called during server startup to start all running tasks
- **processImportedTasks()** - When tasks are imported via WebSocketT
- **Loop scheduling mechanism** - Self-referential calls for continuous execution

### Indirect Triggers:
- WebSocket client sends 'toggle-status' message
- Server startup loads and starts all running tasks from MongoDB
- Task completion schedules the next loop iteration

## 2. Job Queue Flow After startTask

### Initial Job Creation:
1. `startTask` creates SCHEDULE jobs for each marketplace:
   - `OPENSEA_SCHEDULE`
   - `BLUR_SCHEDULE`
   - `MAGICEDEN_SCHEDULE`

2. Jobs are added to BullMQ queue with:
   - Unique jobId: `${task._id}-${marketplace}-schedule`
   - Unlimited retry policy for SCHEDULE jobs
   - Priority based on marketplace

### Job Processing:
1. Worker picks up SCHEDULE job from queue
2. Calls appropriate processor:
   - `processOpenseaScheduledBid()`
   - `processBlurScheduledBid()`
   - `processMagicedenScheduledBid()`

3. Each processor:
   - Validates task is still running
   - Fetches collection/trait data
   - Creates specific bid jobs:
     - `OPENSEA_TOKEN_BID` / `OPENSEA_TRAIT_BID` / `OPENSEA_COLLECTION_BID`
     - Similar for Blur and MagicEden

4. Bid jobs are processed to place actual bids on marketplaces

## 3. Task Status Progression

### Status Flow:
```
STARTED → PROCESSING → SCHEDULED → RUNNING → COMPLETED → SCHEDULED (loop)
```

### Detailed Progression:
1. **STARTED** - Initial state when startTask is called
2. **PROCESSING** - Set when SCHEDULE job begins processing
3. **SCHEDULED** - Set after bid jobs are queued
4. **RUNNING** - Set after 1 second delay when jobs are actively being processed
5. **COMPLETED** - Set when all marketplaces finish processing
6. **SCHEDULED** (again) - For next loop iteration

### Key Status Transitions:
- `startTask()` sets status to `STARTED`
- `processOpenseaScheduledBid()` transitions to `PROCESSING` then `SCHEDULED`
- Parent job tracking updates status to `RUNNING` when jobs complete
- Loop scheduling sets back to `SCHEDULED` after completion

## 4. Circuit Breaker Functionality

### Purpose:
Prevents tasks from consuming resources when repeatedly failing

### Operation:
1. Tracks consecutive timeouts per task+marketplace
2. After 5 consecutive timeouts:
   - Suspends the marketplace for that task for 5 minutes
   - Prevents new jobs from being created
3. Automatically resumes after suspension period
4. Success resets the consecutive failure counter

### Implementation:
- Located in `src/utils/task-circuit-breaker.ts`
- Checked in `startTask` before creating jobs
- Updated by worker on job timeout/success

## 5. WebSocket Subscriptions

### Subscription Flow:
1. `startTask` calls `subscribeToCollections()` for running tasks
2. Sends `join_the_party` message to WebSocket server
3. Server subscribes to real-time marketplace events for the collection
4. Incoming events trigger counterbid logic if enabled

### Subscription Management:
- All running tasks are subscribed regardless of marketplace selection
- Subscriptions persist across task restarts
- Handled by `WebSocketManager` with automatic reconnection

## 6. Task, Job, and Marketplace Relationships

### Hierarchy:
```
Task (MongoDB document)
  ├── Selected Marketplaces (opensea, blur, magiceden)
  ├── Bid Type (collection, token, trait)
  └── Configuration (price, traits, outbid options)
     
Job Types:
  ├── SCHEDULE Jobs (one per marketplace)
  │   └── Create specific bid jobs
  ├── BID Jobs (multiple per marketplace)
  │   └── Execute actual marketplace bids
  └── COUNTERBID Jobs (triggered by WebSocket events)
```

### Parent Job Tracking:
- Tracks completion percentage across all jobs for a task
- Updates task status based on aggregate completion
- Handles marketplace-specific completion tracking

## 7. Deduplication Logic

### Purpose:
Prevents duplicate jobs in queue to avoid redundant processing

### Implementation:
1. **Loop Restarts** (`start=true`):
   - Skip deduplication entirely
   - Always add new SCHEDULE jobs
   - Ensures continuous execution

2. **Initial Start** (`start=false`):
   - Query existing jobs in queue
   - Filter out jobs that already exist for the task
   - Only add genuinely new jobs

### Rationale:
- Loop restarts need fresh jobs to continue execution
- Initial starts should avoid creating duplicates if jobs already exist
- Prevents queue bloat and redundant API calls

## Key Insights

1. **Fault Tolerance**: SCHEDULE jobs have unlimited retries to ensure tasks continue running
2. **Resource Management**: Circuit breaker prevents runaway failing tasks
3. **Real-time Capability**: WebSocket subscriptions enable instant counterbidding
4. **Scalability**: Job queue allows distributed processing across workers
5. **State Consistency**: Multiple tracking mechanisms ensure task state accuracy

## Common Issues and Solutions

1. **Stuck Tasks**: Periodic checks detect and recover tasks stuck at 100% completion
2. **Memory Leaks**: BoundedCache and automatic cleanup prevent unbounded growth
3. **Race Conditions**: Task locking and deduplication prevent duplicate processing
4. **Network Failures**: Retry logic and circuit breaker handle transient failures