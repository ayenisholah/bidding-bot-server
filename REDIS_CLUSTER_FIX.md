# Redis Cluster CROSSSLOT Error Fix

## Problem
The error "CROSSSLOT Keys in request don't hash to the same slot" occurs when using BullMQ with Redis Cluster. This happens because:

1. Redis Cluster distributes keys across different nodes based on a hash function
2. Redis commands that operate on multiple keys require all keys to be on the same node
3. BullMQ uses multiple keys for queue management (wait, active, stalled, etc.)

## Solution
Use Redis hash tags to ensure all related keys are stored on the same slot/node.

### Changes Made

1. **Updated Queue Name** in `listingScheduler.ts`:
   ```typescript
   // Before
   const LISTING_QUEUE_NAME = 'listing-scheduler';
   
   // After
   const LISTING_QUEUE_NAME = '{listing}:scheduler';
   ```

2. **Added Prefix to Queue Options**:
   ```typescript
   const queueOptions = {
     connection,
     prefix: '{listing}',  // Added this
     defaultJobOptions: {
       // ...
     }
   };
   ```

3. **Added Prefix to Queue Events**:
   ```typescript
   queueEvents = new QueueEvents(LISTING_QUEUE_NAME, { 
     connection, 
     prefix: '{listing}'  // Added this
   });
   ```

4. **Added Prefix to Worker**:
   ```typescript
   new Worker(
     LISTING_QUEUE_NAME,
     async (job) => { /* ... */ },
     {
       connection,
       concurrency: WORKER_CONCURRENCY,
       prefix: '{listing}',  // Added this
     }
   );
   ```

## How Hash Tags Work

When Redis sees a key with curly braces like `{listing}:scheduler:wait`, it only uses the content within the braces (`listing`) to determine which slot/node to use. This ensures all keys with the same hash tag go to the same node:

- `{listing}:scheduler:wait` → slot determined by "listing"
- `{listing}:scheduler:active` → slot determined by "listing"
- `{listing}:scheduler:stalled` → slot determined by "listing"

All these keys will be on the same Redis node, allowing BullMQ operations to work correctly.

## To Apply the Fix

1. Stop the server
2. Start the server again
3. The listing scheduler will now work without CROSSSLOT errors

## Note
The main bidding queue already uses `prefix: '{bull}'` so it doesn't have this issue.