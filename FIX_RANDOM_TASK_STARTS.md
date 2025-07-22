# Fix for Random Task Starts Issue

## Root Cause
The queue event handlers incorrectly parse job IDs using `split('-')`, but job IDs contain multiple hyphens in unpredictable positions.

**Example problematic job IDs:**
- `taskId123-opensea-Eyes-Blue-cryptopunks` (from createJobKey)
- `taskId123-crypto-punks-OPENSEA_COLLECTION_BID` (from generateJobKey)

When parsing with `split('-')[1]`, it gets the wrong value, causing:
- Wrong taskId extraction
- Wrong marketplace identification  
- Tasks completing for the wrong task
- Random tasks starting when they shouldn't

## Solution

### Option 1: Use the JobIdParser (Quick Fix)

Replace the problematic parsing in `initializeQueueEventHandlers()`:

**Current code (line ~1715):**
```typescript
const taskId = jobId.split('-')[0];
const marketplace = jobId.split('-')[1].toLowerCase();
```

**Replace with:**
```typescript
import { JobIdParser } from './utils/job-id-parser';

// In the event handler:
const parsed = JobIdParser.parseJobId(jobId);
if (!parsed.isValid) {
  console.error(`Invalid job ID format: ${jobId}`);
  return;
}

const taskId = parsed.taskId;
const marketplace = parsed.marketplace?.toLowerCase();

// Validate the taskId
if (!JobIdParser.isValidTaskId(taskId)) {
  console.error(`Invalid task ID format: ${taskId} from job ${jobId}`);
  return;
}
```

### Option 2: Better Solution - Store Metadata in Job Data

Modify job creation to include metadata:

**In processJob function:**
```typescript
// When creating jobs, add metadata
const jobData = {
  ...task,
  _metadata: {
    taskId: task._id.toString(),
    marketplace: 'opensea' // or whatever marketplace
  }
};
```

**In event handlers:**
```typescript
// Get job data to access metadata
const job = await queue.getJob(jobId);
const taskId = job?.data?._metadata?.taskId || job?.data?._id;
const marketplace = job?.data?._metadata?.marketplace;
```

### Option 3: Use Unique Delimiter (Best Long-term)

Change job key generation to use a delimiter that won't appear in data:

**Update createJobKey:**
```typescript
function createJobKey(job: Job) {
  // ... existing logic ...
  const key = `${baseKey}::${uniqueKey}::${job?.data?.contract?.slug || job?.data?.slug}`;
  return key;
}
```

**Update parsing:**
```typescript
const parts = jobId.split('::');
const taskId = parts[0];
```

## Implementation Steps

1. **Add import at top of index.ts:**
```typescript
import { JobIdParser } from './utils/job-id-parser';
```

2. **Update the completed event handler (around line 1715):**
```typescript
// Replace the current parsing logic
const parsed = JobIdParser.parseJobId(jobId);
if (!parsed.isValid || !JobIdParser.isValidTaskId(parsed.taskId)) {
  console.error(`[QueueEvent] Invalid job ID: ${jobId}`);
  return;
}

const taskId = parsed.taskId;
const task = activeTasks.get(taskId);

if (!task || !task.running) return;

// For marketplace, prefer to get it from job data or parsed result
const marketplace = parsed.marketplace || 'unknown';
```

3. **Update the failed event handler (around line 1971):**
Apply the same parsing fix.

4. **Add logging to debug:**
```typescript
console.log(`[QueueEvent] Processing job: ${jobId}, parsed taskId: ${taskId}, marketplace: ${marketplace}`);
```

## Testing

1. Add this debug function to monitor job processing:

```typescript
function debugJobParsing(jobId: string) {
  console.log('\n=== JOB ID PARSING DEBUG ===');
  console.log(`Original jobId: ${jobId}`);
  console.log(`Split by '-': `, jobId.split('-'));
  
  const parsed = JobIdParser.parseJobId(jobId);
  console.log(`Parsed result:`, parsed);
  
  if (parsed.isValid) {
    console.log(`✓ Valid taskId: ${parsed.taskId}`);
    console.log(`✓ Marketplace: ${parsed.marketplace || 'not found'}`);
  } else {
    console.log(`✗ Failed to parse job ID`);
  }
  console.log('=========================\n');
}
```

2. Call it in the event handlers to verify parsing:
```typescript
debugJobParsing(jobId);
```

## Expected Results

After implementing this fix:
- Correct taskId extraction from complex job IDs
- Tasks only process their own completion events
- No more random task starts
- Proper marketplace identification

## Verification

Monitor the logs for:
- "Invalid job ID" errors (should be none)
- Task completion messages (should match the correct task)
- Random task starts (should stop happening)