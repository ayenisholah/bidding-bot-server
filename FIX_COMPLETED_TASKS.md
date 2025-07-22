# Fix for Task Completion Cross-Contamination

## Root Cause
The `completedTasks` global object accumulates state across all tasks and is never properly reset, causing:
1. Task A's completion state to affect Task B
2. Old completion states persisting across task restarts
3. Tasks starting randomly when other tasks complete

## Critical Issues Found

### 1. Global State Contamination
```typescript
// Current problematic code
const completedTasks: CompletedTasks = {};  // Global object, never cleaned
```

### 2. No Reset on Task Start
When a task restarts after completing a loop, its `completedTasks` entry still shows all marketplaces as `true`, causing immediate re-triggering.

### 3. Incorrect Task Isolation
The completion check doesn't properly isolate tasks - if Task A completes, it might trigger Task B to start.

## Solution

### Step 1: Import the Task Completion Manager

Add to imports in `index.ts`:
```typescript
import { taskCompletionManager } from './utils/task-completion-manager';
```

### Step 2: Reset Completion State on Task Start

In the `startTask` function (around line 3433), add:
```typescript
async function startTask(task: ITask, start: boolean, marketplace?: string) {
  const taskId = task._id.toString();
  
  // CRITICAL: Reset completion state when task starts
  taskCompletionManager.resetTask(taskId);
  taskCompletionManager.initializeTask(taskId);
  
  // ... rest of the function
}
```

### Step 3: Fix the Completion Logic

Replace the problematic section (lines 1754-1773) in the queue event handler:

**Current problematic code:**
```typescript
if (parentJob.completedJobs + parentJob.failedJobs === parentJob.totalJobs) {
  if (!completedTasks[taskId]) {
    completedTasks[taskId] = {
      opensea: false,
      magiceden: false,
      blur: false
    }
  }
  completedTasks[taskId][marketplace as 'opensea' | 'magiceden' | 'blur'] = true
  console.log(GREEN + `Task ${taskId} completed for ${marketplace}` + RESET);
  const availableMarketplaces = enabledAndSelectedMarketplaces.map(m => m.toLowerCase())

  // Check if all available marketplaces have completed
  if (availableMarketplaces && availableMarketplaces.length > 0) {
    const allMarketsComplete = availableMarketplaces.every(m =>
      completedTasks[taskId]?.[m as 'opensea' | 'magiceden' | 'blur']
    );
```

**Replace with:**
```typescript
if (parentJob.completedJobs + parentJob.failedJobs === parentJob.totalJobs) {
  // Use the manager for proper isolation
  taskCompletionManager.markMarketplaceComplete(taskId, marketplace as any);
  console.log(GREEN + `Task ${taskId} completed for ${marketplace}` + RESET);
  
  // CRITICAL: Only check this specific task's required marketplaces
  const taskSpecificMarketplaces = task.selectedMarketplaces
    .map(m => m.toLowerCase())
    .filter(m => enabledMarketplaces.map(em => em.toLowerCase()).includes(m));
  
  if (taskSpecificMarketplaces.length > 0) {
    const allMarketsComplete = taskCompletionManager.areAllMarketplacesComplete(
      taskId,
      taskSpecificMarketplaces
    );
    
    // Double-check that we're looking at the right task
    if (allMarketsComplete && runningPercentage > 99) {
      // Verify this is actually the task we think it is
      const verifyTask = activeTasks.get(taskId);
      if (!verifyTask || verifyTask._id.toString() !== taskId) {
        console.error(`[TaskCompletion] Task mismatch! Expected ${taskId}, got ${verifyTask?._id}`);
        return;
      }
      
      console.log(GREEN + `Task ${taskId} completed for all marketplaces` + RESET);
      // ... continue with task completion logic
```

### Step 4: Add Task Cleanup

In the task deletion/cleanup logic, add:
```typescript
// When a task is deleted or stopped
taskCompletionManager.cleanupTask(taskId);
```

### Step 5: Add Debug Endpoint

Add this endpoint to help debug completion states:
```typescript
app.get('/debug/task-completion', (req, res) => {
  const states = {};
  taskCompletionManager.getAllCompletionStates().forEach((state, taskId) => {
    states[taskId] = state;
  });
  
  res.json({
    completionStates: states,
    activeTasks: Array.from(activeTasks.keys()),
    verification: taskCompletionManager.verifyTaskIsolation()
  });
});
```

### Step 6: Fix the Failed Event Handler

Apply the same fixes to the 'failed' event handler (around line 2008-2026).

## Additional Safety Checks

Add these validations to prevent cross-task contamination:

```typescript
// Before scheduling next loop (line ~1820)
// Verify we have the correct task
if (task._id.toString() !== taskId) {
  console.error(`[CRITICAL] Task ID mismatch in completion handler!`);
  console.error(`Expected: ${taskId}, Got: ${task._id.toString()}`);
  return; // DO NOT PROCEED
}

// Verify the task exists and is still running
const currentTaskState = activeTasks.get(taskId);
if (!currentTaskState || !currentTaskState.running) {
  console.log(`[TaskCompletion] Task ${taskId} is no longer active, skipping restart`);
  return;
}
```

## Testing the Fix

1. Create multiple tasks with different loop intervals
2. Let them run for several loops
3. Monitor logs for:
   - "Task mismatch" errors
   - Correct task IDs in completion messages
   - Tasks only restarting their own loops

## Expected Results

After implementing these fixes:
- Each task's completion state is isolated
- Tasks only restart based on their own completion
- No cross-contamination between tasks
- Completion states properly reset on task restart

## Monitoring

Add this to your monitoring routine:
```typescript
setInterval(() => {
  const states = taskCompletionManager.getAllCompletionStates();
  console.log(`[Monitor] Active completion states: ${states.size}`);
  console.log(`[Monitor] Active tasks: ${activeTasks.size}`);
  
  // Warn if we have completion states for non-existent tasks
  states.forEach((_, taskId) => {
    if (!activeTasks.has(taskId)) {
      console.warn(`[Monitor] Found orphaned completion state for task ${taskId}`);
      taskCompletionManager.cleanupTask(taskId);
    }
  });
}, 60000); // Every minute
```