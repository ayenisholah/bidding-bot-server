# Fix for Duplicate Task IDs Issue

## Problem
After 2-3 loops, tasks can accumulate duplicate entries in the `currentTasks` array, causing state management issues to persist even with the new thread-safe managers.

## Root Causes
1. `fetchCurrentTasks()` pushes to `currentTasks` without clearing it first
2. `processNewTask()` doesn't check for existing tasks before adding
3. No deduplication mechanism in place
4. Multiple task tracking collections can become inconsistent

## Solution

### 1. Update `fetchCurrentTasks()` (around line 916)

**Replace:**
```typescript
currentTasks.push(...formattedTasks);
```

**With:**
```typescript
// Clear existing tasks to prevent duplicates
TaskDeduplicationManager.resetTaskArray(currentTasks, formattedTasks);
```

### 2. Update `processNewTask()` (around line 2484)

**Replace:**
```typescript
currentTasks.push(task);
```

**With:**
```typescript
// Add task with duplicate prevention
TaskDeduplicationManager.addTaskToArray(currentTasks, task);
```

### 3. Add import at the top of `index.ts`

```typescript
import { TaskDeduplicationManager } from './utils/task-deduplication';
```

### 4. Add periodic consistency check (in server startup)

Add after server.listen():
```typescript
// Periodic task consistency check (every 5 minutes)
setInterval(() => {
  const { inconsistencies, duplicates } = TaskDeduplicationManager.verifyTaskConsistency(
    currentTasks,
    activeTasks,
    taskStatus
  );
  
  if (duplicates.length > 0) {
    console.error('[TaskConsistency] Found duplicates:', duplicates);
    // Fix duplicates
    const deduplicated = TaskDeduplicationManager.deduplicateTasks(currentTasks);
    currentTasks.length = 0;
    currentTasks.push(...deduplicated);
    updateTaskIndexes(currentTasks);
  }
  
  if (inconsistencies.length > 0) {
    console.warn('[TaskConsistency] Found inconsistencies:', inconsistencies);
    // Clean up orphaned data
    const cleaned = TaskDeduplicationManager.cleanupOrphanedData(
      currentTasks,
      activeTasks,
      taskStatus,
      taskTimeouts,
      taskCountdownTimers
    );
    console.log(`[TaskConsistency] Cleaned up ${cleaned} orphaned entries`);
  }
}, 5 * 60 * 1000); // 5 minutes
```

### 5. Update task removal logic

When removing tasks, ensure they're removed from ALL collections:
```typescript
function removeTaskCompletely(taskId: string) {
  // Remove from currentTasks
  const index = currentTasks.findIndex(t => t._id === taskId);
  if (index !== -1) {
    currentTasks.splice(index, 1);
  }
  
  // Remove from activeTasks
  activeTasks.delete(taskId);
  
  // Remove from taskStatus
  delete taskStatus[taskId];
  
  // Clear timers
  const countdownTimer = taskCountdownTimers.get(taskId);
  if (countdownTimer) {
    clearInterval(countdownTimer);
    taskCountdownTimers.delete(taskId);
  }
  
  const timeout = taskTimeouts.get(taskId);
  if (timeout) {
    clearTimeout(timeout);
    taskTimeouts.delete(taskId);
  }
  
  // Update indexes
  updateTaskIndexes(currentTasks);
}
```

## Testing

1. Start the server and create multiple tasks
2. Let them run for 3-4 loops
3. Check console for duplicate warnings
4. Verify task counts remain consistent
5. Monitor for state management issues

## Verification Commands

Add these debug endpoints to help verify the fix:

```typescript
app.get('/debug/task-consistency', (req, res) => {
  const { inconsistencies, duplicates } = TaskDeduplicationManager.verifyTaskConsistency(
    currentTasks,
    activeTasks,
    taskStatus
  );
  
  res.json({
    currentTasksCount: currentTasks.length,
    activeTasksCount: activeTasks.size,
    taskStatusCount: Object.keys(taskStatus).length,
    duplicates,
    inconsistencies,
    taskIds: currentTasks.map(t => t._id)
  });
});

app.get('/debug/deduplicate', (req, res) => {
  const before = currentTasks.length;
  const deduplicated = TaskDeduplicationManager.deduplicateTasks(currentTasks);
  currentTasks.length = 0;
  currentTasks.push(...deduplicated);
  updateTaskIndexes(currentTasks);
  
  res.json({
    before,
    after: currentTasks.length,
    removed: before - currentTasks.length
  });
});
```

## Expected Results

After implementing these fixes:
- No duplicate task IDs in `currentTasks`
- Consistent state across all task tracking collections
- State management issues should be fully resolved
- Tasks should complete and restart properly without accumulating duplicates