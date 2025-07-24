# validateBidCount Function Optimization

## Current Implementation Issues

The current `validateBidCount` function runs every 5 seconds and makes 4 separate database queries per task:
- 3 queries for each marketplace (OpenSea, MagicEden, Blur)
- 1 query for expiring orders

With 10 active tasks, this results in 40 database queries every 5 seconds (480 queries/minute).

## Optimized Implementation

```typescript
async function validateBidCount() {
  // Early exit if no tasks
  if (currentTasks.length === 0) {
    return;
  }

  try {
    const taskIds = currentTasks.map(task => task._id);
    const now = new Date();
    const oneMinuteFromNow = new Date(Date.now() + 10 * 1000);
    
    // Batch process all tasks in a single query using Prisma's groupBy
    const [orderCounts, expiringOrders] = await Promise.all([
      // Get counts for all tasks grouped by taskId and marketplace
      prisma.order.groupBy({
        by: ['taskId', 'marketplace'],
        where: {
          taskId: { in: taskIds },
          expiresAt: { gt: now }
        },
        _count: {
          _all: true
        }
      }),
      
      // Get all expiring orders across all tasks
      prisma.order.findMany({
        where: {
          taskId: { in: taskIds },
          expiresAt: { lte: oneMinuteFromNow }
        },
        select: {
          taskId: true,
          id: true
        }
      })
    ]);
    
    // Process order counts
    const bidStatsMap: { [taskId: string]: { opensea: number; magiceden: number; blur: number } } = {};
    
    // Initialize all tasks with zero counts
    for (const task of currentTasks) {
      bidStatsMap[task._id] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }
    
    // Populate counts from grouped results
    for (const group of orderCounts) {
      const marketplace = group.marketplace.toLowerCase();
      if (bidStatsMap[group.taskId] && (marketplace === 'opensea' || marketplace === 'magiceden' || marketplace === 'blur')) {
        bidStatsMap[group.taskId][marketplace as 'opensea' | 'magiceden' | 'blur'] = group._count._all;
      }
    }
    
    // Update global bidStats
    Object.assign(bidStats, bidStatsMap);
    
    // Process expiring orders by task
    if (expiringOrders.length > 0) {
      const tasksToRestart = new Set<string>();
      const orderIdsToDelete: string[] = [];
      
      for (const order of expiringOrders) {
        tasksToRestart.add(order.taskId);
        orderIdsToDelete.push(order.id);
      }
      
      // Batch delete expiring orders
      await prisma.order.deleteMany({
        where: {
          id: { in: orderIdsToDelete }
        }
      });
      
      // Restart affected tasks
      const restartPromises = Array.from(tasksToRestart).map(taskId => {
        const task = currentTasks.find(t => t._id === taskId);
        if (task) {
          return startTask(task, true);
        }
        return null;
      }).filter(Boolean);
      
      await Promise.allSettled(restartPromises);
    }
    
  } catch (error: any) {
    // Handle specific Prisma errors
    if (error?.code === 'P2021') {
      console.log(YELLOW + `Order table doesn't exist. Run 'npx prisma migrate dev' to create it.` + RESET);
      // Set all bid stats to 0 when table doesn't exist
      for (const task of currentTasks) {
        bidStats[task._id] = {
          opensea: 0,
          magiceden: 0,
          blur: 0
        };
      }
    } else {
      await logger.logError(ErrorCategory.DATABASE, 'Failed to validate bid counts', error);
      console.error(RED + `Error validating bid counts` + RESET, error);
    }
  }
}
```

## Benefits of Optimization

1. **Reduced Database Queries**: From 4 queries per task to 2 queries total
   - 10 tasks: 40 queries → 2 queries (95% reduction)
   - 50 tasks: 200 queries → 2 queries (99% reduction)

2. **Better Performance**:
   - Single grouped query for all bid counts
   - Batch processing of expiring orders
   - Parallel execution with Promise.all

3. **Improved Error Handling**:
   - Centralized error handling
   - Graceful fallback when table doesn't exist

4. **Early Exit Optimization**:
   - Skip processing when no tasks are active

5. **Memory Efficiency**:
   - Process results in batches
   - No unnecessary intermediate arrays

## Implementation Notes

- Uses Prisma's `groupBy` for efficient aggregation
- Maintains the same external behavior
- All task restarts are handled concurrently
- Error logging is improved with proper categorization