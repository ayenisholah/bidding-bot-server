/**
 * Quick diagnostic tool to check for duplicate task IDs
 * Run this in your index.ts to verify the issue
 */

export function checkForDuplicateTasks(currentTasks: any[]): void {
  // console.log('\n=== TASK DUPLICATION DIAGNOSTIC ===');
  
  // Count occurrences of each task ID
  const taskIdCounts = new Map<string, number>();
  const taskDetails = new Map<string, any[]>();
  
  currentTasks.forEach((task, index) => {
    const id = task._id.toString();
    taskIdCounts.set(id, (taskIdCounts.get(id) || 0) + 1);
    
    if (!taskDetails.has(id)) {
      taskDetails.set(id, []);
    }
    taskDetails.get(id)!.push({
      index,
      name: task.name || task.contract?.slug || 'Unknown',
      running: task.running,
      marketplaces: task.selectedMarketplaces
    });
  });
  
  // Report findings
  // console.log(`Total tasks in currentTasks array: ${currentTasks.length}`);
  // console.log(`Unique task IDs: ${taskIdCounts.size}`);
  
  let duplicatesFound = false;
  taskIdCounts.forEach((count, id) => {
    if (count > 1) {
      duplicatesFound = true;
      console.error(`\n❌ DUPLICATE FOUND: Task ID ${id} appears ${count} times!`);
      const details = taskDetails.get(id) || [];
      details.forEach(detail => {
        // console.log(`   - Index ${detail.index}: ${detail.name} (running: ${detail.running}, markets: ${detail.marketplaces.join(', ')})`);
      });
    }
  });
  
  if (!duplicatesFound) {
    // console.log('✅ No duplicate task IDs found');
  } else {
    // console.log('\n⚠️  DUPLICATES DETECTED - This will cause state management issues!');
  }
  
  // console.log('=== END DIAGNOSTIC ===\n');
}

/**
 * Add this function call in your index.ts at these locations:
 * 
 * 1. After fetchCurrentTasks() completes:
 *    checkForDuplicateTasks(currentTasks);
 * 
 * 2. After processNewTask() runs:
 *    checkForDuplicateTasks(currentTasks);
 * 
 * 3. In your broadcastBidRates() function to monitor over time:
 *    if (Date.now() % 60000 < 1000) { // Check once per minute
 *      checkForDuplicateTasks(currentTasks);
 *    }
 */