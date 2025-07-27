import axios from 'axios';

const SERVER_URL = 'http://localhost:3003';

async function checkSchedulingState() {
  try {
    const response = await axios.get(`${SERVER_URL}/debug/scheduling`);
    const data = response.data;
    
    console.log('\n=== SCHEDULING DEBUG INFO ===');
    console.log(`Timestamp: ${data.timestamp}`);
    console.log('\nSummary:');
    console.log(`- Total Locks: ${data.summary.totalLocks}`);
    console.log(`- Active Locks: ${data.summary.activeLocks}`);
    console.log(`- Stale Locks (>5min): ${data.summary.staleLocks}`);
    console.log(`- Total Loop Counters: ${data.summary.totalLoopCounters}`);
    console.log(`- Active Tasks: ${data.summary.totalActiveTasks}`);
    console.log(`- Running Tasks: ${data.summary.runningTasks}`);
    
    if (data.lockStates.length > 0) {
      console.log('\nLock States:');
      data.lockStates.forEach((lock: any) => {
        console.log(`  Task ${lock.taskId}: locked=${lock.locked}, age=${lock.age}s`);
      });
    }
    
    if (data.loopCounters.length > 0) {
      console.log('\nLoop Counters:');
      data.loopCounters.forEach((counter: any) => {
        console.log(`  Task ${counter.taskId}: loop #${counter.loopCount}`);
      });
    }
    
    if (data.activeTasks.length > 0) {
      console.log('\nActive Tasks:');
      data.activeTasks.forEach((task: any) => {
        console.log(`  Task ${task.taskId}: running=${task.running}, status=${task.status}, loopInterval=${JSON.stringify(task.loopInterval)}`);
      });
    }
    
    // Check for potential issues
    if (data.summary.staleLocks > 0) {
      console.log('\n⚠️  WARNING: Found stale locks that may prevent scheduling!');
    }
    
    // Check if any tasks have high loop counts but no recent activity
    const highLoopTasks = data.loopCounters.filter((c: any) => c.loopCount > 10);
    if (highLoopTasks.length > 0) {
      console.log(`\n✅ Found tasks with loop count > 10: ${highLoopTasks.map((t: any) => t.taskId).join(', ')}`);
      console.log('   This indicates infinite loop execution is working!');
    }
    
  } catch (error) {
    console.error('Error fetching scheduling debug info:', error);
  }
}

// Check every 30 seconds
console.log('Starting infinite loop monitoring...');
console.log('This will check the scheduling state every 30 seconds.');
console.log('Look for tasks with increasing loop counts to verify infinite execution.');
console.log('Press Ctrl+C to stop.\n');

checkSchedulingState();
setInterval(checkSchedulingState, 30000);