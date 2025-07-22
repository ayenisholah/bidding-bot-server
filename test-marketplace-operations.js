// Test script for marketplace-specific operations

const scenarios = [
  {
    name: "Marketplace-specific stop while RUNNING",
    initial: { 
      status: 'RUNNING', 
      runningPercentage: 75, 
      nextLoopTime: new Date(Date.now() + 300000),
      waitingTime: 300
    },
    operation: "stopTask(task, false, 'opensea')",
    expected: {
      during: { status: 'CANCELLING', previousState: 'RUNNING' },
      after: { 
        status: 'RUNNING', 
        runningPercentage: 75, // Preserved
        nextLoopTime: 'preserved',
        waitingTime: 'preserved'
      }
    }
  },
  {
    name: "Marketplace-specific stop while SCHEDULED",
    initial: { 
      status: 'SCHEDULED', 
      runningPercentage: 0, 
      nextLoopTime: new Date(Date.now() + 600000),
      waitingTime: 600
    },
    operation: "stopTask(task, false, 'blur')",
    expected: {
      during: { status: 'CANCELLING', previousState: 'SCHEDULED' },
      after: { 
        status: 'SCHEDULED', 
        runningPercentage: 0,
        nextLoopTime: 'preserved',
        waitingTime: 'preserved'
      }
    }
  },
  {
    name: "Full task stop (no marketplace)",
    initial: { 
      status: 'RUNNING', 
      runningPercentage: 50,
      nextLoopTime: new Date(Date.now() + 300000),
      waitingTime: 300
    },
    operation: "stopTask(task, false)",
    expected: {
      during: { status: 'CANCELLING' },
      after: { 
        status: 'STOPPED', 
        runningPercentage: 0,
        nextLoopTime: 'deleted',
        waitingTime: 0
      }
    }
  },
  {
    name: "Marketplace-specific start while RUNNING",
    initial: { 
      status: 'RUNNING', 
      runningPercentage: 50,
      completedTasks: { opensea: true, magiceden: true, blur: false }
    },
    operation: "startTask(task, true, 'blur')",
    expected: {
      during: { status: 'RUNNING' }, // No status change
      after: { 
        status: 'RUNNING',
        completedTasks: { opensea: true, magiceden: true, blur: false } // Reset blur
      }
    }
  }
];

console.log('=== Marketplace-Specific Operations Test Scenarios ===\n');

scenarios.forEach((scenario, index) => {
  console.log(`${index + 1}. ${scenario.name}`);
  console.log(`   Initial State: ${JSON.stringify(scenario.initial)}`);
  console.log(`   Operation: ${scenario.operation}`);
  console.log(`   Expected During Cancellation: ${JSON.stringify(scenario.expected.during)}`);
  console.log(`   Expected After Completion: ${JSON.stringify(scenario.expected.after)}`);
  console.log('');
});

console.log('Key Implementation Points:');
console.log('1. Marketplace-specific operations preserve task state and progress');
console.log('2. previousState is stored and restored after cancellation');
console.log('3. nextLoopTime and waitingTime are preserved for marketplace operations');
console.log('4. Full task stop clears all state and transitions to STOPPED');
console.log('5. startTask with marketplace does not change task status');