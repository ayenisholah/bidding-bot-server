# Queue Optimization Integration Guide

This guide demonstrates how to integrate the optimized queue event handling into the main codebase.

## Step 1: Import Dependencies

Add these imports to the top of your `index.ts` file:

```typescript
// Import the utility functions
import { parseJobId, getOrInitTaskStatus } from "./utils/queueUtils";

// Import the handler functions
import {
	processCancellationJob,
	processRegularJob,
	scheduleNextRun,
} from "./utils/queueHandlers";
```

## Step 2: Replace Queue Event Handlers

Replace the existing queue event handlers with these optimized versions:

```typescript
// Main event handlers for the queue
queueEvents.on("completed", async ({ jobId }) => {
	try {
		const jobInfo = parseJobId(jobId);
		if (!jobInfo) return;

		if (jobInfo.isCancelJob) {
			await processCancellationJob(
				jobInfo,
				true, // isCompleted = true for 'completed' event
				taskStatus,
				activeTasks,
				parentJobs,
				Wallet,
				cancelAllRelatedBids
			);
		} else {
			await processRegularJob(
				jobInfo,
				true, // isCompleted = true for 'completed' event
				taskStatus,
				activeTasks,
				parentJobs,
				completedTasks,
				enabledMarketplaces,
				scheduleNextRun,
				taskTimeouts,
				startTask,
				stopTask,
				getExpiry
			);
		}
	} catch (error) {
		console.error(RED + "Error in completed event handler:" + RESET, error);
	}
});

queueEvents.on("failed", async ({ jobId }) => {
	try {
		const jobInfo = parseJobId(jobId);
		if (!jobInfo) return;

		if (jobInfo.isCancelJob) {
			await processCancellationJob(
				jobInfo,
				false, // isCompleted = false for 'failed' event
				taskStatus,
				activeTasks,
				parentJobs,
				Wallet,
				cancelAllRelatedBids
			);
		} else {
			await processRegularJob(
				jobInfo,
				false, // isCompleted = false for 'failed' event
				taskStatus,
				activeTasks,
				parentJobs,
				completedTasks,
				enabledMarketplaces,
				scheduleNextRun,
				taskTimeouts,
				startTask,
				stopTask,
				getExpiry
			);
		}
	} catch (error) {
		console.error(RED + "Error in failed event handler:" + RESET, error);
	}
});
```

## Step 3: Update Related Code

If any code directly references the internal structure of the event handlers, update it to use the new utility functions instead.

## Complete Implementation Example

Here's a complete example of how your code should look after the update:

```typescript
// At the top of the file with other imports
import { parseJobId, getOrInitTaskStatus } from "./utils/queueUtils";
import {
	processCancellationJob,
	processRegularJob,
	scheduleNextRun,
} from "./utils/queueHandlers";

// ... existing code ...

interface CompletedTasks {
	[key: string]: {
		opensea: boolean;
		magiceden: boolean;
		blur: boolean;
	};
}
const completedTasks: CompletedTasks = {};

// Main event handlers for the queue
queueEvents.on("completed", async ({ jobId }) => {
	try {
		const jobInfo = parseJobId(jobId);
		if (!jobInfo) return;

		if (jobInfo.isCancelJob) {
			await processCancellationJob(
				jobInfo,
				true,
				taskStatus,
				activeTasks,
				parentJobs,
				Wallet,
				cancelAllRelatedBids
			);
		} else {
			await processRegularJob(
				jobInfo,
				true,
				taskStatus,
				activeTasks,
				parentJobs,
				completedTasks,
				enabledMarketplaces,
				scheduleNextRun,
				taskTimeouts,
				startTask,
				stopTask,
				getExpiry
			);
		}
	} catch (error) {
		console.error(RED + "Error in completed event handler:" + RESET, error);
	}
});

queueEvents.on("failed", async ({ jobId }) => {
	try {
		const jobInfo = parseJobId(jobId);
		if (!jobInfo) return;

		if (jobInfo.isCancelJob) {
			await processCancellationJob(
				jobInfo,
				false,
				taskStatus,
				activeTasks,
				parentJobs,
				Wallet,
				cancelAllRelatedBids
			);
		} else {
			await processRegularJob(
				jobInfo,
				false,
				taskStatus,
				activeTasks,
				parentJobs,
				completedTasks,
				enabledMarketplaces,
				scheduleNextRun,
				taskTimeouts,
				startTask,
				stopTask,
				getExpiry
			);
		}
	} catch (error) {
		console.error(RED + "Error in failed event handler:" + RESET, error);
	}
});

// ... rest of your code ...
```

## Testing the Changes

After implementing these changes, you should test the following scenarios:

1. **Regular Job Completion**

   - Create and run a normal bidding task
   - Verify it completes successfully and updates the task status correctly

2. **Cancellation Job Handling**

   - Trigger cancellation of a bid
   - Verify the cancellation percentage is calculated correctly
   - Check that all related bids are cancelled properly

3. **Failure Handling**

   - Simulate a job failure
   - Verify the failure is handled gracefully
   - Check that the task status is updated appropriately

4. **Timer Cleanup**

   - Start and stop tasks several times
   - Monitor memory usage to ensure timers are being properly cleaned up

5. **Performance Measurement**
   - Measure the execution time of the new handlers versus the old ones
   - Verify reduced memory usage

## Troubleshooting

If you encounter issues with the integration:

1. Check that all dependencies are correctly imported
2. Verify that the utility functions are receiving the correct parameters
3. Make sure any references to the internal event handler logic are updated
4. Check the console for error messages from the new try/catch blocks
