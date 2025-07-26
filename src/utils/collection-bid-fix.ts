// Fix for collection bid status management

// The issue:
// 1. Collection bids set status to RUNNING immediately when scheduled
// 2. This happens BEFORE the actual bid job is processed
// 3. processBulkJobs properly manages QUEUEING->RUNNING transition for trait/token bids
// 4. Collection bids bypass this flow, causing inconsistent status

// Current flow for collection bids:
// - Schedule job runs → sets parentJobs → adds bid job → sets status=RUNNING
// - Bid job runs → completes → updates percentage to 100%
// - Status stays at RUNNING (never was QUEUEING)

// Current flow for trait/token bids:
// - Schedule job runs → calls processBulkJobs
// - processBulkJobs: sets status=QUEUEING → adds all jobs → sets status=RUNNING
// - Jobs run → complete → update percentages

// The fix:
// 1. Remove immediate status=RUNNING for collection bids in schedule functions
// 2. Set status=QUEUEING when adding collection bid job
// 3. Set status=RUNNING when collection bid job starts processing
// 4. This makes collection bids consistent with trait/token bid flow

// Changes needed in src/index.ts:

// 1. In processOpenseaScheduledBid (around line 9062):
//    Remove: taskStatus[currentTask._id].status = 'RUNNING'
//    Add: taskStatus[currentTask._id].status = 'QUEUEING'

// 2. In processBlurScheduledBid (around line 9526):
//    Remove: taskStatus[currentTask._id].status = 'RUNNING'
//    Add: taskStatus[currentTask._id].status = 'QUEUEING'

// 3. In processMagicedenScheduledBid (around line 11491):
//    Remove: taskStatus[currentTask._id].status = 'RUNNING'
//    Add: taskStatus[currentTask._id].status = 'QUEUEING'

// 4. In processOpenseaCollectionBid, processBlurCollectionBid, processMagicedenCollectionBid:
//    Add at the beginning (after checking task is running):
//    if (taskStatus[taskId]?.status === 'QUEUEING') {
//      taskStatus[taskId].status = 'RUNNING';
//    }

export const COLLECTION_BID_STATUS_FIX = {
  description: "Fix collection bid status management to be consistent with trait/token bids",
  
  changes: [
    {
      file: "src/index.ts",
      function: "processOpenseaScheduledBid",
      line: "~9062",
      change: "Replace taskStatus[currentTask._id].status = 'RUNNING' with 'QUEUEING'"
    },
    {
      file: "src/index.ts", 
      function: "processBlurScheduledBid",
      line: "~9526",
      change: "Replace taskStatus[currentTask._id].status = 'RUNNING' with 'QUEUEING'"
    },
    {
      file: "src/index.ts",
      function: "processMagicedenScheduledBid", 
      line: "~11491",
      change: "Replace taskStatus[currentTask._id].status = 'RUNNING' with 'QUEUEING'"
    },
    {
      file: "src/index.ts",
      functions: ["processOpenseaCollectionBid", "processBlurCollectionBid", "processMagicedenCollectionBid"],
      change: "Add status transition from QUEUEING to RUNNING at the start of processing"
    }
  ]
};