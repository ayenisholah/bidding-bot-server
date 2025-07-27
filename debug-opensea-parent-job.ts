#!/usr/bin/env ts-node

/**
 * Debug script to test OpenSea parent job tracking
 * 
 * This script simulates the parent job creation and tracking flow
 * to identify why OpenSea trait bid jobs aren't updating the parent job counter
 */

import { config } from 'dotenv';
config();

// Simulate the parent job structure
interface BulkJobParent {
  parentId: string;
  totalJobs: number;
  completedJobs: number;
  failedJobs: number;
  startTime: number;
  taskId: string;
  jobName: string;
}

// Create a Map to simulate parentJobs
const parentJobs = new Map<string, BulkJobParent>();

// Simulate completeCollectionBidJob function
function completeCollectionBidJob(taskId: string, marketplace: string, success: boolean = true) {
  const parentId = `${taskId}-${marketplace}`.toLowerCase();
  let parentJob = parentJobs.get(parentId);

  console.log(`\n[${marketplace}] completeCollectionBidJob called`);
  console.log(`  - taskId: ${taskId}`);
  console.log(`  - parentId: ${parentId}`);
  console.log(`  - success: ${success}`);
  console.log(`  - parentJob exists: ${!!parentJob}`);
  console.log(`  - parentJobs Map size: ${parentJobs.size}`);
  console.log(`  - All keys in Map: ${Array.from(parentJobs.keys()).join(', ')}`);

  if (!parentJob) {
    console.log(`  - WARNING: Parent job not found!`);
    parentJob = {
      parentId: parentId,
      totalJobs: 1,
      completedJobs: 0,
      failedJobs: 0,
      startTime: Date.now(),
      taskId: taskId,
      jobName: `${marketplace.toUpperCase()}_COLLECTION_BID`
    };
    parentJobs.set(parentId, parentJob);
    console.log(`  - Created new parent job`);
  }

  if (success) {
    parentJob.completedJobs++;
    console.log(`  - Job completed: ${parentJob.completedJobs}/${parentJob.totalJobs}`);
  } else {
    parentJob.failedJobs++;
    console.log(`  - Job failed: ${parentJob.failedJobs}/${parentJob.totalJobs}`);
  }

  // Ensure the parent job is saved back to the Map
  parentJobs.set(parentId, parentJob);
  console.log(`  - Parent job saved back to Map`);
}

// Test scenario 1: Normal flow
console.log('=== Test 1: Normal Flow ===');
const taskId1 = 'test-task-1';
const parentJobId1 = `${taskId1}-opensea`.toLowerCase();

// Create parent job (simulating processOpenseaScheduledBid)
parentJobs.set(parentJobId1, {
  parentId: parentJobId1,
  totalJobs: 5,
  completedJobs: 0,
  failedJobs: 0,
  startTime: Date.now(),
  taskId: taskId1,
  jobName: 'OPENSEA_TRAIT_BID',
});
console.log(`Created parent job: ${parentJobId1} with 5 total jobs`);

// Simulate completing jobs
for (let i = 1; i <= 5; i++) {
  completeCollectionBidJob(taskId1, 'opensea', true);
}

// Test scenario 2: Case sensitivity issue
console.log('\n\n=== Test 2: Case Sensitivity ===');
const taskId2 = 'test-task-2';
const parentJobId2 = `${taskId2}-OPENSEA`.toLowerCase(); // Note: OPENSEA in caps

// Create parent job with different case
parentJobs.set(parentJobId2, {
  parentId: parentJobId2,
  totalJobs: 3,
  completedJobs: 0,
  failedJobs: 0,
  startTime: Date.now(),
  taskId: taskId2,
  jobName: 'OPENSEA_TRAIT_BID',
});
console.log(`Created parent job: ${parentJobId2} with 3 total jobs`);

// Try to complete with lowercase
completeCollectionBidJob(taskId2, 'opensea', true);

// Test scenario 3: processBulkJobs simulation
console.log('\n\n=== Test 3: ProcessBulkJobs Simulation ===');
const taskId3 = 'test-task-3';
const parentJobId3 = `${taskId3}-opensea`.toLowerCase();

// First create in processOpenseaScheduledBid
parentJobs.set(parentJobId3, {
  parentId: parentJobId3,
  totalJobs: 10,
  completedJobs: 0,
  failedJobs: 0,
  startTime: Date.now(),
  taskId: taskId3,
  jobName: 'OPENSEA_TRAIT_BID',
});
console.log(`[processOpenseaScheduledBid] Created parent job: ${parentJobId3} with 10 total jobs`);

// Simulate processBulkJobs
const marketplace = 'OPENSEA_TRAIT_BID'.split('_')[0]; // Should be 'OPENSEA'
const bulkJobId = `${taskId3}-${marketplace}`.toLowerCase();
console.log(`\n[processBulkJobs] marketplace: ${marketplace}`);
console.log(`[processBulkJobs] bulkJobId: ${bulkJobId}`);
console.log(`[processBulkJobs] bulkJobId === parentJobId3: ${bulkJobId === parentJobId3}`);

if (!parentJobs.has(bulkJobId)) {
  console.log(`[processBulkJobs] WARNING: Would create new parent job!`);
} else {
  console.log(`[processBulkJobs] Parent job already exists, not overwriting`);
}

// Try to complete jobs
completeCollectionBidJob(taskId3, 'opensea', true);

// Final summary
console.log('\n\n=== Final Summary ===');
console.log('All parent jobs in Map:');
parentJobs.forEach((job, key) => {
  console.log(`  ${key}: ${job.completedJobs}/${job.totalJobs} completed, ${job.failedJobs} failed`);
});