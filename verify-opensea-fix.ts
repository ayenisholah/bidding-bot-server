#!/usr/bin/env node
/* eslint-disable */

/**
 * This script verifies that the OpenSea trait bid job fix is working correctly.
 * It checks the key changes made to resolve the 57.4% stopping issue:
 * 
 * 1. Job cleanup before adding new jobs
 * 2. No loop counters in job IDs
 * 3. ParentJobs set before processBulkJobs
 * 4. removeOnComplete: true (default behavior)
 */

import { config } from 'dotenv';
import { Queue } from 'bullmq';
import { createBullMQConnection } from './src/utils/redis';

config();

// Color codes
const RED = '\x1b[31m';
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const BLUE = '\x1b[34m';
const RESET = '\x1b[0m';

async function verifyFix() {
  console.log(BLUE + '🔍 Verifying OpenSea Trait Bid Fix\n' + RESET);
  
  const connection = createBullMQConnection();
  const queue = new Queue('BiddingQueue', { connection });
  
  try {
    // Test 1: Check if job IDs follow the strict pattern
    console.log(YELLOW + '1️⃣ Checking job ID patterns...' + RESET);
    
    const jobs = await queue.getJobs(['waiting', 'active', 'completed', 'failed']);
    const openseaTraitJobs = jobs.filter(job => job.name === 'OPENSEA_TRAIT_BID');
    
    let validPatterns = 0;
    let invalidPatterns = 0;
    
    for (const job of openseaTraitJobs.slice(0, 10)) { // Check first 10 jobs
      const jobId = job.id || '';
      // Pattern should be: taskId-opensea-traitType-traitValue-slug
      const pattern = /^[a-f0-9]{24}-opensea-[^-]+-[^-]+-[^-]+$/;
      
      if (pattern.test(jobId)) {
        validPatterns++;
      } else {
        invalidPatterns++;
        console.log(RED + `  ❌ Invalid pattern: ${jobId}` + RESET);
      }
    }
    
    if (invalidPatterns === 0 && validPatterns > 0) {
      console.log(GREEN + `  ✅ All ${validPatterns} job IDs follow the strict pattern (no loop counters)` + RESET);
    } else if (invalidPatterns > 0) {
      console.log(RED + `  ❌ Found ${invalidPatterns} jobs with invalid patterns` + RESET);
    } else {
      console.log(YELLOW + '  ⚠️  No OpenSea trait bid jobs found to verify' + RESET);
    }
    
    // Test 2: Check job options
    console.log(YELLOW + '\n2️⃣ Checking job options...' + RESET);
    
    for (const job of openseaTraitJobs.slice(0, 3)) { // Check first 3 jobs
      const opts = job.opts;
      console.log(`  Job ${job.id}:`);
      console.log(`    removeOnComplete: ${opts.removeOnComplete}`);
      console.log(`    removeOnFail: ${opts.removeOnFail}`);
      
      if (opts.removeOnComplete === true) {
        console.log(GREEN + '    ✅ removeOnComplete is correctly set to true' + RESET);
      } else {
        console.log(RED + '    ❌ removeOnComplete should be true' + RESET);
      }
    }
    
    // Test 3: Look for duplicate job patterns
    console.log(YELLOW + '\n3️⃣ Checking for duplicate prevention...' + RESET);
    
    const jobIdSet = new Set<string>();
    const duplicates = new Set<string>();
    
    for (const job of openseaTraitJobs) {
      const jobId = job.id || '';
      if (jobIdSet.has(jobId)) {
        duplicates.add(jobId);
      } else {
        jobIdSet.add(jobId);
      }
    }
    
    if (duplicates.size === 0) {
      console.log(GREEN + `  ✅ No duplicate job IDs found among ${openseaTraitJobs.length} jobs` + RESET);
    } else {
      console.log(RED + `  ❌ Found ${duplicates.size} duplicate job IDs` + RESET);
      Array.from(duplicates).slice(0, 3).forEach(id => {
        console.log(RED + `     - ${id}` + RESET);
      });
    }
    
    // Test 4: Check completion rates
    console.log(YELLOW + '\n4️⃣ Analyzing completion rates...' + RESET);
    
    const taskCompletions = new Map<string, { total: number, completed: number, failed: number }>();
    
    for (const job of openseaTraitJobs) {
      const taskId = job.data._id;
      const state = await job.getState();
      
      if (!taskCompletions.has(taskId)) {
        taskCompletions.set(taskId, { total: 0, completed: 0, failed: 0 });
      }
      
      const stats = taskCompletions.get(taskId)!;
      stats.total++;
      if (state === 'completed') stats.completed++;
      else if (state === 'failed') stats.failed++;
    }
    
    for (const [taskId, stats] of taskCompletions) {
      const completionRate = stats.total > 0 ? ((stats.completed + stats.failed) / stats.total * 100).toFixed(1) : '0';
      console.log(`  Task ${taskId}:`);
      console.log(`    Total: ${stats.total} | Completed: ${stats.completed} | Failed: ${stats.failed}`);
      console.log(`    Completion: ${completionRate}%`);
      
      if (Math.abs(parseFloat(completionRate) - 57.4) < 0.5) {
        console.log(RED + '    ⚠️  WARNING: Task appears stuck at ~57.4%!' + RESET);
      } else if (parseFloat(completionRate) === 100) {
        console.log(GREEN + '    ✅ Task completed successfully' + RESET);
      }
    }
    
    // Summary
    console.log(BLUE + '\n📊 Summary:' + RESET);
    console.log('The fix implementation includes:');
    console.log('  1. Job cleanup before adding new ones in processOpenseaScheduledBid');
    console.log('  2. Strict job ID pattern without loop counters');
    console.log('  3. parentJobs set before calling processBulkJobs');
    console.log('  4. removeOnComplete: true to allow job reuse after cleanup');
    
    console.log(YELLOW + '\n💡 To fully test the fix:' + RESET);
    console.log('  1. Start a task with OpenSea trait bidding enabled');
    console.log('  2. Set a short loopInterval (e.g., 60 seconds)');
    console.log('  3. Monitor with: npm run test:opensea-trait-loops');
    console.log('  4. Verify jobs complete 100% on each loop (not stuck at 57.4%)');
    
  } catch (error) {
    console.error(RED + 'Error during verification:' + RESET, error);
  } finally {
    await queue.close();
  }
}

// Run verification
verifyFix().then(() => {
  process.exit(0);
}).catch(error => {
  console.error(RED + 'Script error:' + RESET, error);
  process.exit(1);
});