#!/usr/bin/env node
/* eslint-disable */

import mongoose from 'mongoose';
import Task from './src/models/task.model';
import { config } from 'dotenv';
import { Queue } from 'bullmq';
import { createBullMQConnection } from './src/utils/redis';

config();

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/BIDDING_BOT';

// Color codes for output
const RED = '\x1b[31m';
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const BLUE = '\x1b[34m';
const MAGENTA = '\x1b[35m';
const RESET = '\x1b[0m';

async function connectToMongoDB() {
  try {
    await mongoose.connect(MONGODB_URI);
    console.log(GREEN + '✅ Connected to MongoDB' + RESET);
  } catch (error) {
    console.error(RED + '❌ MongoDB connection error:' + RESET, error);
    process.exit(1);
  }
}

async function monitorQueue() {
  const connection = createBullMQConnection();
  const queue = new Queue('BiddingQueue', { connection });
  
  console.log(BLUE + '\n📊 Queue Monitoring Started\n' + RESET);
  
  // Monitor for 5 loops (adjust as needed)
  let loopCount = 0;
  const maxLoops = 5;
  
  const checkInterval = setInterval(async () => {
    try {
      // Get queue stats
      const waiting = await queue.getWaitingCount();
      const active = await queue.getActiveCount();
      const completed = await queue.getCompletedCount();
      const failed = await queue.getFailedCount();
      
      // Get OpenSea trait bid jobs specifically
      const jobs = await queue.getJobs(['waiting', 'active', 'completed', 'failed']);
      const openseaTraitJobs = jobs.filter(job => job.name === 'OPENSEA_TRAIT_BID');
      const magicedenTraitJobs = jobs.filter(job => job.name === 'MAGICEDEN_TRAIT_BID');
      
      // Group by task ID
      const openseaByTask = new Map<string, { waiting: number, active: number, completed: number, failed: number }>();
      const magicedenByTask = new Map<string, { waiting: number, active: number, completed: number, failed: number }>();
      
      for (const job of openseaTraitJobs) {
        const taskId = job.data._id;
        const state = await job.getState();
        
        if (!openseaByTask.has(taskId)) {
          openseaByTask.set(taskId, { waiting: 0, active: 0, completed: 0, failed: 0 });
        }
        
        const stats = openseaByTask.get(taskId)!;
        if (state === 'waiting') stats.waiting++;
        else if (state === 'active') stats.active++;
        else if (state === 'completed') stats.completed++;
        else if (state === 'failed') stats.failed++;
      }
      
      for (const job of magicedenTraitJobs) {
        const taskId = job.data._id;
        const state = await job.getState();
        
        if (!magicedenByTask.has(taskId)) {
          magicedenByTask.set(taskId, { waiting: 0, active: 0, completed: 0, failed: 0 });
        }
        
        const stats = magicedenByTask.get(taskId)!;
        if (state === 'waiting') stats.waiting++;
        else if (state === 'active') stats.active++;
        else if (state === 'completed') stats.completed++;
        else if (state === 'failed') stats.failed++;
      }
      
      // Print stats
      console.log(YELLOW + '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━' + RESET);
      console.log(BLUE + `⏰ Time: ${new Date().toLocaleTimeString()} | Loop Check #${loopCount + 1}` + RESET);
      console.log(YELLOW + '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━' + RESET);
      
      console.log(MAGENTA + '\n📊 Overall Queue Stats:' + RESET);
      console.log(`  Waiting: ${waiting} | Active: ${active} | Completed: ${completed} | Failed: ${failed}`);
      
      if (openseaByTask.size > 0) {
        console.log(GREEN + '\n🌊 OpenSea Trait Bid Jobs by Task:' + RESET);
        for (const [taskId, stats] of openseaByTask) {
          const total = stats.waiting + stats.active + stats.completed + stats.failed;
          const percentComplete = total > 0 ? ((stats.completed + stats.failed) / total * 100).toFixed(1) : '0.0';
          
          console.log(`  Task ${taskId}:`);
          console.log(`    Total: ${total} | Waiting: ${stats.waiting} | Active: ${stats.active} | Completed: ${stats.completed} | Failed: ${stats.failed}`);
          console.log(`    Progress: ${percentComplete}%`);
          
          // Check for the issue - jobs added but showing 0 waiting
          if (total > 0 && stats.waiting === 0 && stats.active === 0 && stats.completed + stats.failed < total) {
            console.log(RED + '    ⚠️  ISSUE DETECTED: Jobs seem to be missing!' + RESET);
          }
        }
      }
      
      if (magicedenByTask.size > 0) {
        console.log(BLUE + '\n✨ MagicEden Trait Bid Jobs by Task:' + RESET);
        for (const [taskId, stats] of magicedenByTask) {
          const total = stats.waiting + stats.active + stats.completed + stats.failed;
          const percentComplete = total > 0 ? ((stats.completed + stats.failed) / total * 100).toFixed(1) : '0.0';
          
          console.log(`  Task ${taskId}:`);
          console.log(`    Total: ${total} | Waiting: ${stats.waiting} | Active: ${stats.active} | Completed: ${stats.completed} | Failed: ${stats.failed}`);
          console.log(`    Progress: ${percentComplete}%`);
        }
      }
      
      // Get active tasks to see loop iterations
      const tasks = await Task.find({ running: true, selectedMarketplaces: { $in: ['OPENSEA'] } }).limit(5);
      if (tasks.length > 0) {
        console.log(YELLOW + '\n📋 Active Tasks with OpenSea:' + RESET);
        for (const task of tasks) {
          console.log(`  ${task._id}: ${task.contract.slug} | Loop Interval: ${task.loopInterval}s`);
        }
      }
      
      loopCount++;
      
      // Stop after maxLoops checks
      if (loopCount >= maxLoops) {
        console.log(GREEN + '\n✅ Monitoring complete!' + RESET);
        clearInterval(checkInterval);
        await queue.close();
        await mongoose.disconnect();
        process.exit(0);
      }
      
    } catch (error) {
      console.error(RED + 'Error checking queue:' + RESET, error);
    }
  }, 30000); // Check every 30 seconds
  
  console.log(YELLOW + `\n🔍 Monitoring queue for ${maxLoops} checks (every 30 seconds)...` + RESET);
  console.log(YELLOW + 'Focus on OpenSea trait bid jobs to verify they execute properly on all loops.' + RESET);
}

async function main() {
  try {
    await connectToMongoDB();
    await monitorQueue();
  } catch (error) {
    console.error(RED + 'Script error:' + RESET, error);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log(YELLOW + '\n\n⚡ Shutting down gracefully...' + RESET);
  await mongoose.disconnect();
  process.exit(0);
});

main();