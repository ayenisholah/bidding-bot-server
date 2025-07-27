#!/usr/bin/env node
/* eslint-disable */

import mongoose from 'mongoose';
import { config } from 'dotenv';
import { Queue } from 'bullmq';
import { createBullMQConnection } from './src/utils/redis';
import redisClient from './src/utils/redis';

config();

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/BIDDING_BOT';

// Color codes for output
const RED = '\x1b[31m';
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const BLUE = '\x1b[34m';
const MAGENTA = '\x1b[35m';
const RESET = '\x1b[0m';

interface JobStats {
  taskId: string;
  loopNumber: number;
  timestamp: Date;
  totalJobs: number;
  waitingJobs: number;
  activeJobs: number;
  completedJobs: number;
  failedJobs: number;
  jobIds: string[];
}

const loopStats = new Map<string, JobStats[]>();

async function connectToServices() {
  try {
    await mongoose.connect(MONGODB_URI);
    console.log(GREEN + '✅ Connected to MongoDB' + RESET);
    
    await redisClient.initialize();
    console.log(GREEN + '✅ Connected to Redis' + RESET);
  } catch (error) {
    console.error(RED + '❌ Connection error:' + RESET, error);
    process.exit(1);
  }
}

async function checkOpenSeaTraitJobs() {
  const connection = createBullMQConnection();
  const queue = new Queue('BiddingQueue', { connection });
  const redis = redisClient.getClient();
  
  console.log(BLUE + '\n🔍 OpenSea Trait Bid Loop Testing\n' + RESET);
  
  let checkCount = 0;
  const maxChecks = 20; // Monitor for 10 minutes (20 * 30 seconds)
  
  const checkInterval = setInterval(async () => {
    try {
      checkCount++;
      console.log(YELLOW + `\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━` + RESET);
      console.log(BLUE + `Check #${checkCount} at ${new Date().toLocaleTimeString()}` + RESET);
      console.log(YELLOW + `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━` + RESET);
      
      // Get all jobs
      const allJobs = await queue.getJobs(['waiting', 'active', 'completed', 'failed']);
      
      // Filter OpenSea trait bid jobs
      const openseaTraitJobs = allJobs.filter(job => job.name === 'OPENSEA_TRAIT_BID');
      
      // Group by task ID and analyze
      const taskStats = new Map<string, { 
        waiting: string[], 
        active: string[], 
        completed: string[], 
        failed: string[],
        allJobIds: Set<string>
      }>();
      
      for (const job of openseaTraitJobs) {
        const taskId = job.data._id;
        const state = await job.getState();
        const jobId = job.id || 'unknown';
        
        if (!taskStats.has(taskId)) {
          taskStats.set(taskId, { 
            waiting: [], 
            active: [], 
            completed: [], 
            failed: [],
            allJobIds: new Set()
          });
        }
        
        const stats = taskStats.get(taskId)!;
        stats.allJobIds.add(jobId);
        
        if (state === 'waiting') stats.waiting.push(jobId);
        else if (state === 'active') stats.active.push(jobId);
        else if (state === 'completed') stats.completed.push(jobId);
        else if (state === 'failed') stats.failed.push(jobId);
      }
      
      // Check for loop counter in Redis
      if (redis) {
        console.log(MAGENTA + '\n📊 Task Loop Counters:' + RESET);
        const keys = await redis.keys('taskLoopCounter:*');
        for (const key of keys) {
          const value = await redis.get(key);
          const taskId = key.split(':')[1];
          console.log(`  ${taskId}: Loop #${value}`);
        }
      }
      
      // Analyze each task
      for (const [taskId, stats] of taskStats) {
        const total = stats.waiting.length + stats.active.length + stats.completed.length + stats.failed.length;
        const percentComplete = total > 0 ? ((stats.completed.length + stats.failed.length) / total * 100).toFixed(1) : '0.0';
        
        console.log(GREEN + `\n🎯 OpenSea Task ${taskId}:` + RESET);
        console.log(`  Total Jobs: ${total}`);
        console.log(`  Waiting: ${stats.waiting.length} | Active: ${stats.active.length} | Completed: ${stats.completed.length} | Failed: ${stats.failed.length}`);
        console.log(`  Progress: ${percentComplete}%`);
        
        // Detect the 57.4% issue
        if (percentComplete === '57.4' || (percentComplete === '57.3' || percentComplete === '57.5')) {
          console.log(RED + '  ⚠️  WARNING: Task appears to be stuck at ~57.4%!' + RESET);
        }
        
        // Check for jobs that were added but show 0 waiting (the main issue)
        if (total > 0 && stats.waiting.length === 0 && stats.active.length === 0 && percentComplete !== '100.0') {
          console.log(RED + '  ❌ ISSUE DETECTED: Jobs completed but new loop jobs not in queue!' + RESET);
          console.log(RED + '     This indicates the deduplication issue is occurring.' + RESET);
        }
        
        // Store stats for trend analysis
        if (!loopStats.has(taskId)) {
          loopStats.set(taskId, []);
        }
        
        // Try to determine loop number from Redis
        let loopNumber = 1;
        if (redis) {
          const loopCounterValue = await redis.get(`taskLoopCounter:${taskId}`);
          if (loopCounterValue) {
            loopNumber = parseInt(loopCounterValue) || 1;
          }
        }
        
        loopStats.get(taskId)!.push({
          taskId,
          loopNumber,
          timestamp: new Date(),
          totalJobs: total,
          waitingJobs: stats.waiting.length,
          activeJobs: stats.active.length,
          completedJobs: stats.completed.length,
          failedJobs: stats.failed.length,
          jobIds: Array.from(stats.allJobIds)
        });
        
        // Show recent job IDs for debugging
        if (stats.waiting.length > 0) {
          console.log(`  Sample waiting job IDs: ${stats.waiting.slice(0, 3).join(', ')}`);
        }
      }
      
      // Compare with MagicEden for reference
      const magicedenTraitJobs = allJobs.filter(job => job.name === 'MAGICEDEN_TRAIT_BID');
      if (magicedenTraitJobs.length > 0) {
        console.log(BLUE + '\n📊 MagicEden Reference (for comparison):' + RESET);
        const meStats = new Map<string, { total: number, waiting: number, completed: number }>();
        
        for (const job of magicedenTraitJobs) {
          const taskId = job.data._id;
          const state = await job.getState();
          
          if (!meStats.has(taskId)) {
            meStats.set(taskId, { total: 0, waiting: 0, completed: 0 });
          }
          
          const stats = meStats.get(taskId)!;
          stats.total++;
          if (state === 'waiting') stats.waiting++;
          else if (state === 'completed') stats.completed++;
        }
        
        for (const [taskId, stats] of meStats) {
          console.log(`  Task ${taskId}: Total: ${stats.total} | Waiting: ${stats.waiting} | Completed: ${stats.completed}`);
        }
      }
      
      // Analyze trends
      if (checkCount > 3) {
        console.log(YELLOW + '\n📈 Trend Analysis:' + RESET);
        for (const [taskId, statsHistory] of loopStats) {
          if (statsHistory.length >= 2) {
            const recent = statsHistory[statsHistory.length - 1];
            const previous = statsHistory[statsHistory.length - 2];
            
            // Check if jobs were added (total increased)
            if (recent.totalJobs > previous.totalJobs) {
              const jobsAdded = recent.totalJobs - previous.totalJobs;
              console.log(GREEN + `  ✅ Task ${taskId}: ${jobsAdded} new jobs added (Loop ${recent.loopNumber})` + RESET);
            }
            
            // Check if stuck at same completion percentage
            const recentPercent = recent.totalJobs > 0 ? ((recent.completedJobs + recent.failedJobs) / recent.totalJobs * 100) : 0;
            const prevPercent = previous.totalJobs > 0 ? ((previous.completedJobs + previous.failedJobs) / previous.totalJobs * 100) : 0;
            
            if (Math.abs(recentPercent - 57.4) < 0.5 && Math.abs(prevPercent - 57.4) < 0.5) {
              console.log(RED + `  ❌ Task ${taskId}: Stuck at ~57.4% for multiple checks!` + RESET);
            }
          }
        }
      }
      
      if (checkCount >= maxChecks) {
        console.log(GREEN + '\n\n✅ Monitoring complete!' + RESET);
        
        // Final summary
        console.log(YELLOW + '\n📊 Final Summary:' + RESET);
        for (const [taskId, statsHistory] of loopStats) {
          const loops = new Set(statsHistory.map(s => s.loopNumber)).size;
          const maxJobs = Math.max(...statsHistory.map(s => s.totalJobs));
          const stuckAt574 = statsHistory.filter(s => {
            const percent = s.totalJobs > 0 ? ((s.completedJobs + s.failedJobs) / s.totalJobs * 100) : 0;
            return Math.abs(percent - 57.4) < 0.5;
          }).length;
          
          console.log(`\n  Task ${taskId}:`);
          console.log(`    Loops observed: ${loops}`);
          console.log(`    Max jobs seen: ${maxJobs}`);
          console.log(`    Times stuck at ~57.4%: ${stuckAt574}`);
          
          if (stuckAt574 > 2) {
            console.log(RED + '    ❌ CONFIRMED: Task exhibits the 57.4% stopping issue' + RESET);
          } else if (loops > 1) {
            console.log(GREEN + '    ✅ Task appears to be looping correctly' + RESET);
          }
        }
        
        clearInterval(checkInterval);
        await queue.close();
        await mongoose.disconnect();
        process.exit(0);
      }
      
    } catch (error) {
      console.error(RED + 'Error during check:' + RESET, error);
    }
  }, 30000); // Check every 30 seconds
  
  console.log(YELLOW + `\n🎯 Monitoring OpenSea trait bid jobs for ${maxChecks} checks...` + RESET);
  console.log(YELLOW + 'Looking for:' + RESET);
  console.log('  1. Jobs stopping at 57.4% on second loop');
  console.log('  2. Jobs being added but showing 0 waiting');
  console.log('  3. Comparison with MagicEden behavior');
}

async function main() {
  try {
    await connectToServices();
    await checkOpenSeaTraitJobs();
  } catch (error) {
    console.error(RED + 'Script error:' + RESET, error);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log(YELLOW + '\n\n⚡ Shutting down gracefully...' + RESET);
  await mongoose.disconnect();
  redisClient.quit();
  process.exit(0);
});

main();