#!/usr/bin/env node

/**
 * Recovery Script for Stuck Tasks
 * 
 * This script identifies and fixes tasks that are stuck in RUNNING state
 * with high completion percentages but never reach 100%.
 * 
 * Usage: npm run scripts:fix-stuck-tasks
 */

import mongoose from 'mongoose';
import { config } from "dotenv";
import redisClient, { createBullMQConnection, getRedisConnection } from '../utils/redis';

config();

// Constants
const RED = '\x1b[31m';
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const BLUE = '\x1b[36m';
const RESET = '\x1b[0m';

interface TaskStatus {
  status: string;
  runningPercentage: number;
  cancellationPercentage: number;
  totalJobs: number;
  completedJobs: number;
  waitingTime: number;
  nextLoopTime?: Date;
  lastUpdate?: number;
}

interface ParentJob {
  totalJobs: number;
  completedJobs: number;
  failedJobs?: number;
  taskId: string;
  marketplace?: string;
}

// Redis keys used by the main application
const TASK_STATUS_KEY = 'taskStatus';
const PARENT_JOBS_KEY = 'parentJobs';
const ACTIVE_TASKS_KEY = 'activeTasks';

async function connectToMongoDB(): Promise<void> {
  const MONGODB_URI = process.env.MONGODB_URI;
  if (!MONGODB_URI) {
    throw new Error('MONGODB_URI environment variable is required');
  }

  try {
    await mongoose.connect(MONGODB_URI);
    console.log(GREEN + '[Recovery] Connected to MongoDB' + RESET);
  } catch (error) {
    console.error(RED + '[Recovery] Failed to connect to MongoDB:' + RESET, error);
    throw error;
  }
}

async function getTaskStatusFromRedis(): Promise<{ [taskId: string]: TaskStatus }> {
  try {
    const redis = getRedisConnection();
    const taskStatusData = await redis.hgetall(TASK_STATUS_KEY);
    const taskStatus: { [taskId: string]: TaskStatus } = {};

    for (const [taskId, dataStr] of Object.entries(taskStatusData)) {
      try {
        taskStatus[taskId] = JSON.parse(dataStr);
      } catch (error) {
        console.error(RED + `[Recovery] Failed to parse task status for ${taskId}:` + RESET, error);
      }
    }

    return taskStatus;
  } catch (error) {
    console.error(RED + '[Recovery] Failed to get task status from Redis:' + RESET, error);
    return {};
  }
}

async function getParentJobsFromRedis(): Promise<Map<string, ParentJob>> {
  const parentJobs = new Map<string, ParentJob>();

  try {
    const redis = getRedisConnection();
    const parentJobsData = await redis.hgetall(PARENT_JOBS_KEY);

    for (const [parentJobId, dataStr] of Object.entries(parentJobsData)) {
      try {
        parentJobs.set(parentJobId, JSON.parse(dataStr));
      } catch (error) {
        console.error(RED + `[Recovery] Failed to parse parent job for ${parentJobId}:` + RESET, error);
      }
    }

    return parentJobs;
  } catch (error) {
    console.error(RED + '[Recovery] Failed to get parent jobs from Redis:' + RESET, error);
    return new Map();
  }
}

async function updateTaskStatusInRedis(taskId: string, updates: Partial<TaskStatus>): Promise<void> {
  try {
    const redis = getRedisConnection();
    const currentDataStr = await redis.hget(TASK_STATUS_KEY, taskId);
    if (!currentDataStr) {
      console.error(RED + `[Recovery] Task status not found for ${taskId}` + RESET);
      return;
    }

    const currentData = JSON.parse(currentDataStr);
    const updatedData = { ...currentData, ...updates };

    await redis.hset(TASK_STATUS_KEY, taskId, JSON.stringify(updatedData));
    console.log(GREEN + `[Recovery] Updated task status for ${taskId}` + RESET);
  } catch (error) {
    console.error(RED + `[Recovery] Failed to update task status for ${taskId}:` + RESET, error);
  }
}

function identifyStuckTasks(taskStatus: { [taskId: string]: TaskStatus }): string[] {
  const stuckTasks: string[] = [];
  const now = Date.now();

  for (const [taskId, status] of Object.entries(taskStatus)) {
    if (status.status === 'RUNNING' && 
        status.runningPercentage >= 85 && 
        status.runningPercentage < 100) {
      
      // Check if task has been stuck for more than 5 minutes
      const lastUpdate = status.lastUpdate || 0;
      const stuckTime = now - lastUpdate;
      const stuckMinutes = stuckTime / (1000 * 60);

      if (stuckMinutes > 5) {
        console.log(YELLOW + `[Recovery] Found stuck task: ${taskId}` + RESET);
        console.log(YELLOW + `  Status: ${status.status}` + RESET);
        console.log(YELLOW + `  Progress: ${status.runningPercentage.toFixed(1)}%` + RESET);
        console.log(YELLOW + `  Stuck for: ${stuckMinutes.toFixed(1)} minutes` + RESET);
        console.log(YELLOW + `  Jobs: ${status.completedJobs}/${status.totalJobs}` + RESET);
        stuckTasks.push(taskId);
      }
    }
  }

  return stuckTasks;
}

function analyzeStuckTask(
  taskId: string,
  taskStatus: TaskStatus,
  parentJobs: Map<string, ParentJob>
): void {
  console.log(BLUE + `\n[Recovery Analysis] Task ${taskId}:` + RESET);
  console.log(BLUE + `  Status: ${taskStatus.status}` + RESET);
  console.log(BLUE + `  Progress: ${taskStatus.runningPercentage.toFixed(1)}%` + RESET);
  console.log(BLUE + `  Jobs: ${taskStatus.completedJobs}/${taskStatus.totalJobs}` + RESET);

  // Find related parent jobs
  const relatedParentJobs: Array<[string, ParentJob]> = [];
  for (const [parentJobId, parentJob] of parentJobs.entries()) {
    if (parentJob.taskId === taskId || parentJobId.startsWith(taskId)) {
      relatedParentJobs.push([parentJobId, parentJob]);
    }
  }

  console.log(BLUE + `  Related parent jobs: ${relatedParentJobs.length}` + RESET);

  let totalExpected = 0;
  let totalCompleted = 0;

  for (const [parentJobId, parentJob] of relatedParentJobs) {
    console.log(BLUE + `    ${parentJobId}: ${parentJob.completedJobs}/${parentJob.totalJobs}` + RESET);
    totalExpected += parentJob.totalJobs || 0;
    totalCompleted += parentJob.completedJobs || 0;
  }

  if (totalExpected > 0) {
    const actualPercentage = (totalCompleted / totalExpected) * 100;
    console.log(BLUE + `  Actual progress: ${totalCompleted}/${totalExpected} (${actualPercentage.toFixed(1)}%)` + RESET);

    if (actualPercentage >= 100) {
      console.log(GREEN + `  Task should be completed - parent jobs show 100% completion` + RESET);
    } else if (actualPercentage >= 95) {
      console.log(YELLOW + `  Task near completion - likely stuck on final jobs` + RESET);
    }
  }
}

async function fixStuckTask(taskId: string): Promise<boolean> {
  try {
    console.log(GREEN + `\n[Recovery] Fixing stuck task: ${taskId}` + RESET);

    // Force completion by setting percentage to 100% and status to COMPLETED
    await updateTaskStatusInRedis(taskId, {
      runningPercentage: 100,
      status: 'COMPLETED',
      lastUpdate: Date.now()
    });

    // After a brief delay, set to SCHEDULED to resume normal operation
    setTimeout(async () => {
      await updateTaskStatusInRedis(taskId, {
        status: 'SCHEDULED',
        cancellationPercentage: 0,
        waitingTime: 0
      });
      console.log(GREEN + `[Recovery] Task ${taskId} reset to SCHEDULED status` + RESET);
    }, 2000);

    return true;
  } catch (error) {
    console.error(RED + `[Recovery] Failed to fix task ${taskId}:` + RESET, error);
    return false;
  }
}

async function main(): Promise<void> {
  console.log(BLUE + '[Recovery] Starting stuck task recovery script' + RESET);

  try {
    // Connect to databases
    await connectToMongoDB();
    await createBullMQConnection();

    // Get current state from Redis
    console.log(BLUE + '[Recovery] Loading current task state from Redis...' + RESET);
    const taskStatus = await getTaskStatusFromRedis();
    const parentJobs = await getParentJobsFromRedis();

    console.log(BLUE + `[Recovery] Found ${Object.keys(taskStatus).length} tasks in Redis` + RESET);
    console.log(BLUE + `[Recovery] Found ${parentJobs.size} parent jobs in Redis` + RESET);

    // Identify stuck tasks
    const stuckTasks = identifyStuckTasks(taskStatus);

    if (stuckTasks.length === 0) {
      console.log(GREEN + '[Recovery] No stuck tasks found - all tasks appear healthy' + RESET);
      return;
    }

    console.log(YELLOW + `[Recovery] Found ${stuckTasks.length} stuck tasks` + RESET);

    // Analyze each stuck task
    for (const taskId of stuckTasks) {
      analyzeStuckTask(taskId, taskStatus[taskId], parentJobs);
    }

    // Ask for user confirmation before fixing (in production, you might want to auto-fix)
    console.log(YELLOW + '\n[Recovery] The above tasks will be force-completed and reset to SCHEDULED.' + RESET);
    console.log(YELLOW + '[Recovery] This will allow them to resume normal operation.' + RESET);

    // For the specific stuck tasks mentioned by the user, auto-fix them
    const targetTasks = ['68985da8490751bf0e9fcd37', '68985da8490751bf0e9fcd3c'];
    const tasksToFix = stuckTasks.filter(taskId => targetTasks.includes(taskId));

    if (tasksToFix.length > 0) {
      console.log(GREEN + `\n[Recovery] Auto-fixing known stuck tasks: ${tasksToFix.join(', ')}` + RESET);

      let fixedCount = 0;
      for (const taskId of tasksToFix) {
        const success = await fixStuckTask(taskId);
        if (success) {
          fixedCount++;
        }
      }

      console.log(GREEN + `[Recovery] Successfully fixed ${fixedCount}/${tasksToFix.length} stuck tasks` + RESET);
    }

    // Handle remaining stuck tasks
    const otherStuckTasks = stuckTasks.filter(taskId => !targetTasks.includes(taskId));
    if (otherStuckTasks.length > 0) {
      console.log(YELLOW + `\n[Recovery] Other stuck tasks found: ${otherStuckTasks.join(', ')}` + RESET);
      console.log(YELLOW + '[Recovery] These can be fixed manually by running the fix for specific task IDs' + RESET);
    }

  } catch (error) {
    console.error(RED + '[Recovery] Error in recovery script:' + RESET, error);
    process.exit(1);
  } finally {
    // Close connections
    await mongoose.connection.close();
    // Redis client is managed by the RedisClient class, no need to manually disconnect
    console.log(BLUE + '[Recovery] Recovery script completed' + RESET);
  }
}

// Run the recovery script
if (require.main === module) {
  main().catch((error) => {
    console.error(RED + '[Recovery] Fatal error:' + RESET, error);
    process.exit(1);
  });
}

export { main as runStuckTaskRecovery };