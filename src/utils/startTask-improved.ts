import { ITask } from '../models/task.model';
import { IWallet } from '../models/wallet.model';
import { Job, Queue } from 'bullmq';
import { ethers } from 'ethers';
import BidLogs from '../models/logs.model';
import { walletCache } from './wallet-cache';
import { approvalCache } from './approval-cache';
import { taskCircuitBreaker } from './task-circuit-breaker';
import { decryptPrivateKey } from './crypto';
import { approveMarketplaceWithCache } from '../functions/approveMarketplace-improved';
import { logger, ErrorCategory } from './logger';

// Constants - these would be imported from the main file
const YELLOW = '\x1b[33m';
const RED = '\x1b[31m';
const GREEN = '\x1b[32m';
const CYAN = '\x1b[36m';
const RESET = '\x1b[0m';

interface StartTaskResult {
  success: boolean;
  taskId: string;
  errors: string[];
  warnings: string[];
  metrics: {
    duration: number;
    approvalsChecked: number;
    approvalsCached: number;
    approvalsFailed: number;
    jobsCreated: number;
    jobsSkipped: number;
  };
}

interface ApprovalResult {
  marketplace: string;
  approved: boolean;
  cached: boolean;
  error?: string;
}

/**
 * Improved startTask function with comprehensive error handling
 */
export async function startTaskImproved(
  task: ITask,
  start: boolean,
  marketplace: string | undefined,
  // Dependencies injected for better testability
  deps: {
    API_KEY?: string;
    RATE_LIMIT?: string;
    SEAPORT: string;
    MAGICEDEN_MARKETPLACE: string;
    WETH: string;
    OPENSEA_SCHEDULE: string;
    BLUR_SCHEDULE: string;
    MAGICEDEN_SCHEDULE: string;
    queue: Queue;
    taskStatus: any;
    taskLoopCounters: Map<string, number>;
    skipStats: any;
    errorStats: any;
    completedTasks: any;
    activeTasks: Map<string, ITask>;
    currentTasks: ITask[];
    enabledMarketplaces: string[];
    wsManager: any;
    subscribeToCollections: (tasks: ITask[]) => void;
    removeTaskFromAborted: (taskId: string) => Promise<void>;
    addTaskToIndexes: (task: ITask) => void;
    createJobOptions: (priority?: number, timeout?: number, jobName?: string) => any;
    unlockTask: (taskId: string) => void;
  }
): Promise<StartTaskResult> {
  const startTime = Date.now();
  const result: StartTaskResult = {
    success: false,
    taskId: task._id.toString(),
    errors: [],
    warnings: [],
    metrics: {
      duration: 0,
      approvalsChecked: 0,
      approvalsCached: 0,
      approvalsFailed: 0,
      jobsCreated: 0,
      jobsSkipped: 0
    }
  };

  const taskId = task._id.toString();

  try {
    // console.log(YELLOW + `[startTask] Called with task ${taskId}, start=${start}, marketplace=${marketplace || 'all'}` + RESET);

    // Validate API_KEY
    if (!deps.API_KEY) {
      const error = 'API_KEY is not set - cannot start task';
      // console.log(RED + error + RESET);
      // console.log(YELLOW + 'Please set API_KEY environment variable and restart' + RESET);
      result.errors.push(error);
      return result;
    }

    // Validate RATE_LIMIT
    if (!deps.RATE_LIMIT) {
      const error = 'RATE_LIMIT is not set - cannot start task';
      // console.log(RED + error + RESET);
      // console.log(YELLOW + 'Please set RATE_LIMIT environment variable and restart' + RESET);
      result.errors.push(error);
      return result;
    }

    // Initialize taskStatus with proper error handling
    try {
      initializeTaskStatus(taskId, marketplace, deps.taskStatus, deps.taskLoopCounters);
    } catch (error) {
      const errorMsg = `Failed to initialize task status: ${error instanceof Error ? error.message : String(error)}`;
      console.error(RED + errorMsg + RESET);
      result.errors.push(errorMsg);
      // Continue execution as this is not critical
    }

    // Initialize stats if not marketplace-specific
    if (!marketplace) {
      try {
        initializeTaskStats(taskId, deps.skipStats, deps.errorStats, deps.completedTasks);
      } catch (error) {
        const errorMsg = `Failed to initialize task stats: ${error instanceof Error ? error.message : String(error)}`;
        console.error(RED + errorMsg + RESET);
        result.warnings.push(errorMsg);
        // Continue execution as this is not critical
      }
    }

    // Async bid log deletion with proper error handling
    deleteBidLogs(taskId).catch(err => {
      const warning = `Failed to delete bid logs for task ${taskId}: ${err.message}`;
      console.error(YELLOW + warning + RESET);
      result.warnings.push(warning);
    });

    // Process wallet approvals
    const approvalResult = await processWalletApprovals(task, marketplace, deps, result);
    if (!approvalResult.success && approvalResult.critical) {
      return result;
    }

    // Update task state
    try {
      await updateTaskState(task, taskId, start, deps);
    } catch (error) {
      const errorMsg = `Failed to update task state: ${error instanceof Error ? error.message : String(error)}`;
      console.error(RED + errorMsg + RESET);
      result.errors.push(errorMsg);
      // Continue execution as we can still try to create jobs
    }

    // Subscribe to WebSocket
    try {
      await subscribeToWebSocket(task, deps);
    } catch (error) {
      const warning = `WebSocket subscription failed: ${error instanceof Error ? error.message : String(error)}`;
      console.error(YELLOW + warning + RESET);
      result.warnings.push(warning);
      // Continue execution as WebSocket is not critical for job creation
    }

    // Create and queue jobs
    const jobResult = await createAndQueueJobs(task, taskId, start, marketplace, deps, result);
    if (!jobResult.success) {
      result.errors.push('Failed to create and queue jobs');
      return result;
    }

    // Update task status progression
    try {
      updateTaskProgression(taskId, marketplace, deps);
    } catch (error) {
      const warning = `Failed to update task progression: ${error instanceof Error ? error.message : String(error)}`;
      console.error(YELLOW + warning + RESET);
      result.warnings.push(warning);
    }

    result.success = true;

  } catch (error) {
    const errorMsg = `Unexpected error in startTask: ${error instanceof Error ? error.message : String(error)}`;
    console.error(RED + errorMsg + RESET);
    result.errors.push(errorMsg);
    
    // Log to monitoring system
    try {
      await logger.logError(ErrorCategory.TASK_MANAGEMENT, 'Start task failed', {
        taskId,
        marketplace,
        error: error instanceof Error ? error : new Error(String(error)),
        errorMessage: errorMsg,
        stackTrace: error instanceof Error ? error.stack : undefined
      });
    } catch (logError) {
      console.error('Failed to log error:', logError);
    }
  } finally {
    // Always release lock and calculate metrics
    try {
      deps.unlockTask(taskId);
    } catch (error) {
      console.error(RED + `Failed to unlock task ${taskId}: ${error}` + RESET);
    }

    // Calculate final metrics
    result.metrics.duration = Date.now() - startTime;
    
    // Log performance metrics
    // console.log(CYAN + `[startTask] Completed in ${result.metrics.duration}ms for task ${taskId}` + RESET);
    
    if (result.metrics.duration > 5000) {
      const warning = `Task startup took ${(result.metrics.duration / 1000).toFixed(1)}s - consider optimization`;
      // console.log(YELLOW + `[Performance] ${warning}` + RESET);
      result.warnings.push(warning);
    }

    // Log final status
    if (result.success) {
      // console.log(GREEN + `[startTask] Successfully started task ${taskId}` + RESET);
    } else {
      // console.log(RED + `[startTask] Failed to start task ${taskId}: ${result.errors.join(', ')}` + RESET);
    }
  }

  return result;
}

/**
 * Initialize task status with proper state management
 */
function initializeTaskStatus(
  taskId: string,
  marketplace: string | undefined,
  taskStatus: any,
  taskLoopCounters: Map<string, number>
): void {
  if (!taskStatus[taskId]) {
    taskStatus[taskId] = {
      status: 'STARTED',
      runningPercentage: 0,
      waitingTime: 0,
      cancellationPercentage: 0,
      processingPercentage: 0,
      nextLoopTime: new Date()
    };
    taskLoopCounters.delete(taskId);
  } else if (!marketplace) {
    taskStatus[taskId].status = 'STARTED';
    taskStatus[taskId].runningPercentage = 0;
    taskStatus[taskId].cancellationPercentage = 0;
    taskStatus[taskId].processingPercentage = 0;
    taskLoopCounters.delete(taskId);
  } else {
    console.log(GREEN + `Starting marketplace ${marketplace} for task ${taskId} (current status: ${taskStatus[taskId].status})` + RESET);
    if (taskStatus[taskId].previousState) {
      // console.log(YELLOW + `Clearing previousState for task ${taskId}` + RESET);
      delete taskStatus[taskId].previousState;
    }
  }
}

/**
 * Initialize task statistics
 */
function initializeTaskStats(
  taskId: string,
  skipStats: any,
  errorStats: any,
  completedTasks: any
): void {
  skipStats[taskId] = {
    opensea: 0,
    magiceden: 0,
    blur: 0
  };

  errorStats[taskId] = {
    opensea: 0,
    magiceden: 0,
    blur: 0
  };

  completedTasks[taskId] = {
    opensea: false,
    magiceden: false,
    blur: false
  };
}

/**
 * Delete bid logs asynchronously
 */
async function deleteBidLogs(taskId: string): Promise<void> {
  try {
    await BidLogs.deleteMany({ taskId });
  } catch (error) {
    // Re-throw to be caught by caller
    throw new Error(`Bid log deletion failed: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/**
 * Process wallet approvals with comprehensive error handling
 */
async function processWalletApprovals(
  task: ITask,
  marketplace: string | undefined,
  deps: any,
  result: StartTaskResult
): Promise<{ success: boolean; critical: boolean }> {
  try {
    const wallet = await walletCache.getWalletById(task.walletId);
    if (!wallet?.encryptedPrivateKey) {
      const error = `Cannot start task - wallet not found or missing private key: ${task.walletId}`;
      // console.log(RED + error + RESET);
      result.errors.push(error);
      return { success: false, critical: true };
    }

    let privateKey: string;
    try {
      privateKey = decryptPrivateKey(wallet.encryptedPrivateKey);
    } catch (error) {
      const errorMsg = `Failed to decrypt wallet private key: ${error instanceof Error ? error.message : String(error)}`;
      console.error(RED + errorMsg + RESET);
      result.errors.push(errorMsg);
      return { success: false, critical: true };
    }

    const validMarketplaces = ["opensea", "magiceden", "blur"];
    const selectedMarketplaces = marketplace ? [marketplace.toLowerCase()] : task.selectedMarketplaces.map(m => m.toLowerCase());
    const marketplacesToApprove = selectedMarketplaces.filter(m => validMarketplaces.includes(m));

    // console.log(CYAN + `Checking marketplace approvals for wallet ${wallet.address}...` + RESET);

    // Process approvals in parallel with error handling for each
    const approvalPromises = marketplacesToApprove.map(mp => 
      processMarketplaceApproval(mp, wallet, privateKey, deps).catch(error => ({
        marketplace: mp,
        approved: false,
        cached: false,
        error: error instanceof Error ? error.message : String(error)
      }))
    );

    const approvalResults = await Promise.allSettled(approvalPromises);
    
    // Process results
    for (const settledResult of approvalResults) {
      if (settledResult.status === 'fulfilled') {
        const approvalResult = settledResult.value;
        result.metrics.approvalsChecked++;
        
        if (approvalResult.cached) {
          result.metrics.approvalsCached++;
        }
        
        if (!approvalResult.approved) {
          result.metrics.approvalsFailed++;
          const warning = `Failed to approve ${approvalResult.marketplace}: ${approvalResult.error || 'Unknown error'}`;
          result.warnings.push(warning);
        }
      } else {
        result.metrics.approvalsFailed++;
        const error = `Approval promise rejected: ${settledResult.reason}`;
        result.warnings.push(error);
      }
    }

    // Log summary
    if (result.metrics.approvalsFailed > 0) {
      // console.log(YELLOW + `Warning: ${result.metrics.approvalsFailed} marketplace(s) failed approval` + RESET);
    }

    return { success: true, critical: false };

  } catch (error) {
    const errorMsg = `Unexpected error during approval process: ${error instanceof Error ? error.message : String(error)}`;
    console.error(RED + errorMsg + RESET);
    result.errors.push(errorMsg);
    return { success: false, critical: false }; // Not critical, can continue
  }
}

/**
 * Process individual marketplace approval
 */
async function processMarketplaceApproval(
  marketplace: string,
  wallet: IWallet,
  privateKey: string,
  deps: any
): Promise<ApprovalResult> {
  let marketplaceAddress: string;

  switch (marketplace) {
    case "opensea":
      marketplaceAddress = deps.SEAPORT;
      break;
    case "magiceden":
      marketplaceAddress = deps.MAGICEDEN_MARKETPLACE;
      break;
    case "blur":
      // console.log(YELLOW + `Skipping approval for Blur - not required` + RESET);
      return { marketplace, approved: true, cached: false };
    default:
      // console.log(YELLOW + `Unknown marketplace: ${marketplace}, skipping approval` + RESET);
      return { marketplace, approved: false, cached: false, error: 'Unknown marketplace' };
  }

  try {
    // Check cache first
    const cached = await approvalCache.isApproved(wallet.address, marketplaceAddress, deps.WETH);
    if (cached) {
      // console.log(GREEN + `✓ ${marketplace} approval confirmed for wallet ${wallet.address} (cached)` + RESET);
      return { marketplace, approved: true, cached: true };
    }

    // console.log(CYAN + `Checking ${marketplace} approval on-chain...` + RESET);
    const approved = await approveMarketplaceWithCache(marketplaceAddress, privateKey, deps.WETH, process.env.ALCHEMY_API_KEY || '');

    if (approved) {
      // console.log(GREEN + `✓ ${marketplace} approval confirmed for wallet ${wallet.address}` + RESET);
      
      // Update cache with successful approval
      try {
        await approvalCache.setApproval(wallet.address, marketplaceAddress, deps.WETH, {
          approved: true,
          timestamp: Date.now(),
          allowance: ethers.MaxUint256.toString()
        });
      } catch (cacheError) {
        console.error(YELLOW + `Failed to update approval cache: ${cacheError}` + RESET);
        // Continue - caching failure is not critical
      }
      
      return { marketplace, approved: true, cached: false };
    } else {
      // console.log(RED + `✗ Failed to approve ${marketplace} for wallet ${wallet.address}` + RESET);
      return { marketplace, approved: false, cached: false, error: 'Approval transaction failed' };
    }

  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    console.error(RED + `Error approving ${marketplace}: ${errorMsg}` + RESET);
    return { marketplace, approved: false, cached: false, error: errorMsg };
  }
}

/**
 * Update task state in memory
 */
async function updateTaskState(
  task: ITask,
  taskId: string,
  start: boolean,
  deps: any
): Promise<void> {
  await deps.removeTaskFromAborted(taskId);
  
  const updatedTask = { ...task, running: true };
  deps.activeTasks.set(taskId, updatedTask);
  
  const taskIndex = deps.currentTasks.findIndex((t: ITask) => t._id === task._id);
  if (taskIndex !== -1) {
    deps.currentTasks[taskIndex].running = true;
  }

  deps.addTaskToIndexes(updatedTask);
  
  // console.log(GREEN + `Updated task ${task.contract.slug} running status to: ${start}`.toUpperCase() + RESET);
}

/**
 * Subscribe to WebSocket for real-time updates
 */
async function subscribeToWebSocket(task: ITask, deps: any): Promise<void> {
  if (deps.wsManager?.isConnected()) {
    // console.log("subscribing to collection: ", task.contract.slug);
    const newTask = { ...task, running: true };
    deps.subscribeToCollections([newTask]);
  } else {
    throw new Error(`WebSocket not connected for collection ${task.contract.slug}`);
  }
}

/**
 * Create and queue jobs with circuit breaker checks
 */
async function createAndQueueJobs(
  task: ITask,
  taskId: string,
  start: boolean,
  marketplace: string | undefined,
  deps: any,
  result: StartTaskResult
): Promise<{ success: boolean }> {
  try {
    const validMarketplaces = ["opensea", "magiceden", "blur"];
    const selectedMarketplaces = marketplace ? [marketplace.toLowerCase()] : task.selectedMarketplaces.map(m => m.toLowerCase());
    
    // Log marketplace filtering process
    console.log(CYAN + `[createAndQueueJobs] Filtering marketplaces for task ${taskId}:` + RESET);
    console.log(CYAN + `  - Selected marketplaces: ${selectedMarketplaces.join(', ')}` + RESET);
    console.log(CYAN + `  - Enabled marketplaces: ${deps.enabledMarketplaces.join(', ')}` + RESET);
    
    // Pre-filter marketplaces
    const enabledSelectedMarketplaces = selectedMarketplaces
      .filter(m => {
        const isEnabled = deps.enabledMarketplaces.map((em: string) => em.toLowerCase()).includes(m);
        if (!isEnabled) {
          console.log(YELLOW + `  ✗ Filtered out ${m} - not in enabled marketplaces` + RESET);
          result.warnings.push(`${m} is not enabled`);
        } else {
          console.log(GREEN + `  ✓ Including ${m} - is enabled` + RESET);
        }
        return isEnabled;
      });
    
    const availableMarketplaces = enabledSelectedMarketplaces
      .filter(m => validMarketplaces.includes(m))
      .filter(m => {
        if (taskCircuitBreaker.isSuspended(taskId, m)) {
          console.log(YELLOW + `  ✗ Circuit breaker suspended ${m} for task ${taskId}` + RESET);
          result.warnings.push(`Circuit breaker suspended ${m}`);
          result.metrics.jobsSkipped++;
          return false;
        }
        return true;
      });
    
    console.log(CYAN + `  - Final marketplaces for job creation: ${availableMarketplaces.join(', ') || 'none'}` + RESET);

    const jobs = createJobs(task, start, availableMarketplaces, deps);

    if (jobs.length > 0) {
      const jobsWithOptions = jobs.map(job => ({
        ...job,
        opts: deps.createJobOptions(undefined, 60000, job.name)
      }));

      if (start === true) {
        // Loop restart - add all jobs
        // console.log(GREEN + `Loop restart: Adding ${jobs.length} jobs without deduplication` + RESET);
        await deps.queue.addBulk(jobsWithOptions as Job[]);
        result.metrics.jobsCreated = jobs.length;
      } else {
        // Check for duplicates
        const { newJobs, duplicateCount } = await deduplicateJobs(jobsWithOptions, taskId, deps);
        
        if (newJobs.length > 0) {
          // console.log(GREEN + `Adding ${newJobs.length} new jobs (filtered ${duplicateCount} duplicates)` + RESET);
          await deps.queue.addBulk(newJobs as Job[]);
          result.metrics.jobsCreated = newJobs.length;
          result.metrics.jobsSkipped += duplicateCount;
        } else {
          // console.log(YELLOW + `All ${jobs.length} jobs already exist in queue, skipping` + RESET);
          result.metrics.jobsSkipped += jobs.length;
        }
      }
    }

    return { success: true };

  } catch (error) {
    const errorMsg = `Failed to create and queue jobs: ${error instanceof Error ? error.message : String(error)}`;
    console.error(RED + errorMsg + RESET);
    result.errors.push(errorMsg);
    return { success: false };
  }
}

/**
 * Create job objects for marketplaces
 */
function createJobs(task: ITask, start: boolean, marketplaces: string[], deps: any): any[] {
  return marketplaces
    .map(m => {
      const jobId = `${task._id}-${m}-schedule`;
      switch (m) {
        case "opensea":
          return { name: deps.OPENSEA_SCHEDULE, data: { ...task, running: start }, opts: { jobId } };
        case "blur":
          return { name: deps.BLUR_SCHEDULE, data: { ...task, running: start }, opts: { jobId } };
        case "magiceden":
          return { name: deps.MAGICEDEN_SCHEDULE, data: { ...task, running: start }, opts: { jobId } };
        default:
          return null;
      }
    })
    .filter(job => job !== null);
}

/**
 * Deduplicate jobs to avoid creating duplicates
 */
async function deduplicateJobs(
  jobsWithOptions: any[],
  taskId: string,
  deps: any
): Promise<{ newJobs: any[]; duplicateCount: number }> {
  const existingJobNames = new Set<string>();
  
  // Check each job individually
  await Promise.all(
    jobsWithOptions.map(async (jobToAdd) => {
      const jobType = jobToAdd.name;
      const targetJobId = jobToAdd.opts?.jobId || `${taskId}-${jobType}`;
      
      try {
        const existingJob = await deps.queue.getJob(targetJobId);
        if (existingJob) {
          const state = await existingJob.getState();
          if (['waiting', 'active', 'delayed'].includes(state)) {
            existingJobNames.add(jobType);
          }
        }
      } catch (err) {
        // Job doesn't exist, which is fine
      }
    })
  );

  const newJobs = jobsWithOptions.filter(job => !existingJobNames.has(job.name));
  const duplicateCount = jobsWithOptions.length - newJobs.length;

  return { newJobs, duplicateCount };
}

/**
 * Update task status progression
 */
function updateTaskProgression(
  taskId: string,
  marketplace: string | undefined,
  deps: any
): void {
  // For marketplace-specific starts, reset completed tasks tracking
  if (marketplace) {
    // console.log(GREEN + `Recalculating progress for task ${taskId} after adding marketplace ${marketplace}` + RESET);
    
    if (deps.completedTasks[taskId]) {
      deps.completedTasks[taskId][marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur'] = false;
    }
  }
  
  // Task will transition to RUNNING only when markJobAsProcessed is called
  // This ensures the task is actually processing work before showing as RUNNING
}