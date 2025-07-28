// This is the improved startTask function ready to be integrated into src/index.ts
// Simply copy this entire function and replace the existing startTask function

import { ITask } from './src/models/task.model';
import { IWallet } from './src/models/wallet.model';
import { Job, Queue } from 'bullmq';
import { MaxUint256 } from 'ethers';
import BidLogs from './src/models/logs.model';
import { walletCache } from './src/utils/wallet-cache';
import { approvalCache } from './src/utils/approval-cache';
import { taskCircuitBreaker } from './src/utils/task-circuit-breaker';
import { decryptPrivateKey } from './src/utils/crypto';
import { approveMarketplaceWithCache } from './src/functions/approveMarketplace-improved';
import { logger, ErrorCategory } from './src/utils/logger';
import { WETH } from './src/utils/currencies';
// These constants are defined in the main index.ts file
declare const SEAPORT: string;
declare const MAGICEDEN_MARKETPLACE: string;

// Color constants
const YELLOW = '\x1b[33m';
const RED = '\x1b[31m';
const GREEN = '\x1b[32m';
const CYAN = '\x1b[36m';
const RESET = '\x1b[0m';

// These would be imported from the main index.ts file
declare const API_KEY: string | undefined;
declare const taskStatus: any;
declare const taskLoopCounters: Map<string, number>;
declare const skipStats: any;
declare const errorStats: any;
declare const completedTasks: any;
declare const activeTasks: Map<string, ITask>;
declare const currentTasks: ITask[];
declare const enabledMarketplaces: string[];
declare const wsManager: any;
declare const subscribeToCollections: (tasks: ITask[]) => void;
declare const removeTaskFromAborted: (taskId: string) => Promise<void>;
declare const addTaskToIndexes: (task: ITask) => void;
declare const queue: Queue;
declare const OPENSEA_SCHEDULE: string;
declare const BLUR_SCHEDULE: string;
declare const MAGICEDEN_SCHEDULE: string;
declare const createJobOptions: (priority?: number, timeout?: number, jobName?: string) => any;
declare const unlockTask: (taskId: string) => void;

async function startTask(task: ITask, start: boolean, marketplace?: string) {
  const startTime = Date.now();
  const taskId = task._id.toString();
  
  // Initialize result tracking
  const errors: string[] = [];
  const warnings: string[] = [];
  let criticalError = false;

  console.log(YELLOW + `[startTask] Called with task ${task._id}, start=${start}, marketplace=${marketplace || 'all'}` + RESET);

  // Validate environment variables
  if (!API_KEY) {
    console.log(RED + 'API_KEY is not set - cannot start task' + RESET);
    console.log(YELLOW + 'Please set API_KEY environment variable and restart' + RESET);
    try {
      await logger.logError(ErrorCategory.GENERAL, 'Missing API_KEY', { taskId });
    } catch (logError) {
      console.error('Failed to log error:', logError);
    }
    return;
  }

  if (!process.env.RATE_LIMIT) {
    console.log(RED + 'RATE_LIMIT is not set - cannot start task' + RESET);
    console.log(YELLOW + 'Please set RATE_LIMIT environment variable and restart' + RESET);
    try {
      await logger.logError(ErrorCategory.GENERAL, 'Missing RATE_LIMIT', { taskId });
    } catch (logError) {
      console.error('Failed to log error:', logError);
    }
    return;
  }

  // Initialize taskStatus with error handling
  try {
    if (!taskStatus[task._id]) {
      taskStatus[task._id] = {
        status: 'STARTED',
        runningPercentage: 0,
        waitingTime: 0,
        cancellationPercentage: 0,
        processingPercentage: 0,
        nextLoopTime: new Date()
      };
      taskLoopCounters.delete(task._id);
    } else if (!marketplace) {
      taskStatus[task._id].status = 'STARTED';
      taskStatus[task._id].runningPercentage = 0;
      taskStatus[task._id].cancellationPercentage = 0;
      taskStatus[task._id].processingPercentage = 0;
      taskLoopCounters.delete(task._id);
    } else {
      console.log(GREEN + `Starting marketplace ${marketplace} for task ${task._id} (current status: ${taskStatus[task._id].status})` + RESET);
      if (taskStatus[task._id].previousState) {
        console.log(YELLOW + `Clearing previousState for task ${task._id}` + RESET);
        delete taskStatus[task._id].previousState;
      }
    }
  } catch (error) {
    const errorMsg = `Failed to initialize task status: ${error instanceof Error ? error.message : String(error)}`;
    console.error(RED + errorMsg + RESET);
    warnings.push(errorMsg);
    try {
      await logger.logWarning(ErrorCategory.TASK_MANAGEMENT, errorMsg, { taskId, error: error as Error });
    } catch (logError) {
      console.error('Failed to log warning:', logError);
    }
  }

  // Initialize stats with error handling
  if (!marketplace) {
    try {
      skipStats[task._id] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };

      errorStats[task._id] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };

      completedTasks[task._id] = {
        opensea: false,
        magiceden: false,
        blur: false
      };
    } catch (error) {
      const errorMsg = `Failed to initialize task stats: ${error instanceof Error ? error.message : String(error)}`;
      console.error(YELLOW + errorMsg + RESET);
      warnings.push(errorMsg);
    }
  }

  // Async bid log deletion with proper error handling
  BidLogs.deleteMany({ taskId: task._id })
    .then(() => console.log(GREEN + `[Cleanup] Deleted bid logs for task ${taskId}` + RESET))
    .catch((err: any) => {
      const warning = `Failed to delete bid logs for task ${task._id}: ${err.message}`;
      console.error(YELLOW + warning + RESET);
      warnings.push(warning);
      logger.logWarning(ErrorCategory.DATABASE, warning, { taskId, error: err as Error })
        .catch((logError: any) => console.error('Failed to log warning:', logError));
    });

  // Wallet validation and approval processing
  let wallet: IWallet | null = null;
  let privateKey: string | null = null;
  
  try {
    wallet = await walletCache.getWalletById(task.walletId);
    if (!wallet?.encryptedPrivateKey) {
      const error = `Cannot start task - wallet not found or missing private key: ${task.walletId}`;
      console.log(RED + error + RESET);
      errors.push(error);
      criticalError = true;
      try {
        await logger.logError(ErrorCategory.GENERAL, error, { taskId, walletId: task.walletId });
      } catch (logError) {
        console.error('Failed to log error:', logError);
      }
      return;
    }

    try {
      privateKey = decryptPrivateKey(wallet.encryptedPrivateKey);
    } catch (decryptError) {
      const error = `Failed to decrypt wallet private key: ${decryptError instanceof Error ? decryptError.message : String(decryptError)}`;
      console.error(RED + error + RESET);
      errors.push(error);
      criticalError = true;
      try {
        await logger.logError(ErrorCategory.SECURITY, error, { taskId, walletId: task.walletId, error: decryptError as Error });
      } catch (logError) {
        console.error('Failed to log error:', logError);
      }
      return;
    }

    // Process marketplace approvals
    const validMarketplaces = ["opensea", "magiceden", "blur"];
    const selectedMarketplaces = marketplace ? [marketplace.toLowerCase()] : task.selectedMarketplaces.map((m: string) => m.toLowerCase());
    const marketplacesToApprove = selectedMarketplaces.filter((m: string) => validMarketplaces.includes(m));

    console.log(CYAN + `Checking marketplace approvals for wallet ${wallet.address}...` + RESET);

    // Parallel approval checks with individual error handling
    const approvalPromises = marketplacesToApprove.map(async (mp: string) => {
      try {
        let marketplaceAddress: string;

        switch (mp) {
          case "opensea":
            marketplaceAddress = SEAPORT;
            break;
          case "magiceden":
            marketplaceAddress = MAGICEDEN_MARKETPLACE;
            break;
          case "blur":
            console.log(YELLOW + `Skipping approval for Blur - not required` + RESET);
            return { marketplace: mp, approved: true, cached: false, error: null };
          default:
            console.log(YELLOW + `Unknown marketplace: ${mp}, skipping approval` + RESET);
            return { marketplace: mp, approved: false, cached: false, error: 'Unknown marketplace' };
        }

        // Check cache first with error handling
        let cached = false;
        try {
          cached = await approvalCache.isApproved(wallet!.address, marketplaceAddress, WETH);
          if (cached) {
            console.log(GREEN + `✓ ${mp} approval confirmed for wallet ${wallet!.address} (cached)` + RESET);
            return { marketplace: mp, approved: true, cached: true, error: null };
          }
        } catch (cacheError) {
          console.warn(YELLOW + `Cache check failed for ${mp}, checking on-chain: ${cacheError}` + RESET);
          warnings.push(`Approval cache check failed for ${mp}`);
        }

        // Check on-chain approval
        console.log(CYAN + `Checking ${mp} approval on-chain...` + RESET);
        try {
          const approved = await approveMarketplaceWithCache(marketplaceAddress, privateKey!, WETH, process.env.ALCHEMY_API_KEY || '');

          if (approved) {
            console.log(GREEN + `✓ ${mp} approval confirmed for wallet ${wallet!.address}` + RESET);
            
            // Update cache with error handling
            try {
              await approvalCache.setApproval(wallet!.address, marketplaceAddress, WETH, {
                approved: true,
                timestamp: Date.now(),
                allowance: MaxUint256.toString()
              });
            } catch (cacheUpdateError) {
              console.warn(YELLOW + `Failed to update approval cache for ${mp}: ${cacheUpdateError}` + RESET);
              warnings.push(`Failed to cache ${mp} approval`);
            }
            
            return { marketplace: mp, approved: true, cached: false, error: null };
          } else {
            console.log(RED + `✗ Failed to approve ${mp} for wallet ${wallet!.address}` + RESET);
            return { marketplace: mp, approved: false, cached: false, error: 'Approval failed' };
          }
        } catch (approvalError) {
          const error = `Approval check failed for ${mp}: ${approvalError instanceof Error ? approvalError.message : String(approvalError)}`;
          console.error(RED + error + RESET);
          return { marketplace: mp, approved: false, cached: false, error };
        }
      } catch (error) {
        const errorMsg = `Unexpected error processing ${mp} approval: ${error instanceof Error ? error.message : String(error)}`;
        console.error(RED + errorMsg + RESET);
        return { marketplace: mp, approved: false, cached: false, error: errorMsg };
      }
    });

    // Wait for all approvals with Promise.allSettled to handle individual failures
    const approvalResults = await Promise.allSettled(approvalPromises);
    const failedApprovals: string[] = [];
    const approvalErrors: string[] = [];

    for (const result of approvalResults) {
      if (result.status === 'fulfilled') {
        if (!result.value.approved) {
          failedApprovals.push(result.value.marketplace);
          if (result.value.error) {
            approvalErrors.push(`${result.value.marketplace}: ${result.value.error}`);
          }
        }
      } else {
        const error = `Approval promise rejected: ${result.reason}`;
        console.error(RED + error + RESET);
        approvalErrors.push(error);
      }
    }
    
    if (failedApprovals.length > 0) {
      const warning = `Warning: ${failedApprovals.length} marketplace(s) failed approval: ${failedApprovals.join(', ')}`;
      console.log(YELLOW + warning + RESET);
      warnings.push(warning);
      approvalErrors.forEach(err => warnings.push(err));
      
      try {
        await logger.logWarning(ErrorCategory.MARKETPLACE, 'Some marketplace approvals failed', {
          taskId,
          failedMarketplaces: failedApprovals,
          errors: approvalErrors
        });
      } catch (logError) {
        console.error('Failed to log warning:', logError);
      }
    }
  } catch (error) {
    const errorMsg = `Error during marketplace approval check: ${error instanceof Error ? error.message : String(error)}`;
    console.error(RED + errorMsg + RESET);
    errors.push(errorMsg);
    try {
      await logger.logError(ErrorCategory.GENERAL, 'Approval process failed', {
        taskId,
        error: error as Error,
        errorMessage: errorMsg
      });
    } catch (logError) {
      console.error('Failed to log error:', logError);
    }
    // Continue with task execution even if approval check fails
  }

  try {
    // Update task state with error handling
    try {
      await removeTaskFromAborted(taskId);
    } catch (error) {
      console.warn(YELLOW + `Failed to remove task from aborted list: ${error}` + RESET);
      warnings.push('Failed to remove task from aborted list');
    }

    const updatedTask = { ...task, running: true };
    activeTasks.set(taskId, updatedTask);
    
    const taskIndex = currentTasks.findIndex((t: ITask) => t._id === task._id);
    if (taskIndex !== -1) {
      currentTasks[taskIndex].running = true;
    }

    // Add task to indexes with error handling
    try {
      addTaskToIndexes(updatedTask);
    } catch (error) {
      console.warn(YELLOW + `Failed to add task to indexes: ${error}` + RESET);
      warnings.push('Failed to add task to indexes');
    }

    console.log(GREEN + `Updated task ${task.contract.slug} running status to: ${start}`.toUpperCase() + RESET);

    // WebSocket subscription with error handling
    try {
      if (wsManager?.isConnected()) {
        console.log("subscribing to collection: ", task.contract.slug);
        const newTask = { ...task, running: true };
        subscribeToCollections([newTask]);
      } else {
        const warning = `WebSocket not connected, skipping subscription for ${task.contract.slug}`;
        console.log(YELLOW + warning + RESET);
        warnings.push(warning);
      }
    } catch (wsError) {
      const error = `Error subscribing to collection ${task.contract.slug}: ${wsError instanceof Error ? wsError.message : String(wsError)}`;
      console.error(RED + error + RESET);
      warnings.push(error);
      try {
        await logger.logWarning(ErrorCategory.WEBSOCKET, 'WebSocket subscription failed', {
          taskId,
          collection: task.contract.slug,
          error: wsError as Error
        });
      } catch (logError) {
        console.error('Failed to log warning:', logError);
      }
    }

    // Create and queue jobs with comprehensive error handling
    const validMarketplaces = ["opensea", "magiceden", "blur"];
    const selectedMarketplaces = marketplace ? [marketplace.toLowerCase()] : task.selectedMarketplaces.map((m: string) => m.toLowerCase());
    
    // Pre-filter marketplaces with error handling
    const availableMarketplaces = selectedMarketplaces
      .filter((m: string) => {
        try {
          return enabledMarketplaces.map((em: string) => em.toLowerCase()).includes(m);
        } catch (error) {
          console.warn(YELLOW + `Error checking enabled marketplaces: ${error}` + RESET);
          return false;
        }
      })
      .filter((m: string) => validMarketplaces.includes(m))
      .filter((m: string) => {
        try {
          if (taskCircuitBreaker.isSuspended(taskId, m)) {
            console.log(YELLOW + `[Circuit Breaker] Skipping ${m} for task ${taskId} - currently suspended` + RESET);
            warnings.push(`Circuit breaker suspended ${m}`);
            return false;
          }
          return true;
        } catch (error) {
          console.warn(YELLOW + `Circuit breaker check failed for ${m}: ${error}` + RESET);
          return true; // Allow marketplace if circuit breaker check fails
        }
      });
    
    // Create jobs with error handling
    const jobs = availableMarketplaces
      .map((m: string) => {
        try {
          const jobId = `${task._id}-${m}-schedule`;
          switch (m) {
            case "opensea":
              return { name: OPENSEA_SCHEDULE, data: { ...task, running: start }, opts: { jobId } };
            case "blur":
              return { name: BLUR_SCHEDULE, data: { ...task, running: start }, opts: { jobId } };
            case "magiceden":
              return { name: MAGICEDEN_SCHEDULE, data: { ...task, running: start }, opts: { jobId } };
            default:
              return null;
          }
        } catch (error) {
          console.error(RED + `Failed to create job for ${m}: ${error}` + RESET);
          errors.push(`Failed to create job for ${m}`);
          return null;
        }
      })
      .filter((job: any) => job !== null);

    // Queue jobs with error handling
    if (jobs.length > 0) {
      try {
        const jobsWithOptions = jobs.map((job: any) => ({
          ...job,
          opts: createJobOptions(undefined, 60000, job.name)
        }));

        if (start === true) {
          console.log(GREEN + `Loop restart: Adding ${jobs.length} jobs without deduplication` + RESET);
          await queue.addBulk(jobsWithOptions as Job[]);
        } else {
          // Deduplication with error handling
          const existingJobNames = new Set<string>();
          
          // Check each job individually with error handling
          await Promise.all(
            jobsWithOptions.map(async (jobToAdd: any) => {
              try {
                const jobType = jobToAdd.name;
                const targetJobId = jobToAdd.opts?.jobId || `${taskId}-${jobType}`;
                
                const existingJob = await queue.getJob(targetJobId);
                if (existingJob) {
                  const state = await existingJob.getState();
                  if (['waiting', 'active', 'delayed'].includes(state)) {
                    existingJobNames.add(jobType);
                  }
                }
              } catch (err) {
                // Job doesn't exist or error checking, which is fine
                console.debug(`Job check error (non-critical): ${err}`);
              }
            })
          );

          const newJobs = jobsWithOptions.filter((job: any) => !existingJobNames.has(job.name));

          if (newJobs.length > 0) {
            console.log(GREEN + `Adding ${newJobs.length} new jobs (filtered ${jobs.length - newJobs.length} duplicates)` + RESET);
            await queue.addBulk(newJobs as Job[]);
          } else {
            console.log(YELLOW + `All ${jobs.length} jobs already exist in queue, skipping` + RESET);
          }
        }
      } catch (queueError) {
        const error = `Failed to queue jobs: ${queueError instanceof Error ? queueError.message : String(queueError)}`;
        console.error(RED + error + RESET);
        errors.push(error);
        try {
          await logger.logError(ErrorCategory.QUEUE, 'Job queueing failed', {
            taskId,
            jobCount: jobs.length,
            error: queueError as Error
          });
        } catch (logError) {
          console.error('Failed to log error:', logError);
        }
      }
    }

    // Progress task status with error handling
    if (!marketplace) {
      setTimeout(() => {
        try {
          if (taskStatus[taskId]?.status === 'STARTED') {
            console.log(`[TaskProgression] Moving task ${taskId} from STARTED to RUNNING`);
            taskStatus[taskId].status = 'RUNNING';
            taskStatus[taskId].processingPercentage = 100;
          }
        } catch (error) {
          console.error(RED + `Failed to update task progression: ${error}` + RESET);
        }
      }, 1000);
    } else {
      try {
        console.log(GREEN + `Recalculating progress for task ${taskId} after adding marketplace ${marketplace}` + RESET);
        
        if (completedTasks[taskId]) {
          completedTasks[taskId][marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur'] = false;
        }
      } catch (error) {
        console.warn(YELLOW + `Failed to reset marketplace completion status: ${error}` + RESET);
      }
    }

  } catch (error) {
    const errorMsg = `Error starting task ${taskId}: ${error instanceof Error ? error.message : String(error)}`;
    console.error(RED + errorMsg + RESET);
    errors.push(errorMsg);
    criticalError = true;
    
    try {
      await logger.logError(ErrorCategory.TASK_MANAGEMENT, 'Task start failed', {
        taskId,
        marketplace,
        error: error as Error,
        errorMessage: errorMsg,
        stackTrace: error instanceof Error ? error.stack : undefined
      });
    } catch (logError) {
      console.error('Failed to log error:', logError);
    }
  } finally {
    // Always release lock with error handling
    try {
      unlockTask(taskId);
    } catch (unlockError) {
      console.error(RED + `Failed to unlock task ${taskId}: ${unlockError}` + RESET);
    }
    
    // Log performance metrics
    const duration = Date.now() - startTime;
    console.log(CYAN + `[startTask] Completed in ${duration}ms for task ${taskId}` + RESET);
    
    if (duration > 5000) {
      const perfWarning = `Task startup took ${(duration / 1000).toFixed(1)}s - consider optimization`;
      console.log(YELLOW + `[Performance] ${perfWarning}` + RESET);
      warnings.push(perfWarning);
    }

    // Log summary
    if (errors.length > 0 || warnings.length > 0) {
      try {
        await logger.logInfo(ErrorCategory.TASK_MANAGEMENT, 'Task start completed with issues', {
          taskId,
          marketplace,
          duration,
          errorCount: errors.length,
          warningCount: warnings.length,
          errors: errors.slice(0, 5), // Limit to first 5 errors
          warnings: warnings.slice(0, 5), // Limit to first 5 warnings
          criticalError
        });
      } catch (logError) {
        console.error('Failed to log summary:', logError);
      }
    }

    // Return early if critical error occurred
    if (criticalError) {
      console.log(RED + `[startTask] Critical error occurred, task ${taskId} may not be running properly` + RESET);
    }
  }
}