import { EnhancedTaskStateManager, EnhancedTaskStatus } from './enhanced-task-state-manager';
import { EnhancedQueueEventHandlerManager } from './enhanced-queue-event-handlers';
import { EnhancedJobManager } from './enhanced-job-manager';
import { ITask } from '../models/task.model';
import { logger, ErrorCategory } from './logger';

interface TaskExecutionPlan {
  taskId: string;
  phase: 'PROCESSING' | 'QUEUEING' | 'RUNNING' | 'COMPLETING';
  estimatedJobs: number;
  marketplaces: string[];
  startTime: number;
}

/**
 * Enhanced Task Orchestrator that manages the complete task lifecycle
 * STARTED -> PROCESSING -> QUEUEING -> QUEUED -> RUNNING -> COMPLETED -> SCHEDULED
 */
export class EnhancedTaskOrchestrator {
  private stateManager: EnhancedTaskStateManager;
  private queueHandler: EnhancedQueueEventHandlerManager;
  private jobManager: EnhancedJobManager;
  private executionPlans: Map<string, TaskExecutionPlan> = new Map();

  // Colors for console output
  private readonly GREEN = '\x1b[32m';
  private readonly BLUE = '\x1b[34m';
  private readonly YELLOW = '\x1b[33m';
  private readonly CYAN = '\x1b[36m';
  private readonly RESET = '\x1b[0m';

  constructor(
    stateManager: EnhancedTaskStateManager,
    queueHandler: EnhancedQueueEventHandlerManager,
    jobManager: EnhancedJobManager
  ) {
    this.stateManager = stateManager;
    this.queueHandler = queueHandler;
    this.jobManager = jobManager;
    
    this.setupStateChangeListeners();
  }

  /**
   * Start a task with the complete enhanced flow
   */
  async startTask(taskId: string, task: ITask): Promise<void> {
    try {
      // console.log(`${this.CYAN}[Orchestrator] Starting enhanced task execution for ${taskId}${this.RESET}`);
      
      // Initialize task state
      await this.stateManager.initializeEnhancedTaskState(taskId, task, 'STARTED');
      
      // Create execution plan
      const executionPlan: TaskExecutionPlan = {
        taskId,
        phase: 'PROCESSING',
        estimatedJobs: 0, // Will be calculated during processing
        marketplaces: task.selectedMarketplaces,
        startTime: Date.now()
      };
      
      this.executionPlans.set(taskId, executionPlan);
      
      // Start processing phase
      await this.startProcessingPhase(taskId, task);
      
    } catch (error) {
      await logger.logError(ErrorCategory.TASK_MANAGEMENT, 'Error starting enhanced task', {
        taskId,
        error: error as Error
      });
      
      // Transition to stopped state on error
      await this.stateManager.transitionState(taskId, 'STOPPED');
    }
  }

  /**
   * Stop a task and initiate cancellation
   */
  async stopTask(taskId: string, marketplace?: string): Promise<void> {
    try {
      // console.log(`${this.YELLOW}[Orchestrator] Stopping task ${taskId}${marketplace ? ` for ${marketplace}` : ''}${this.RESET}`);
      
      // Start cancellation process
      await this.queueHandler.startTaskCancellation(taskId, marketplace);
      
      // Clean up execution plan
      this.executionPlans.delete(taskId);
      
    } catch (error) {
      await logger.logError(ErrorCategory.TASK_MANAGEMENT, 'Error stopping task', {
        taskId,
        marketplace,
        error: error as Error
      });
    }
  }

  /**
   * Start the processing phase - analyze task requirements
   */
  private async startProcessingPhase(taskId: string, task: ITask): Promise<void> {
    // console.log(`${this.BLUE}[Orchestrator] Starting processing phase for task ${taskId}${this.RESET}`);
    
    const executionPlan = this.executionPlans.get(taskId);
    if (!executionPlan) return;
    
    // Transition to processing state
    await this.stateManager.startProcessing(taskId);
    
    // Simulate processing work (in real implementation, this would analyze the task)
    await this.simulateProcessingWork(taskId, task);
    
    // Move to queueing phase
    executionPlan.phase = 'QUEUEING';
    await this.startQueueingPhase(taskId, task);
  }

  /**
   * Simulate processing work to calculate job requirements
   */
  private async simulateProcessingWork(taskId: string, task: ITask): Promise<void> {
    // Update processing percentage in stages
    const stages = [20, 40, 60, 80, 100];
    
    for (const percentage of stages) {
      await new Promise(resolve => setTimeout(resolve, 200)); // Simulate work
      await this.stateManager.updateTaskProgress(taskId, { processingPercentage: percentage });
      
      if (percentage === 60) {
        // console.log(`${this.BLUE}[Orchestrator] Analyzing traits and tokens for task ${taskId}${this.RESET}`);
      } else if (percentage === 80) {
        // console.log(`${this.BLUE}[Orchestrator] Calculating job requirements for task ${taskId}${this.RESET}`);
      }
    }
    
    // console.log(`${this.GREEN}[Orchestrator] Processing phase completed for task ${taskId}${this.RESET}`);
  }

  /**
   * Start the queueing phase - create and queue all jobs
   */
  private async startQueueingPhase(taskId: string, task: ITask): Promise<void> {
    // console.log(`${this.BLUE}[Orchestrator] Starting queueing phase for task ${taskId}${this.RESET}`);
    
    const executionPlan = this.executionPlans.get(taskId);
    if (!executionPlan) return;
    
    try {
      // Create job batches based on task configuration
      const jobBatches = await this.createJobBatches(taskId, task);
      
      // Update execution plan with estimated jobs
      executionPlan.estimatedJobs = jobBatches.reduce((total, batch) => total + batch.jobs.length, 0);
      
      // console.log(`${this.BLUE}[Orchestrator] Created ${executionPlan.estimatedJobs} jobs across ${jobBatches.length} marketplaces${this.RESET}`);
      
      // Create and queue jobs
      await this.jobManager.createAndQueueTaskJobs(taskId, jobBatches);
      
      // console.log(`${this.GREEN}[Orchestrator] Queueing phase completed for task ${taskId}${this.RESET}`);
      
    } catch (error) {
      await logger.logError(ErrorCategory.TASK_MANAGEMENT, 'Error in queueing phase', {
        taskId,
        error: error as Error
      });
    }
  }

  /**
   * Create job batches based on task configuration
   */
  private async createJobBatches(taskId: string, task: ITask): Promise<any[]> {
    const jobBatches: any[] = [];
    
    // Create jobs for each selected marketplace
    for (const marketplace of task.selectedMarketplaces) {
      const marketplaceLower = marketplace.toLowerCase();
      
      // Simulate job creation (in real implementation, this would create actual job data)
      const jobs = await this.createJobsForMarketplace(taskId, task, marketplaceLower);
      
      if (jobs.length > 0) {
        jobBatches.push({
          marketplace: marketplaceLower,
          jobs: jobs,
          expectedOrderCount: jobs.length
        });
      }
    }
    
    return jobBatches;
  }

  /**
   * Create jobs for a specific marketplace
   */
  private async createJobsForMarketplace(taskId: string, task: ITask, marketplace: string): Promise<any[]> {
    const jobs: any[] = [];
    
    // This is a placeholder - in real implementation, this would:
    // 1. Analyze task.traits to create trait-based jobs
    // 2. Analyze task.tokens to create token-based jobs
    // 3. Create collection-wide jobs if applicable
    // 4. Consider task.bidType, task.priceRange, etc.
    
    // For demonstration, create some dummy jobs
    const jobCount = Math.floor(Math.random() * 50) + 10; // Random 10-60 jobs
    
    for (let i = 0; i < jobCount; i++) {
      jobs.push({
        taskId,
        marketplace,
        jobType: 'BID' as const,
        data: {
          jobIndex: i,
          // In real implementation, this would contain:
          // - trait specifications
          // - token IDs
          // - bid amounts
          // - marketplace-specific parameters
        }
      });
    }
    
    return jobs;
  }

  /**
   * Setup state change listeners to handle transitions
   */
  private setupStateChangeListeners(): void {
    // We can't directly listen to all tasks, so this would need to be called
    // when tasks are initialized. For now, it's a placeholder for the pattern.
  }

  /**
   * Handle state change for a specific task
   */
  private async handleTaskStateChange(taskId: string, oldState: EnhancedTaskStatus, newState: EnhancedTaskStatus): Promise<void> {
    const executionPlan = this.executionPlans.get(taskId);
    
    // console.log(`${this.CYAN}[Orchestrator] Task ${taskId} transitioned: ${oldState} -> ${newState}${this.RESET}`);
    
    switch (newState) {
      case 'QUEUED':
        // console.log(`${this.GREEN}[Orchestrator] All jobs queued for task ${taskId}, ready to run${this.RESET}`);
        await this.stateManager.transitionState(taskId, 'RUNNING');
        if (executionPlan) executionPlan.phase = 'RUNNING';
        break;
        
      case 'RUNNING':
        // console.log(`${this.GREEN}[Orchestrator] Task ${taskId} is now running jobs${this.RESET}`);
        break;
        
      case 'COMPLETED':
        // console.log(`${this.GREEN}[Orchestrator] Task ${taskId} completed successfully${this.RESET}`);
        if (executionPlan) executionPlan.phase = 'COMPLETING';
        break;
        
      case 'SCHEDULED':
        // console.log(`${this.BLUE}[Orchestrator] Task ${taskId} scheduled for next loop${this.RESET}`);
        // Reset execution plan for next iteration
        if (executionPlan) {
          executionPlan.phase = 'PROCESSING';
          executionPlan.startTime = Date.now();
        }
        break;
        
      case 'STOPPED':
        // console.log(`${this.YELLOW}[Orchestrator] Task ${taskId} stopped${this.RESET}`);
        this.executionPlans.delete(taskId);
        break;
    }
  }

  /**
   * Get task execution status
   */
  getTaskExecutionStatus(taskId: string): {
    state?: any;
    plan?: TaskExecutionPlan;
    jobStats?: any;
    duration?: number;
  } {
    const state = this.stateManager.getTaskState(taskId);
    const plan = this.executionPlans.get(taskId);
    const jobStats = this.jobManager.getTaskJobStatistics(taskId);
    
    return {
      state,
      plan,
      jobStats,
      duration: plan ? Date.now() - plan.startTime : undefined
    };
  }

  /**
   * Get all active execution plans
   */
  getActiveExecutionPlans(): TaskExecutionPlan[] {
    return Array.from(this.executionPlans.values());
  }

  /**
   * Initialize state change listener for a task (call this when starting a task)
   */
  initializeTaskStateListener(taskId: string): void {
    this.stateManager.onStateChange(taskId, (oldState, newState) => {
      this.handleTaskStateChange(taskId, oldState, newState);
    });
  }

  /**
   * Clean up orchestrator resources for a task
   */
  async cleanupTask(taskId: string): Promise<void> {
    this.executionPlans.delete(taskId);
    await this.stateManager.cleanupTask(taskId);
  }

  /**
   * Get comprehensive task status for monitoring
   */
  getComprehensiveTaskStatus(taskId: string): any {
    const state = this.stateManager.getTaskState(taskId);
    const plan = this.executionPlans.get(taskId);
    const jobStats = this.jobManager.getTaskJobStatistics(taskId);
    const task = this.stateManager.getActiveTasks().get(taskId);
    
    if (!state || !task) {
      return null;
    }
    
    return {
      taskId,
      status: state.status,
      progress: {
        processing: state.processingPercentage,
        queueing: state.queueingPercentage,
        running: state.runningPercentage,
        cancellation: state.cancellationPercentage
      },
      jobs: {
        total: jobStats.totalJobs,
        queued: jobStats.queuedJobs,
        completed: jobStats.completedJobs,
        failed: jobStats.failedJobs
      },
      marketplaces: {
        selected: task.selectedMarketplaces,
        progress: state.marketplaceProgress,
        breakdown: jobStats.marketplaceBreakdown
      },
      timing: {
        lastUpdate: state.lastUpdate,
        waitingTime: state.waitingTime,
        nextLoop: state.nextLoopTime,
        executionDuration: plan ? Date.now() - plan.startTime : undefined
      },
      phase: plan?.phase,
      version: state.version
    };
  }
}