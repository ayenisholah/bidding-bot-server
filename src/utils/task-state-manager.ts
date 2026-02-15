import { DistributedLockManager } from './lock';
import { ITask } from '../models/task.model';
import { errorLog} from './debug-file-logger';

export type TaskStatus = 'STARTED' | 'RUNNING' | 'COMPLETED' | 'SCHEDULED' | 'CANCELLING' | 'STOPPED' | 'PROCESSING';

export interface TaskStatusInfo {
  status: TaskStatus;
  runningPercentage: number;
  cancellationPercentage: number;
  waitingTime: number;
  processingPercentage: number;
  nextLoopTime?: Date;
  cancellationStartTime?: number;
  lastUpdate?: number;
  version?: number; // Version counter for optimistic locking
}

export interface CompletedMarketplace {
  opensea: boolean;
  magiceden: boolean;
  blur: boolean;
}

export interface BulkJobParent {
  totalJobs: number;
  completedJobs: number;
  failedJobs: number;
  taskId: string;
  marketplace: string;
  // Optional bid tracking fields (only used for cancellation jobs)
  totalBids?: number;
  completedBids?: number;
  failedBids?: number;
}

export interface StateTransition {
  from: TaskStatus[];
  to: TaskStatus;
  condition?: (state: TaskStatusInfo) => boolean;
}

// Valid state transitions
const STATE_TRANSITIONS: StateTransition[] = [
  { from: ['STARTED'], to: 'RUNNING' },
  { from: ['STARTED'], to: 'CANCELLING' },
  { from: ['RUNNING'], to: 'COMPLETED' },
  { from: ['RUNNING'], to: 'CANCELLING' },
  { from: ['RUNNING'], to: 'PROCESSING' },
  { from: ['PROCESSING'], to: 'RUNNING' },
  { from: ['PROCESSING'], to: 'COMPLETED' },
  { from: ['COMPLETED'], to: 'SCHEDULED' },
  { from: ['SCHEDULED'], to: 'STARTED' },
  { from: ['SCHEDULED'], to: 'CANCELLING' },
  { from: ['CANCELLING'], to: 'STOPPED', condition: (state) => state.cancellationPercentage >= 100 },
  { from: ['STOPPED'], to: 'STARTED' },
  { from: ['STOPPED'], to: 'CANCELLING' }, // Added for stopTask flow
];

export class TaskStateManager {
  private taskStatus: Map<string, TaskStatusInfo> = new Map();
  private completedTasks: Map<string, CompletedMarketplace> = new Map();
  private parentJobs: Map<string, BulkJobParent> = new Map();
  private activeTasks: Map<string, ITask> = new Map();
  private taskTimeouts: Map<string, NodeJS.Timeout> = new Map();
  private taskCountdownTimers: Map<string, NodeJS.Timeout> = new Map();
  private lockManager: DistributedLockManager;
  
  // State change listeners
  private stateChangeListeners: Map<string, ((oldState: TaskStatus, newState: TaskStatus) => void)[]> = new Map();

  constructor(lockManager: DistributedLockManager) {
    this.lockManager = lockManager;
  }

  // Thread-safe state transition
  async transitionState(taskId: string, newStatus: TaskStatus, updater?: (info: TaskStatusInfo) => void): Promise<boolean> {
    const lockKey = `task-state:${taskId}`;
    // const timestamp = new Date().toISOString();
    const stackTrace = new Error().stack?.split('\n').slice(2, 5).join('\n    ');

    // console.log(`\n[TaskStateManager-TRANSITION] ${timestamp} Task ${taskId}`);
// console.log(`  Attempting lock acquisition for key: ${lockKey}`);

    // const lockStartTime = Date.now();

    return await this.lockManager.withLock(lockKey, async () => {
      // const lockAcquiredTime = Date.now() - lockStartTime;
// console.log(`  Lock acquired in ${lockAcquiredTime}ms`);
      
      const currentState = this.taskStatus.get(taskId);
      if (!currentState) {
        errorLog('TaskState', `Task ${taskId} has no current state`);
        errorLog('TaskState', `Available tasks: ${Array.from(this.taskStatus.keys()).join(', ')}`);
        errorLog('TaskState', `Called from:\n    ${stackTrace}`);
        return false;
      }

// console.log(`  Current state: ${JSON.stringify({
        // status: currentState.status,
        // version: currentState.version,
        // runningPercentage: currentState.runningPercentage,
        // cancellationPercentage: currentState.cancellationPercentage,
        // waitingTime: currentState.waitingTime,
        // lastUpdate: currentState.lastUpdate ? new Date(currentState.lastUpdate).toISOString() : 'N/A'
      // }, null, 2)}`);
// console.log(`  Requested transition: ${currentState.status} -> ${newStatus}`);

      // Validate state transition
      const validTransition = STATE_TRANSITIONS.find(t => 
        t.from.includes(currentState.status) && t.to === newStatus
      );

      if (!validTransition) {
        errorLog('TaskState', `Invalid state transition for task ${taskId}: ${currentState.status} -> ${newStatus}`);
        errorLog('TaskState', `Valid transitions from ${currentState.status}: ${
          STATE_TRANSITIONS
            .filter(t => t.from.includes(currentState.status))
            .map(t => t.to)
            .join(', ')
        }`);
        errorLog('TaskState', `Called from:\n    ${stackTrace}`);
        return false;
      }

      // Check transition condition if any
      if (validTransition.condition && !validTransition.condition(currentState)) {
        errorLog('TaskState', `State transition condition not met for task ${taskId}`);
        errorLog('TaskState', `Transition: ${currentState.status} -> ${newStatus}`);
        errorLog('TaskState', `Current state details: ${JSON.stringify(currentState)}`);
        return false;
      }

      const oldStatus = currentState.status;
      
      // Update state with version increment
      const updatedState: TaskStatusInfo = {
        ...currentState,
        status: newStatus,
        lastUpdate: Date.now(),
        version: (currentState.version || 0) + 1
      };

      // Apply custom updates if provided
      if (updater) {
// console.log(`  Applying custom updater function`);
        updater(updatedState);
      }

      this.taskStatus.set(taskId, updatedState);

      // Notify listeners
      this.notifyStateChange(taskId, oldStatus, newStatus);

      // console.log(`[StateManager] Task ${taskId} transitioned successfully`);
// console.log(`  From: ${oldStatus} (v${currentState.version || 0})`);
// console.log(`  To: ${newStatus} (v${updatedState.version})`);
// console.log(`  Updated state: ${JSON.stringify({
        // status: updatedState.status,
        // version: updatedState.version,
        // runningPercentage: updatedState.runningPercentage,
        // cancellationPercentage: updatedState.cancellationPercentage
      // }, null, 2)}`);
      
      return true;
    }, 30) || false;
  }

  // Initialize task state
  async initializeTaskState(taskId: string, status: TaskStatus = 'STARTED'): Promise<void> {
    const lockKey = `task-state:${taskId}`;
    // const timestamp = new Date().toISOString();
    // const stackTrace = new Error().stack?.split('\n').slice(2, 5).join('\n    ');

    // console.log(`\n[TaskStateManager-INIT] ${timestamp} Task ${taskId}`);
// console.log(`  Initial status: ${status}`);
// console.log(`  Lock key: ${lockKey}`);
// console.log(`  Called from:\n    ${stackTrace}`);

    // const lockStartTime = Date.now();

    await this.lockManager.withLock(lockKey, async () => {
      // const lockAcquiredTime = Date.now() - lockStartTime;
// console.log(`  Lock acquired in ${lockAcquiredTime}ms`);
      
      if (!this.taskStatus.has(taskId)) {
        const initialState: TaskStatusInfo = {
          status,
          runningPercentage: 0,
          cancellationPercentage: 0,
          waitingTime: 0,
          processingPercentage: 0,
          lastUpdate: Date.now(),
          version: 1
        };
        
        this.taskStatus.set(taskId, initialState);

// console.log(`  Task state initialized successfully`);
// console.log(`  Initial state: ${JSON.stringify(initialState, null, 2)}`);
      } else {
        // const existingState = this.taskStatus.get(taskId);
// console.log(`  Task already exists in state manager`);
// console.log(`  Existing state: ${JSON.stringify(existingState, null, 2)}`);
      }
    }, 30);
  }

  // Get task state (thread-safe read)
  getTaskState(taskId: string): TaskStatusInfo | null {
    return this.taskStatus.get(taskId) || null;
  }

  // Update task progress (thread-safe)
  async updateTaskProgress(taskId: string, updates: Partial<TaskStatusInfo>): Promise<void> {
    const lockKey = `task-state:${taskId}`;
    // const timestamp = new Date().toISOString();

    // console.log(`\n[TaskStateManager-PROGRESS] ${timestamp} Task ${taskId}`);
// console.log(`  Updates: ${JSON.stringify(updates, null, 2)}`);

    await this.lockManager.withLock(lockKey, async () => {
      const current = this.taskStatus.get(taskId);
      if (current) {
        // const previousState = { ...current };

        const updatedState = {
          ...current,
          ...updates,
          lastUpdate: Date.now(),
          version: (current.version || 0) + 1
        };
        
        this.taskStatus.set(taskId, updatedState);
        
// console.log(`  Progress updated successfully`);
// console.log(`  Previous: ${JSON.stringify({
          // runningPercentage: previousState.runningPercentage,
          // cancellationPercentage: previousState.cancellationPercentage,
          // processingPercentage: previousState.processingPercentage,
          // waitingTime: previousState.waitingTime
        // }, null, 2)}`);
// console.log(`  Current: ${JSON.stringify({
          // runningPercentage: updatedState.runningPercentage,
          // cancellationPercentage: updatedState.cancellationPercentage,
          // processingPercentage: updatedState.processingPercentage,
          // waitingTime: updatedState.waitingTime
        // }, null, 2)}`);
      } else {
        errorLog('TaskState', `Task ${taskId} not found in state manager`);
      }
    }, 30);
  }

  // Marketplace completion tracking
  async markMarketplaceComplete(taskId: string, marketplace: 'opensea' | 'magiceden' | 'blur'): Promise<boolean> {
    const lockKey = `task-completion:${taskId}`;
    
    return await this.lockManager.withLock(lockKey, async () => {
      let completed = this.completedTasks.get(taskId) || {
        opensea: false,
        magiceden: false,
        blur: false
      };

      completed[marketplace] = true;
      this.completedTasks.set(taskId, completed);

      // Check if all required marketplaces are complete
      const task = this.activeTasks.get(taskId);
      if (task) {
        const selectedMarkets = task.selectedMarketplaces.map(m => m.toLowerCase());
        const allComplete = selectedMarkets.every(m => 
          completed[m as keyof CompletedMarketplace]
        );
        return allComplete;
      }
      return false;
    }, 30) || false;
  }

  // Parent job management
  async updateParentJob(parentId: string, updates: { completed?: number; failed?: number }): Promise<BulkJobParent | null> {
    const lockKey = `parent-job:${parentId}`;
    
    return await this.lockManager.withLock(lockKey, async () => {
      const job = this.parentJobs.get(parentId);
      if (!job) return null;

      if (updates.completed !== undefined) {
        job.completedJobs += updates.completed;
      }
      if (updates.failed !== undefined) {
        job.failedJobs += updates.failed;
      }

      this.parentJobs.set(parentId, job);
      return job;
    }, 30);
  }

  // Timer management
  setTaskTimeout(taskId: string, timeout: NodeJS.Timeout): void {
    // Clear existing timeout
    const existing = this.taskTimeouts.get(taskId);
    if (existing) {
      clearTimeout(existing);
    }
    this.taskTimeouts.set(taskId, timeout);
  }

  clearTaskTimeout(taskId: string): void {
    const timeout = this.taskTimeouts.get(taskId);
    if (timeout) {
      clearTimeout(timeout);
      this.taskTimeouts.delete(taskId);
    }
  }

  setCountdownTimer(taskId: string, timer: NodeJS.Timeout): void {
    // Clear existing timer
    const existing = this.taskCountdownTimers.get(taskId);
    if (existing) {
      clearInterval(existing);
    }
    this.taskCountdownTimers.set(taskId, timer);
  }

  clearCountdownTimer(taskId: string): void {
    const timer = this.taskCountdownTimers.get(taskId);
    if (timer) {
      clearInterval(timer);
      this.taskCountdownTimers.delete(taskId);
    }
  }

  // Clean up all resources for a task
  async cleanupTask(taskId: string): Promise<void> {
    const lockKey = `task-cleanup:${taskId}`;
    
    await this.lockManager.withLock(lockKey, async () => {
      this.clearTaskTimeout(taskId);
      this.clearCountdownTimer(taskId);
      this.taskStatus.delete(taskId);
      this.completedTasks.delete(taskId);
      this.activeTasks.delete(taskId);
      this.stateChangeListeners.delete(taskId);
      
      // Clean up parent jobs
      const jobsToDelete: string[] = [];
      this.parentJobs.forEach((job, key) => {
        if (job.taskId === taskId) {
          jobsToDelete.push(key);
        }
      });
      jobsToDelete.forEach(key => this.parentJobs.delete(key));
    }, 30);
  }

  // State change notifications
  onStateChange(taskId: string, listener: (oldState: TaskStatus, newState: TaskStatus) => void): void {
    const listeners = this.stateChangeListeners.get(taskId) || [];
    listeners.push(listener);
    this.stateChangeListeners.set(taskId, listeners);
  }

  private notifyStateChange(taskId: string, oldState: TaskStatus, newState: TaskStatus): void {
    const listeners = this.stateChangeListeners.get(taskId) || [];
    listeners.forEach(listener => {
      try {
        listener(oldState, newState);
      } catch (error) {
        errorLog('TaskState', `Error in state change listener for task ${taskId}`, error);
      }
    });
  }

  // Getters for compatibility
  getActiveTasks(): Map<string, ITask> {
    return this.activeTasks;
  }

  getParentJobs(): Map<string, BulkJobParent> {
    return this.parentJobs;
  }

  getAllTaskStatus(): Map<string, TaskStatusInfo> {
    return this.taskStatus;
  }

  // Setters for initialization
  setActiveTask(taskId: string, task: ITask): void {
    this.activeTasks.set(taskId, task);
  }

  deleteActiveTask(taskId: string): void {
    this.activeTasks.delete(taskId);
  }

  setParentJob(parentId: string, job: BulkJobParent): void {
    this.parentJobs.set(parentId, job);
  }

  deleteParentJob(parentId: string): void {
    this.parentJobs.delete(parentId);
  }

  // Reset marketplace completion
  resetMarketplaceCompletion(taskId: string): void {
    this.completedTasks.set(taskId, {
      opensea: false,
      magiceden: false,
      blur: false
    });
  }
}