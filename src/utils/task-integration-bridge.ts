import { ITask } from '../models/task.model';
import { EnhancedTaskOrchestrator } from './enhanced-task-orchestrator';
import { EnhancedTaskStateManager } from './enhanced-task-state-manager';
import { EnhancedQueueEventHandlerManager } from './enhanced-queue-event-handlers';
import { EnhancedJobManager } from './enhanced-job-manager';
import { logger, ErrorCategory } from './logger';

/**
 * Integration bridge between the legacy task system and enhanced task management
 * This handles the transition from the old taskStatus system to the new enhanced system
 */
export class TaskIntegrationBridge {
  private orchestrator: EnhancedTaskOrchestrator;
  private stateManager: EnhancedTaskStateManager;
  private legacyTaskStatus: Map<string, any>;

  constructor(
    stateManager: EnhancedTaskStateManager,
    queueHandler: EnhancedQueueEventHandlerManager,
    jobManager: EnhancedJobManager,
    legacyTaskStatus: Map<string, any>
  ) {
    this.stateManager = stateManager;
    this.orchestrator = new EnhancedTaskOrchestrator(stateManager, queueHandler, jobManager);
    this.legacyTaskStatus = legacyTaskStatus;
  }

  /**
   * Bridge function to start a task using the enhanced system
   * This should be called after the legacy startTask sets up initial conditions
   */
  async startEnhancedTask(task: ITask): Promise<void> {
    try {
      // console.log(`[TaskBridge] Starting enhanced task execution for ${task._id}`);
      
      // Initialize state change listener first
      this.orchestrator.initializeTaskStateListener(task._id);
      
      // Set up legacy status sync
      this.setupLegacyStatusSync(task._id);
      
      // Start the enhanced task flow
      await this.orchestrator.startTask(task._id, task);
      
    } catch (error) {
      await logger.logError(ErrorCategory.TASK_MANAGEMENT, 'Error starting enhanced task', {
        taskId: task._id,
        error: error as Error
      });
    }
  }

  /**
   * Bridge function to stop a task using the enhanced system
   */
  async stopEnhancedTask(taskId: string, marketplace?: string): Promise<void> {
    try {
      // console.log(`[TaskBridge] Stopping enhanced task ${taskId}${marketplace ? ` for ${marketplace}` : ''}`);
      
      await this.orchestrator.stopTask(taskId, marketplace);
      
    } catch (error) {
      await logger.logError(ErrorCategory.TASK_MANAGEMENT, 'Error stopping enhanced task', {
        taskId,
        marketplace,
        error: error as Error
      });
    }
  }

  /**
   * Sync enhanced task state with legacy taskStatus
   * This ensures backward compatibility with existing WebSocket and UI code
   */
  private setupLegacyStatusSync(taskId: string): void {
    this.stateManager.onStateChange(taskId, (oldState, newState) => {
      try {
        // Get enhanced state
        const enhancedState = this.stateManager.getTaskState(taskId);
        if (!enhancedState) return;

        // Update legacy taskStatus to maintain compatibility
        const legacyStatus = this.legacyTaskStatus.get ? this.legacyTaskStatus.get(taskId) : (this.legacyTaskStatus as any)[taskId];
        
        if (legacyStatus || this.legacyTaskStatus.set) {
          const updatedLegacyStatus = {
            ...legacyStatus,
            status: this.mapEnhancedStatusToLegacy(newState),
            runningPercentage: enhancedState.runningPercentage,
            cancellationPercentage: enhancedState.cancellationPercentage,
            processingPercentage: enhancedState.processingPercentage,
            waitingTime: enhancedState.waitingTime,
            nextLoopTime: enhancedState.nextLoopTime
          };

          // Update legacy status (handle both Map and object cases)
          if (this.legacyTaskStatus.set) {
            this.legacyTaskStatus.set(taskId, updatedLegacyStatus);
          } else {
            (this.legacyTaskStatus as any)[taskId] = updatedLegacyStatus;
          }
        }

        // console.log(`[TaskBridge] Synced state for task ${taskId}: ${oldState} -> ${newState}`);
        
      } catch (error) {
        console.error(`[TaskBridge] Error syncing legacy status for task ${taskId}:`, error);
      }
    });
  }

  /**
   * Map enhanced status to legacy status for backward compatibility
   */
  private mapEnhancedStatusToLegacy(enhancedStatus: string): string {
    switch (enhancedStatus) {
      case 'STARTED':
        return 'STARTED';
      case 'PROCESSING':
        return 'STARTED'; // Legacy system doesn't have PROCESSING
      case 'QUEUEING':
        return 'STARTED'; // Legacy system doesn't have QUEUEING
      case 'QUEUED':
        return 'STARTED'; // Legacy system doesn't have QUEUED
      case 'RUNNING':
        return 'RUNNING';
      case 'COMPLETED':
        return 'COMPLETED';
      case 'SCHEDULED':
        return 'SCHEDULED';
      case 'CANCELLING':
        return 'CANCELLING';
      case 'STOPPED':
        return 'STOPPED';
      default:
        return 'STARTED';
    }
  }

  /**
   * Check if a task should use enhanced mode
   * For now, all tasks can use enhanced mode
   */
  shouldUseEnhancedMode(task: ITask): boolean {
    // Add any logic here to determine if a task should use enhanced mode
    // For now, enable for all tasks
    return true;
  }

  /**
   * Get comprehensive task status for monitoring
   */
  getTaskStatus(taskId: string): any {
    return this.orchestrator.getComprehensiveTaskStatus(taskId);
  }

  /**
   * Get orchestrator for advanced operations
   */
  getOrchestrator(): EnhancedTaskOrchestrator {
    return this.orchestrator;
  }

  /**
   * Clean up resources for a task
   */
  async cleanupTask(taskId: string): Promise<void> {
    await this.orchestrator.cleanupTask(taskId);
  }

  /**
   * Trigger next processing phase manually (for debugging)
   */
  async triggerProcessing(taskId: string): Promise<void> {
    const state = this.stateManager.getTaskState(taskId);
    if (state && state.status === 'STARTED') {
      await this.stateManager.transitionState(taskId, 'PROCESSING');
    }
  }

  /**
   * Force task state progression (for debugging)
   */
  async forceStateProgression(taskId: string): Promise<void> {
    const state = this.stateManager.getTaskState(taskId);
    if (!state) return;

    // console.log(`[TaskBridge] Forcing progression for task ${taskId} from ${state.status}`);

    switch (state.status) {
      case 'STARTED':
        await this.stateManager.startProcessing(taskId);
        break;
      case 'PROCESSING':
        await this.stateManager.updateTaskProgress(taskId, { processingPercentage: 100 });
        await this.stateManager.transitionState(taskId, 'QUEUEING');
        break;
      case 'QUEUEING':
        // Simulate all jobs queued
        await this.stateManager.updateTaskProgress(taskId, { queueingPercentage: 100 });
        await this.stateManager.transitionState(taskId, 'QUEUED');
        break;
      case 'QUEUED':
        await this.stateManager.transitionState(taskId, 'RUNNING');
        break;
    }
  }
}