/**
 * Validates and fixes task completion state inconsistencies
 */

import { ITask } from '../models/task.model';

interface TaskStatus {
  status: string;
  runningPercentage: number;
  cancellationPercentage: number;
  waitingTime: number;
  processingPercentage: number;
  nextLoopTime?: Date;
  lastUpdate?: number;
}

interface ParentJob {
  totalJobs: number;
  completedJobs: number;
  failedJobs: number;
  taskId: string;
}

interface CompletedMarketplace {
  opensea: boolean;
  magiceden: boolean;
  blur: boolean;
}

export class TaskCompletionValidator {
  /**
   * Check if a task should be completed based on job completion state
   */
  static shouldTaskBeCompleted(
    taskId: string,
    task: ITask,
    parentJobs: Map<string, ParentJob>,
    enabledMarketplaces: string[]
  ): {
    shouldComplete: boolean;
    reason: string;
    completionData: {
      totalJobs: number;
      completedJobs: number;
      failedJobs: number;
      marketplaceStatus: { [key: string]: any };
    }
  } {
    const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
    const enabledAndSelected = selectedMarketplaces.filter(m =>
      enabledMarketplaces.map(em => em.toLowerCase()).includes(m)
    );

    let totalJobs = 0;
    let totalCompleted = 0;
    let totalFailed = 0;
    const marketplaceStatus: { [key: string]: any } = {};

    // Check each marketplace
    for (const marketplace of enabledAndSelected) {
      const parentId = `${taskId}-${marketplace}`.toLowerCase();
      const parentJob = parentJobs.get(parentId);
      
      if (parentJob) {
        const completed = parentJob.completedJobs;
        const failed = parentJob.failedJobs;
        const total = parentJob.totalJobs;
        const finished = completed + failed;
        
        totalJobs += total;
        totalCompleted += completed;
        totalFailed += failed;
        
        marketplaceStatus[marketplace] = {
          completed,
          failed,
          total,
          finished,
          isComplete: finished >= total,
          percentage: total > 0 ? (finished / total) * 100 : 100
        };
      } else {
        // No parent job means no jobs were created for this marketplace
        marketplaceStatus[marketplace] = {
          completed: 0,
          failed: 0,
          total: 0,
          finished: 0,
          isComplete: true,
          percentage: 100
        };
      }
    }

    const overallPercentage = totalJobs > 0 ? ((totalCompleted + totalFailed) / totalJobs) * 100 : 100;
    
    // Determine if task should be completed
    let shouldComplete = false;
    let reason = '';

    if (totalJobs === 0) {
      shouldComplete = true;
      reason = 'No jobs were created for this task';
    } else if (overallPercentage >= 100) {
      const allMarketplacesComplete = enabledAndSelected.every(marketplace => 
        marketplaceStatus[marketplace]?.isComplete === true
      );
      
      if (allMarketplacesComplete) {
        shouldComplete = true;
        reason = 'All marketplaces completed';
      } else {
        shouldComplete = true;
        reason = `Overall percentage is 100% (${totalCompleted + totalFailed}/${totalJobs} jobs finished) but not all marketplaces marked complete - forcing completion`;
      }
    } else {
      reason = `Not complete: ${overallPercentage.toFixed(1)}% (${totalCompleted + totalFailed}/${totalJobs})`;
    }

    return {
      shouldComplete,
      reason,
      completionData: {
        totalJobs,
        completedJobs: totalCompleted,
        failedJobs: totalFailed,
        marketplaceStatus
      }
    };
  }

  /**
   * Validate and fix stuck tasks
   */
  static validateTaskState(
    taskId: string,
    taskStatus: TaskStatus,
    task: ITask,
    parentJobs: Map<string, ParentJob>,
    enabledMarketplaces: string[]
  ): {
    isStuck: boolean;
    suggestedAction: 'complete' | 'reset' | 'none';
    reason: string;
    details: any;
  } {
    const validation = this.shouldTaskBeCompleted(taskId, task, parentJobs, enabledMarketplaces);
    
    // Check if task is stuck
    const isRunningAt100 = taskStatus.status === 'RUNNING' && taskStatus.runningPercentage >= 100;
    const shouldBeCompleted = validation.shouldComplete;
    const lastUpdateAge = taskStatus.lastUpdate ? Date.now() - taskStatus.lastUpdate : 0;
    
    if (isRunningAt100 && shouldBeCompleted && lastUpdateAge > 10000) { // Stuck for more than 10 seconds
      return {
        isStuck: true,
        suggestedAction: 'complete',
        reason: `Task stuck at RUNNING with 100% completion for ${Math.round(lastUpdateAge / 1000)}s. ${validation.reason}`,
        details: validation.completionData
      };
    }
    
    if (taskStatus.status === 'RUNNING' && taskStatus.runningPercentage < 100 && !shouldBeCompleted) {
      return {
        isStuck: false,
        suggestedAction: 'none',
        reason: 'Task is properly running',
        details: validation.completionData
      };
    }

    return {
      isStuck: false,
      suggestedAction: 'none',
      reason: 'Task state appears normal',
      details: validation.completionData
    };
  }

  /**
   * Get diagnostic information for a task
   */
  static getDiagnostics(
    taskId: string,
    taskStatus: TaskStatus,
    task: ITask | undefined,
    parentJobs: Map<string, ParentJob>,
    completedTasks: { [key: string]: CompletedMarketplace },
    enabledMarketplaces: string[]
  ) {
    if (!task) {
      return {
        taskId,
        error: 'Task not found in activeTasks',
        taskStatus,
        parentJobs: [],
        completedTasksEntry: completedTasks[taskId]
      };
    }

    const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
    const enabledAndSelected = selectedMarketplaces.filter(m =>
      enabledMarketplaces.map(em => em.toLowerCase()).includes(m)
    );

    const parentJobDiagnostics = enabledAndSelected.map(marketplace => {
      const parentId = `${taskId}-${marketplace}`.toLowerCase();
      const parentJob = parentJobs.get(parentId);
      return {
        marketplace,
        parentId,
        exists: !!parentJob,
        ...parentJob
      };
    });

    const validation = this.shouldTaskBeCompleted(taskId, task, parentJobs, enabledMarketplaces);
    const stateValidation = this.validateTaskState(taskId, taskStatus, task, parentJobs, enabledMarketplaces);

    return {
      taskId,
      taskName: (task as any).name || task.contract?.slug,
      taskRunning: task.running,
      selectedMarketplaces,
      enabledAndSelected,
      taskStatus,
      completedTasksEntry: completedTasks[taskId],
      parentJobDiagnostics,
      validation,
      stateValidation,
      recommendations: this.getRecommendations(stateValidation, validation)
    };
  }

  private static getRecommendations(
    stateValidation: any,
    completionValidation: any
  ): string[] {
    const recommendations: string[] = [];

    if (stateValidation.isStuck) {
      recommendations.push(`URGENT: ${stateValidation.reason}`);
      recommendations.push(`Action: ${stateValidation.suggestedAction}`);
    }

    if (completionValidation.shouldComplete) {
      recommendations.push(`Task should be completed: ${completionValidation.reason}`);
    }

    if (recommendations.length === 0) {
      recommendations.push('Task appears to be functioning normally');
    }

    return recommendations;
  }
}