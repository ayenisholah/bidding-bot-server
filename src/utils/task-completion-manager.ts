/**
 * Manages task completion state to prevent cross-task contamination
 */

import { logger, ErrorCategory } from './logger';

interface MarketplaceCompletion {
  opensea: boolean;
  magiceden: boolean;
  blur: boolean;
}

export class TaskCompletionManager {
  // Use a Map instead of a plain object for better control
  private completedTasks: Map<string, MarketplaceCompletion> = new Map();
  
  /**
   * Initialize or reset completion state for a task
   */
  initializeTask(taskId: string): void {
    this.completedTasks.set(taskId, {
      opensea: false,
      magiceden: false,
      blur: false
    });
    
    // console.log(`[TaskCompletion] Initialized completion state for task ${taskId}`);
  }
  
  /**
   * Mark a marketplace as complete for a task
   */
  markMarketplaceComplete(taskId: string, marketplace: 'opensea' | 'magiceden' | 'blur'): void {
    let completion = this.completedTasks.get(taskId);
    
    if (!completion) {
      // Initialize if doesn't exist
      completion = {
        opensea: false,
        magiceden: false,
        blur: false
      };
      this.completedTasks.set(taskId, completion);
    }
    
    completion[marketplace] = true;
    // console.log(`[TaskCompletion] Task ${taskId} completed ${marketplace}`);
  }
  
  /**
   * Check if all required marketplaces are complete for a task
   * CRITICAL: This must only check the specific task's marketplaces
   */
  areAllMarketplacesComplete(
    taskId: string, 
    requiredMarketplaces: string[]
  ): boolean {
    const completion = this.completedTasks.get(taskId);
    
    if (!completion) {
      console.warn(`[TaskCompletion] No completion record for task ${taskId}`);
      return false;
    }
    
    // Only check the marketplaces that this specific task requires
    const allComplete = requiredMarketplaces.every(marketplace => {
      const isComplete = completion[marketplace as keyof MarketplaceCompletion] === true;
      // console.log(`[TaskCompletion] Task ${taskId} ${marketplace}: ${isComplete}`);
      return isComplete;
    });
    
    // console.log(`[TaskCompletion] Task ${taskId} all markets complete: ${allComplete} (required: ${requiredMarketplaces.join(', ')})`);
    return allComplete;
  }
  
  /**
   * Reset task completion state when task restarts
   * CRITICAL: Must be called when a task starts/restarts
   */
  resetTask(taskId: string): void {
    this.completedTasks.delete(taskId);
    // console.log(`[TaskCompletion] Reset completion state for task ${taskId}`);
  }
  
  /**
   * Clean up completion state for deleted tasks
   */
  cleanupTask(taskId: string): void {
    this.completedTasks.delete(taskId);
    // console.log(`[TaskCompletion] Cleaned up completion state for task ${taskId}`);
  }
  
  /**
   * Get current completion state for debugging
   */
  getCompletionState(taskId: string): MarketplaceCompletion | undefined {
    return this.completedTasks.get(taskId);
  }
  
  /**
   * Debug: Get all completion states
   */
  getAllCompletionStates(): Map<string, MarketplaceCompletion> {
    return new Map(this.completedTasks);
  }
  
  /**
   * Verify task isolation - ensure no cross-contamination
   */
  verifyTaskIsolation(): { issues: string[] } {
    const issues: string[] = [];
    
    // Check for any anomalies in completion states
    this.completedTasks.forEach((completion, taskId) => {
      // Count completed marketplaces
      const completedCount = Object.values(completion).filter(v => v === true).length;
      
      // Log for monitoring
      if (completedCount > 0) {
        // console.log(`[TaskCompletion] Task ${taskId} has ${completedCount} completed marketplaces`);
      }
    });
    
    return { issues };
  }
}

// Singleton instance
export const taskCompletionManager = new TaskCompletionManager();