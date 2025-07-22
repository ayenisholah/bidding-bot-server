import { ITask } from '../models/task.model';

/**
 * Utility functions for preventing task duplication and maintaining consistency
 */

export class TaskDeduplicationManager {
  /**
   * Safely add a task to the currentTasks array, preventing duplicates
   */
  static addTaskToArray(currentTasks: ITask[], newTask: ITask): boolean {
    const existingIndex = currentTasks.findIndex(t => t._id === newTask._id);
    
    if (existingIndex === -1) {
      // Task doesn't exist, add it
      currentTasks.push(newTask);
      return true;
    } else {
      // Task exists, update it
      currentTasks[existingIndex] = newTask;
      return false;
    }
  }

  /**
   * Clear and repopulate the currentTasks array
   */
  static resetTaskArray(currentTasks: ITask[], newTasks: ITask[]): void {
    // Clear the array while maintaining the reference
    currentTasks.length = 0;
    
    // Add all new tasks
    currentTasks.push(...newTasks);
  }

  /**
   * Remove duplicate tasks from an array based on _id
   */
  static deduplicateTasks(tasks: ITask[]): ITask[] {
    const seen = new Set<string>();
    return tasks.filter(task => {
      const id = task._id.toString();
      if (seen.has(id)) {
        console.warn(`[Deduplication] Found duplicate task with ID: ${id}`);
        return false;
      }
      seen.add(id);
      return true;
    });
  }

  /**
   * Verify consistency between different task collections
   */
  static verifyTaskConsistency(
    currentTasks: ITask[],
    activeTasks: Map<string, ITask>,
    taskStatus: { [key: string]: any }
  ): { 
    inconsistencies: string[],
    duplicates: string[] 
  } {
    const inconsistencies: string[] = [];
    const duplicates: string[] = [];
    
    // Check for duplicates in currentTasks
    const idCount = new Map<string, number>();
    currentTasks.forEach(task => {
      const id = task._id.toString();
      idCount.set(id, (idCount.get(id) || 0) + 1);
    });
    
    idCount.forEach((count, id) => {
      if (count > 1) {
        duplicates.push(`Task ${id} appears ${count} times in currentTasks`);
      }
    });
    
    // Check consistency between collections
    currentTasks.forEach(task => {
      const id = task._id.toString();
      
      // Check if task exists in activeTasks
      if (!activeTasks.has(id) && task.running) {
        inconsistencies.push(`Task ${id} is in currentTasks but not in activeTasks`);
      }
      
      // Check if task has status
      if (!taskStatus[id] && task.running) {
        inconsistencies.push(`Task ${id} has no status entry`);
      }
    });
    
    // Check for orphaned entries in activeTasks
    activeTasks.forEach((task, id) => {
      const existsInCurrent = currentTasks.some(t => t._id.toString() === id);
      if (!existsInCurrent) {
        inconsistencies.push(`Task ${id} is in activeTasks but not in currentTasks`);
      }
    });
    
    return { inconsistencies, duplicates };
  }

  /**
   * Clean up orphaned task data
   */
  static cleanupOrphanedData(
    currentTasks: ITask[],
    activeTasks: Map<string, ITask>,
    taskStatus: { [key: string]: any },
    taskTimeouts: Map<string, NodeJS.Timeout>,
    taskCountdownTimers: Map<string, NodeJS.Timeout>
  ): number {
    let cleanedCount = 0;
    const validTaskIds = new Set(currentTasks.map(t => t._id.toString()));
    
    // Clean up activeTasks
    const activeTasksToDelete: string[] = [];
    activeTasks.forEach((task, id) => {
      if (!validTaskIds.has(id)) {
        activeTasksToDelete.push(id);
      }
    });
    
    activeTasksToDelete.forEach(id => {
      activeTasks.delete(id);
      cleanedCount++;
    });
    
    // Clean up taskStatus
    Object.keys(taskStatus).forEach(id => {
      if (!validTaskIds.has(id)) {
        delete taskStatus[id];
        cleanedCount++;
      }
    });
    
    // Clean up timers
    taskTimeouts.forEach((timeout, id) => {
      if (!validTaskIds.has(id)) {
        clearTimeout(timeout);
        taskTimeouts.delete(id);
        cleanedCount++;
      }
    });
    
    taskCountdownTimers.forEach((timer, id) => {
      if (!validTaskIds.has(id)) {
        clearInterval(timer);
        taskCountdownTimers.delete(id);
        cleanedCount++;
      }
    });
    
    return cleanedCount;
  }
}