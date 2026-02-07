import { ITask } from '../models/task.model';
/**
 * Task Index Service
 *
 * Provides fast O(1) task lookups by contract address or slug.
 * Maintains indexes for running tasks to optimize marketplace message routing.
 */
class TaskIndexService {
  // Contract address -> Set of tasks
  private contractToTasksMap = new Map<string, Set<ITask>>();

  // Slug -> Set of tasks
  private slugToTasksMap = new Map<string, Set<ITask>>();

  /**
   * Rebuild indexes from scratch with the given task list.
   * Clears existing indexes and repopulates them.
   */
  public updateTaskIndexes(tasks: ITask[]): void {
    this.contractToTasksMap.clear();
    this.slugToTasksMap.clear();

    tasks.forEach(task => {
      if (!task.running || !task.contract) return;

      const contractKey = task.contract.contractAddress?.toLowerCase();
      const slugKey = task.contract.slug?.toLowerCase();

      if (contractKey) {
        if (!this.contractToTasksMap.has(contractKey)) {
          this.contractToTasksMap.set(contractKey, new Set());
        }
        this.contractToTasksMap.get(contractKey)!.add(task);
      }

      if (slugKey) {
        if (!this.slugToTasksMap.has(slugKey)) {
          this.slugToTasksMap.set(slugKey, new Set());
        }
        this.slugToTasksMap.get(slugKey)!.add(task);
      }
    });
  }

  /**
   * Fast task lookup - O(1) instead of O(n).
   * Returns tasks matching the given contract address or slug.
   */
  public getRelevantTasks(contractAddress?: string, slug?: string): ITask[] {
    const tasks = new Set<ITask>();

    if (contractAddress) {
      const contractTasks = this.contractToTasksMap.get(contractAddress.toLowerCase());
      if (contractTasks) {
        contractTasks.forEach(task => tasks.add(task));
      }
    }

    if (slug) {
      const slugTasks = this.slugToTasksMap.get(slug.toLowerCase());
      if (slugTasks) {
        slugTasks.forEach(task => tasks.add(task));
      }
    }
    return Array.from(tasks);
  }

  /**
   * Add a single task to the indexes.
   * Only indexes running tasks with contract information.
   */
  public addTaskToIndexes(task: ITask): void {
    if (!task.running || !task.contract) return;

    const taskId = task._id.toString();
    const contractKey = task.contract.contractAddress?.toLowerCase();
    const slugKey = task.contract.slug?.toLowerCase();

    if (contractKey) {
      if (!this.contractToTasksMap.has(contractKey)) {
        this.contractToTasksMap.set(contractKey, new Set());
      }
      this.contractToTasksMap.get(contractKey)!.add(task);
    }

    if (slugKey) {
      if (!this.slugToTasksMap.has(slugKey)) {
        this.slugToTasksMap.set(slugKey, new Set());
      }
      this.slugToTasksMap.get(slugKey)!.add(task);
    }

  }

  /**
   * Remove a single task from the indexes.
   *
   * Note: Uses taskId matching instead of reference equality because task objects
   * are recreated via spread operator on start/stop, making Set.delete(task) fail silently.
   */
  public removeTaskFromIndexes(task: ITask): void {
    const taskId = task._id.toString();
    const contractKey = task.contract?.contractAddress?.toLowerCase();
    const slugKey = task.contract?.slug?.toLowerCase();

    let removedFromContract = false;
    let removedFromSlug = false;

    if (contractKey && this.contractToTasksMap.has(contractKey)) {
      const contractTasks = this.contractToTasksMap.get(contractKey)!;
      // Find task by ID instead of reference (Set.delete uses reference equality)
      for (const t of contractTasks) {
        if (t._id.toString() === taskId) {
          contractTasks.delete(t);
          removedFromContract = true;
          break;
        }
      }
      if (contractTasks.size === 0) {
        this.contractToTasksMap.delete(contractKey);
      }
    }

    if (slugKey && this.slugToTasksMap.has(slugKey)) {
      const slugTasks = this.slugToTasksMap.get(slugKey)!;
      // Find task by ID instead of reference (Set.delete uses reference equality)
      for (const t of slugTasks) {
        if (t._id.toString() === taskId) {
          slugTasks.delete(t);
          removedFromSlug = true;
          break;
        }
      }
      if (slugTasks.size === 0) {
        this.slugToTasksMap.delete(slugKey);
      }
    }

  }

  /**
   * Get the internal contract-to-tasks map.
   * Used for special cases like Blur message handling.
   */
  public getContractToTasksMap(): Map<string, Set<ITask>> {
    return this.contractToTasksMap;
  }

  /**
   * Get statistics about the current indexes
   */
  public getStats(): { contractCount: number; slugCount: number } {
    return {
      contractCount: this.contractToTasksMap.size,
      slugCount: this.slugToTasksMap.size
    };
  }
}

export const taskIndexService = new TaskIndexService();
