/**
 * Centralized error statistics management with duplicate prevention
 */

export interface BidCounts {
  [key: string]: {
    opensea: number;
    magiceden: number;
    blur: number;
  };
}

export interface ErrorContext {
  bidType?: 'token' | 'trait' | 'collection';
  tokenId?: string | number;
  trait?: string;
  slug?: string;
  contractAddress?: string;
}

export class ErrorStatsManager {
  private errorStats: BidCounts = {};
  private seenJobIds: { [taskId: string]: Set<string> } = {};

  constructor() {}

  /**
   * Generate unique job ID based on task, marketplace, and context
   */
  private generateJobId(taskId: string, marketplace: string, context: ErrorContext): string {
    const { bidType, tokenId, trait } = context;
    
    if (bidType === 'token' && tokenId !== undefined) {
      return `${taskId}-${marketplace}-token-${tokenId}`;
    } else if (bidType === 'trait' && trait) {
      return `${taskId}-${marketplace}-trait-${trait}`;
    } else {
      return `${taskId}-${marketplace}-collection`;
    }
  }

  /**
   * Update error stats with duplicate prevention
   */
  updateErrorStats(taskId: string, marketplace: string, error: any, context: ErrorContext = {}): boolean {
    // Initialize task stats if not exists
    if (!this.errorStats[taskId]) {
      this.errorStats[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }

    // Initialize seen jobIds set for this task if not exists
    if (!this.seenJobIds[taskId]) {
      this.seenJobIds[taskId] = new Set<string>();
    }

    // Generate job ID for this error
    const jobId = this.generateJobId(taskId, marketplace, context);

    // Check if we've already recorded this error
    if (this.seenJobIds[taskId].has(jobId)) {
      return false; // Duplicate - not recorded
    }

    // Record the error
    this.seenJobIds[taskId].add(jobId);
    
    // Validate marketplace
    const validMarketplaces = ['opensea', 'magiceden', 'blur'] as const;
    if (!validMarketplaces.includes(marketplace as any)) {
      console.warn(`Invalid marketplace for error stats: ${marketplace}`);
      return false;
    }

    // Increment error count
    this.errorStats[taskId][marketplace as keyof BidCounts[string]]++;
    return true; // Successfully recorded
  }

  /**
   * Reset seen jobIds for a task (called when startTask is called without marketplace)
   */
  resetSeenJobIds(taskId: string): void {
    if (this.seenJobIds[taskId]) {
      this.seenJobIds[taskId].clear();
    }
  }

  /**
   * Reset error stats for a task
   */
  resetErrorStats(taskId: string): void {
    if (this.errorStats[taskId]) {
      this.errorStats[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }
  }

  /**
   * Get error stats for a task
   */
  getErrorStats(taskId?: string): BidCounts | BidCounts[string] {
    if (taskId) {
      return this.errorStats[taskId] || { opensea: 0, magiceden: 0, blur: 0 };
    }
    return this.errorStats;
  }

  /**
   * Get all error stats (for backward compatibility)
   */
  getAllErrorStats(): BidCounts {
    return this.errorStats;
  }

  /**
   * Clean up old task data
   */
  cleanup(taskId: string): void {
    delete this.errorStats[taskId];
    delete this.seenJobIds[taskId];
  }

  /**
   * Get debug info for a task
   */
  getDebugInfo(taskId: string): { errorStats: BidCounts[string]; seenJobIds: string[] } {
    return {
      errorStats: this.errorStats[taskId] || { opensea: 0, magiceden: 0, blur: 0 },
      seenJobIds: Array.from(this.seenJobIds[taskId] || [])
    };
  }
}

// Singleton instance
export const errorStatsManager = new ErrorStatsManager();