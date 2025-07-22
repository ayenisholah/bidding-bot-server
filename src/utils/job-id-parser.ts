/**
 * Utility to correctly parse job IDs based on their format
 */

export interface ParsedJobId {
  taskId: string;
  marketplace?: string;
  jobType?: string;
  isValid: boolean;
}

export class JobIdParser {
  /**
   * Parse a job ID based on its format
   */
  static parseJobId(jobId: string): ParsedJobId {
    if (!jobId) {
      return { taskId: '', isValid: false };
    }

    // Handle cancellation jobs: CANCEL_{MARKETPLACE}_BID-{taskId}-{orderId}
    if (jobId.includes('CANCEL_') && jobId.includes('_BID')) {
      const parts = jobId.split('-');
      if (parts.length >= 2) {
        const jobType = parts[0]; // e.g., "CANCEL_OPENSEA_BID"
        const taskId = parts[1];
        const marketplace = jobType.replace('CANCEL_', '').replace('_BID', '');
        
        return {
          taskId,
          marketplace,
          jobType: 'CANCEL',
          isValid: true
        };
      }
    }

    // Handle regular bidding jobs created by createJobKey or generateJobKey
    // Format: {taskId}-{...other parts}
    // The taskId is always the first part before the first hyphen
    const firstHyphenIndex = jobId.indexOf('-');
    if (firstHyphenIndex > 0) {
      const taskId = jobId.substring(0, firstHyphenIndex);
      
      // Try to extract marketplace from the remaining parts
      const remainingParts = jobId.substring(firstHyphenIndex + 1);
      let marketplace: string | undefined;
      
      // Check for marketplace indicators in the remaining parts
      if (remainingParts.includes('opensea')) {
        marketplace = 'opensea';
      } else if (remainingParts.includes('magiceden')) {
        marketplace = 'magiceden';
      } else if (remainingParts.includes('blur')) {
        marketplace = 'blur';
      }
      
      return {
        taskId,
        marketplace,
        jobType: 'BID',
        isValid: true
      };
    }

    return { taskId: '', isValid: false };
  }

  /**
   * Extract marketplace from job data instead of job ID
   * This is more reliable than parsing the job ID
   */
  static getMarketplaceFromJobName(jobName: string): string | null {
    if (jobName.includes('OPENSEA')) return 'opensea';
    if (jobName.includes('MAGICEDEN')) return 'magiceden';
    if (jobName.includes('BLUR')) return 'blur';
    return null;
  }

  /**
   * Validate that a taskId looks correct (MongoDB ObjectId format)
   */
  static isValidTaskId(taskId: string): boolean {
    // MongoDB ObjectIds are 24 character hex strings
    return /^[a-fA-F0-9]{24}$/.test(taskId);
  }
}

/**
 * IMPORTANT: The current job ID parsing in the queue event handlers is fragile
 * because it assumes fixed positions after splitting by '-', but job IDs can have
 * multiple hyphens in various positions.
 * 
 * Instead of relying on jobId parsing, consider:
 * 1. Storing taskId and marketplace in the job data
 * 2. Using a more robust delimiter (e.g., '::' or '|')
 * 3. Using this parser utility for safer extraction
 */