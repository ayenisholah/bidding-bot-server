interface RetryOptions {
  maxRetries?: number;
  initialDelay?: number;
  maxDelay?: number;
  backoffMultiplier?: number;
}

const DEFAULT_RETRY_OPTIONS: Required<RetryOptions> = {
  maxRetries: 3,
  initialDelay: 100,
  maxDelay: 5000,
  backoffMultiplier: 2,
};

/**
 * Executes a database operation with retry logic for connection issues
 * @param operation The database operation to execute
 * @param options Retry configuration options
 * @returns The result of the operation
 */
export async function withRetry<T>(
  operation: () => Promise<T>,
  options: RetryOptions = {}
): Promise<T> {
  const config = { ...DEFAULT_RETRY_OPTIONS, ...options };
  let lastError: Error | undefined;
  let delay = config.initialDelay;

  for (let attempt = 1; attempt <= config.maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error as Error;

      // Check if it's a connection-related error
      const isConnectionError = 
        error instanceof Error && 
        (error.message.includes('Connection terminated') ||
         error.message.includes('Connection lost') ||
         error.message.includes('ECONNREFUSED') ||
         error.message.includes('connection') ||
         error.message.includes('timeout') ||
         error.message.includes('ETIMEDOUT'));

      if (!isConnectionError) {
        throw error; // Don't retry non-connection errors
      }

      if (attempt === config.maxRetries) {
        console.error(`[DB Retry] Failed after ${config.maxRetries} attempts:`, error);
        throw error;
      }

      console.warn(`[DB Retry] Attempt ${attempt} failed, retrying in ${delay}ms...`);
      await new Promise(resolve => setTimeout(resolve, delay));
      
      // Exponential backoff with jitter
      delay = Math.min(
        config.maxDelay,
        delay * config.backoffMultiplier + Math.random() * 100
      );
    }
  }

  throw lastError;
}