import { Prisma } from '@prisma/client';
import prisma from './prisma';

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
 * Executes a Prisma operation with retry logic for connection pool timeouts
 * @param operation The Prisma operation to execute
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

      // Check if it's a connection pool timeout error
      const isConnectionPoolError = 
        error instanceof Prisma.PrismaClientKnownRequestError &&
        error.message.includes('Timed out fetching a new connection from the connection pool');

      // Also retry on connection errors
      const isConnectionError = 
        error instanceof Error && 
        (error.message.includes('Connection terminated') ||
         error.message.includes('Connection lost') ||
         error.message.includes('ECONNREFUSED'));

      if (!isConnectionPoolError && !isConnectionError) {
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

/**
 * Wraps Prisma transaction with retry logic
 * @param transaction The transaction function
 * @param options Retry configuration options
 * @returns The result of the transaction
 */
export async function withTransactionRetry<T>(
  transaction: (tx: Prisma.TransactionClient) => Promise<T>,
  options: RetryOptions = {}
): Promise<T> {
  return withRetry(
    () => prisma.$transaction(transaction, {
      maxWait: 10000, // Maximum time to wait for a transaction slot
      timeout: 30000, // Maximum time for the transaction to complete
    }),
    options
  );
}