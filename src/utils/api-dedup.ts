/**
 * API Request Deduplication Utility
 *
 * Prevents duplicate concurrent API requests by tracking in-flight requests.
 * When multiple requests come in for the same resource, only the first one
 * makes the actual API call - subsequent requests share the same Promise.
 *
 * This solves the Bottleneck "duplicate job ID" error by removing the need
 * for static job IDs while still preventing duplicate API calls.
 */

import { warnLog } from './debug-file-logger';

// Track pending requests globally
const pendingRequests = new Map<string, Promise<any>>();

// Stats for debugging
let stats = {
  totalRequests: 0,
  dedupedRequests: 0,
  activeRequests: 0,
};

// Timeout for requests (2 minutes)
const REQUEST_TIMEOUT = 120000; // 2 minutes

/**
 * Execute an API call with automatic deduplication.
 *
 * @param key - Unique identifier for the request (e.g., 'opensea-floor-azuki')
 * @param fetchFn - The async function that makes the actual API call
 * @param options - Optional configuration
 * @returns The result from the API call (shared among concurrent requests)
 *
 * @example
 * // Instead of:
 * const data = await limiter.schedule({ id: 'api-opensea-floor-azuki' }, () => axios.get(...))
 *
 * // Use:
 * const data = await apiDedup('opensea-floor-azuki', () =>
 *   limiter.schedule({ priority: 0 }, () => axios.get(...))
 * )
 */
export async function apiDedup<T>(
  key: string,
  fetchFn: () => Promise<T>,
  options?: {
    /** Log debug messages (default: false - disable to reduce log noise) */
    debug?: boolean;
    /** Category for debug logs (default: 'API') */
    category?: string;
    /** Timeout in milliseconds (default: 120000 / 2 minutes) */
    timeout?: number;
  }
): Promise<T> {
  const { debug = false, category = 'API', timeout = REQUEST_TIMEOUT } = options || {};

  stats.totalRequests++;

  // Check if there's already a pending request for this key
  const pending = pendingRequests.get(key);
  if (pending) {
    stats.dedupedRequests++;
    if (debug) {
    }
    return pending;
  }

  // No pending request - create new one
  stats.activeRequests++;
  if (debug) {
  }

  // Create timeout promise
  const timeoutPromise = new Promise<never>((_, reject) => {
    setTimeout(() => reject(new Error(`Request timeout after ${timeout}ms`)), timeout);
  });

  // Race between request and timeout
  const request = Promise.race([fetchFn(), timeoutPromise])
    .then(result => {
      pendingRequests.delete(key);
      stats.activeRequests--;
      if (debug) {
      }
      return result;
    })
    .catch(error => {
      pendingRequests.delete(key);
      stats.activeRequests--;

      if (error?.message?.includes('timeout')) {
        warnLog(category, `[Dedup] TIMEOUT | key=${key} | Cleaned up stalled request`);
      } else if (debug) {
        warnLog(category, `[Dedup] ERROR | key=${key} | error=${error?.message || 'unknown'}`);
      }
      throw error;
    });

  pendingRequests.set(key, request);
  return request;
}

/**
 * Get current deduplication statistics
 */
export function getApiDedupStats() {
  return {
    ...stats,
    pendingCount: pendingRequests.size,
    pendingKeys: Array.from(pendingRequests.keys()),
  };
}

/**
 * Reset statistics (useful for testing)
 */
export function resetApiDedupStats() {
  stats = {
    totalRequests: 0,
    dedupedRequests: 0,
    activeRequests: 0,
  };
}

/**
 * Clear all pending requests (use with caution - mainly for shutdown)
 */
export function clearPendingRequests() {
  pendingRequests.clear();
  stats.activeRequests = 0;
}

export default apiDedup;
