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

import { debugLog } from './debug-file-logger';

// Track pending requests globally
const pendingRequests = new Map<string, Promise<any>>();

// Stats for debugging
let stats = {
  totalRequests: 0,
  dedupedRequests: 0,
  activeRequests: 0,
};

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
    /** Log debug messages (default: true) */
    debug?: boolean;
    /** Category for debug logs (default: 'API') */
    category?: string;
  }
): Promise<T> {
  const { debug = true, category = 'API' } = options || {};

  stats.totalRequests++;

  // Check if there's already a pending request for this key
  const pending = pendingRequests.get(key);
  if (pending) {
    stats.dedupedRequests++;
    if (debug) {
      debugLog(category, `[Dedup] SHARED | key=${key} | active=${pendingRequests.size} | saved=${stats.dedupedRequests}/${stats.totalRequests}`);
    }
    return pending;
  }

  // No pending request - create new one
  stats.activeRequests++;
  if (debug) {
    debugLog(category, `[Dedup] NEW | key=${key} | active=${pendingRequests.size + 1}`);
  }

  const startTime = Date.now();

  // Create the request promise and track it
  const request = fetchFn()
    .then(result => {
      const elapsed = Date.now() - startTime;
      pendingRequests.delete(key);
      stats.activeRequests--;
      if (debug) {
        debugLog(category, `[Dedup] DONE | key=${key} | elapsed=${elapsed}ms | active=${pendingRequests.size}`);
      }
      return result;
    })
    .catch(error => {
      const elapsed = Date.now() - startTime;
      pendingRequests.delete(key);
      stats.activeRequests--;
      if (debug) {
        debugLog(category, `[Dedup] ERROR | key=${key} | elapsed=${elapsed}ms | error=${error?.message || 'unknown'}`);
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
