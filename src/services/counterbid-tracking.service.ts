import { LRUCache } from 'lru-cache';
import { addIfNotExists } from '../utils/lru-helpers';
// Constants
const DEBUG = process.env.DEBUG === 'true';

/**
 * Counterbid Tracking Service
 *
 * Handles counterbid-related utilities:
 * - Deduplication utilities for seen items
 * - Execution latency tracking
 */
class CounterbidTrackingService {
  // Execution latency samples for performance monitoring
  // Key: unique sample ID, Value: { marketplace, latency, success, timestamp }
  private executionLatencySamples = new LRUCache<string, {
    marketplace: string;
    latency: number;
    success: boolean;
    timestamp: number;
  }>({
    max: 1000,     // Last 1000 samples
    ttl: 3600000   // 1 hour TTL
  });

  // Sample counter for unique IDs
  private executionSampleCounter = 0;

  // =============================================================================
  // MARKETPLACE TOP OFFER CACHE - NO-OP METHODS (Cache Removed)
  // =============================================================================
  // These methods are kept as no-ops for backward compatibility.
  // The marketplaceTopOfferCache was removed - all bids now fetch from API.
  // =============================================================================

  /**
   * Get cache key for marketplace top offer lookup (NO-OP)
   */
  public getTopOfferCacheKey(taskId: string, marketplace: string, identifier: string): string {
    return `${taskId}:${marketplace.toLowerCase()}:${identifier}`;
  }

  /**
   * Get cached marketplace top offer (NO-OP - always returns undefined)
   */
  public getMarketplaceTopOffer(_taskId: string, _marketplace: string, _identifier: string): {
    topOfferAmount: number;
    topOfferWei: string;
    topBidderAddress: string;
    isOurBid: boolean;
    timestamp: number;
  } | undefined {
    return undefined;
  }

  /**
   * Set marketplace top offer in cache (NO-OP)
   */
  public setMarketplaceTopOffer(
    _taskId: string,
    _marketplace: string,
    _identifier: string,
    _topOffer: any
  ): void {
    // No-op - cache removed
  }

  /**
   * Update cache when we successfully place a bid (NO-OP)
   */
  public updateTopOfferAfterOurBid(
    _taskId: string,
    _marketplace: string,
    _identifier: string,
    _ourBidAmountEth: number,
    _ourBidAmountWei: string,
    _ourWalletAddress: string
  ): void {
    // No-op - cache removed
  }

  /**
   * Update cache when incoming WebSocket bid is higher than cached (NO-OP)
   */
  public updateTopOfferFromIncomingBid(
    _taskId: string,
    _marketplace: string,
    _identifier: string,
    _incomingAmountEth: number,
    _incomingAmountWei: string,
    _incomingBidderAddress: string,
    _ourWalletAddress: string
  ): void {
    // No-op - cache removed
  }

  /**
   * Clear all cached top offers for a task (NO-OP)
   */
  public clearTopOffersByTask(_taskId: string): void {
    // No-op - cache removed
  }

  /**
   * Check if we need to counterbid (NO-OP - always returns true to fetch from API)
   * Returns: { shouldCounterbid: boolean, reason: string, cachedOffer?: MarketplaceTopOffer }
   */
  public checkShouldCounterbid(
    _taskId: string,
    _marketplace: string,
    _identifier: string,
    _incomingBidAmountEth: number,
    _incomingBidderAddress: string,
    _ourWalletAddress: string
  ): { shouldCounterbid: boolean; reason: string; cachedOffer?: {
    topOfferAmount: number;
    topOfferWei: string;
    topBidderAddress: string;
    isOurBid: boolean;
    timestamp: number;
  } } {
    // Always return true - fetch from API for real-time accuracy
    return { shouldCounterbid: true, reason: 'cache_disabled', cachedOffer: undefined };
  }


  // =============================================================================
  // SEEN ITEMS DEDUPLICATION HELPERS
  // =============================================================================

  /**
   * Add an item to a seen items cache
   */
  public addSeenItem(cache: LRUCache<string, number>, key: string): void {
    const now = Date.now();
    cache.set(key, now);
    // LRUCache handles cleanup automatically
  }

  /**
   * Check if an item has been seen before
   * Note: Manual .delete() calls after queuing counterbids are intentional.
   * They allow immediate re-processing of the same order/bid if needed.
   * The TTL system acts as a safety net for any items not manually deleted.
   */
  public hasSeenItem(cache: LRUCache<string, number>, key: string): boolean {
    // LRUCache.get() handles expiry check automatically
    return cache.has(key);
  }

  /**
   * Atomic check-and-add for deduplication: returns true if this is a NEW item (not seen before),
   * false if it was already seen. This prevents race conditions in concurrent handlers.
   */
  public markSeenIfNew(cache: LRUCache<string, number>, key: string): boolean {
    const now = Date.now();
    return addIfNotExists(cache, key, now);
  }

  // =============================================================================
  // STATS & DIAGNOSTICS
  // =============================================================================

  /**
   * Get statistics about cache usage
   */
  public getStats(): {
    executionSamples: number;
  } {
    return {
      executionSamples: this.executionLatencySamples.size
    };
  }

  // =============================================================================
  // EXECUTION LATENCY TRACKING (PLACEHOLDER - KEPT FOR COMPATIBILITY)
  // =============================================================================

  /**
   * Record execution completion for latency tracking
   */
  public recordExecutionComplete(
    marketplace: string,
    taskId: string,
    enqueuedAt: number,
    completedAt: number,
    success: boolean,
    tokenId?: string,
    _traceId?: string
  ): void {
    const latency = completedAt - enqueuedAt;
    const sampleId = `exec-${this.executionSampleCounter++}`;

    this.executionLatencySamples.set(sampleId, {
      marketplace: marketplace.toLowerCase(),
      latency,
      success,
      timestamp: completedAt
    });

    if (DEBUG) {
    }
  }

  /**
   * Get execution latency statistics for a marketplace (or all marketplaces)
   */
  public getExecutionLatencyStats(marketplace?: string): {
    p50: number; p95: number; p99: number; avg: number; max: number;
    sampleCount: number; successRate: number;
  } {
    // Collect samples for the specified marketplace (or all if not specified)
    const samples: number[] = [];
    let successCount = 0;
    let totalCount = 0;

    for (const sample of this.executionLatencySamples.values()) {
      // Filter by marketplace if specified
      if (marketplace && sample.marketplace !== marketplace.toLowerCase()) {
        continue;
      }

      samples.push(sample.latency);
      totalCount++;
      if (sample.success) {
        successCount++;
      }
    }

    // Return zeros if no samples
    if (samples.length === 0) {
      return { p50: 0, p95: 0, p99: 0, avg: 0, max: 0, sampleCount: 0, successRate: 0 };
    }

    // Sort samples for percentile calculation
    samples.sort((a, b) => a - b);

    // Calculate statistics
    const p50 = samples[Math.floor(samples.length * 0.5)] || 0;
    const p95 = samples[Math.floor(samples.length * 0.95)] || 0;
    const p99 = samples[Math.floor(samples.length * 0.99)] || 0;
    const max = samples[samples.length - 1] || 0;
    const avg = Math.round(samples.reduce((sum, val) => sum + val, 0) / samples.length);
    const successRate = totalCount > 0 ? Math.round((successCount / totalCount) * 100) : 0;

    return { p50, p95, p99, avg, max, sampleCount: samples.length, successRate };
  }

  /**
   * Get execution latency samples cache for debugging/monitoring
   * @internal - For use by lru-cache-sampler.ts only
   */
  public getExecutionLatencySamples(): LRUCache<string, { marketplace: string; latency: number; success: boolean; timestamp: number }> {
    return this.executionLatencySamples;
  }
}

export const counterbidTrackingService = new CounterbidTrackingService();

/**
 * Get execution latency samples cache for debugging/monitoring
 * @internal - For use by lru-cache-sampler.ts only
 */
export function getExecutionLatencySamples(): LRUCache<string, { marketplace: string; latency: number; success: boolean; timestamp: number }> {
  return counterbidTrackingService.getExecutionLatencySamples();
}
