import { BoundedCache } from '../utils/bounded-cache';
import { debugLog } from '../utils/debug-file-logger';

// Constants
const DEBUG = process.env.DEBUG === 'true';

// Type definitions
interface PendingCounterbidEntry {
  timestamp: number;
  priceWei: number;  // Current queued price in wei
  jobId: string;     // BullMQ job ID for updating
}

interface MarketplaceTopOffer {
  topOfferAmount: number;    // Top offer in ETH
  topOfferWei: string;       // Top offer in wei (for precision)
  topBidderAddress: string;  // Address of top bidder (ours or competitor)
  isOurBid: boolean;         // true = we are the top bidder
  timestamp: number;         // When this was last verified
}

/**
 * Counterbid Tracking Service
 *
 * Handles all counterbid-related state management:
 * - Pending counterbid job tracking (prevents duplicate bids on same token)
 * - Marketplace top offer caching (prevents unnecessary counterbids)
 * - Deduplication utilities for seen items
 *
 * This service coordinates bid timing and prevents race conditions across
 * marketplace WebSocket handlers.
 */
class CounterbidTrackingService {
  // Track tokens with pending counterbid jobs
  // Key format: "counterbid-{taskId}-{marketplace}-{tokenId}" → PendingCounterbidEntry
  private pendingCounterbids = new BoundedCache<string, PendingCounterbidEntry>(
    10000,
    60 * 1000, // 60 second TTL
    'pendingCounterbids'
  );

  // Marketplace top offer cache
  // Key format: "{taskId}:{marketplace}:{identifier}"
  // Cache with 10k capacity and 5 min TTL (stale data auto-expires)
  private marketplaceTopOfferCache = new BoundedCache<string, MarketplaceTopOffer>(
    10000,
    5 * 60 * 1000, // 5 min TTL
    'marketplaceTopOfferCache'
  );

  // Index for efficient cleanup by taskId
  private topOfferTaskIndex = new Map<string, Set<string>>();

  // =============================================================================
  // PENDING COUNTERBID MANAGEMENT
  // =============================================================================

  /**
   * Mark a token as having a pending counterbid job.
   * Returns true if marked successfully, false if another counterbid is already pending.
   */
  public markCounterbidPending(
    taskId: string,
    marketplace: string,
    tokenId: string,
    priceWei: number,
    jobId: string
  ): boolean {
    const key = `counterbid-${taskId}-${marketplace}-${tokenId}`;
    return this.pendingCounterbids.addIfNotExists(key, {
      timestamp: Date.now(),
      priceWei,
      jobId
    });
  }

  /**
   * Get the pending counterbid entry for a token, if any.
   */
  public getPendingCounterbid(
    taskId: string,
    marketplace: string,
    tokenId: string
  ): PendingCounterbidEntry | undefined {
    const key = `counterbid-${taskId}-${marketplace}-${tokenId}`;
    return this.pendingCounterbids.get(key);
  }

  /**
   * Update the price in a pending counterbid entry.
   */
  public updatePendingCounterbidPrice(
    taskId: string,
    marketplace: string,
    tokenId: string,
    newPriceWei: number
  ): void {
    const key = `counterbid-${taskId}-${marketplace}-${tokenId}`;
    const existing = this.pendingCounterbids.get(key);
    if (existing) {
      this.pendingCounterbids.set(key, { ...existing, priceWei: newPriceWei });
    }
  }

  /**
   * Clear the pending counterbid status for a token after the job completes.
   */
  public clearCounterbidPending(taskId: string, marketplace: string, tokenId: string): void {
    const key = `counterbid-${taskId}-${marketplace}-${tokenId}`;
    this.pendingCounterbids.delete(key);
  }

  // =============================================================================
  // MARKETPLACE TOP OFFER CACHE
  // =============================================================================
  // Tracks the marketplace's ACTUAL top offer (including competitor bids) for each bid slot.
  // This is DIFFERENT from orderCache which only tracks OUR placed orders.
  //
  // Purpose:
  // - Regular bid processors: ALWAYS check API, update cache on success
  // - Counterbid handlers: Cache-first, API fallback on miss
  // - Prevents counterbidding when we're already the top bidder
  // - Prevents placing bids that won't become the top offer
  //
  // Cache key format: "{taskId}:{marketplace}:{identifier}"
  // - Token: "abc123:opensea:17855"
  // - Trait: "abc123:opensea:Background:Blue"
  // - Collection: "abc123:opensea:collection"
  // =============================================================================

  /**
   * Get cache key for marketplace top offer lookup
   */
  public getTopOfferCacheKey(taskId: string, marketplace: string, identifier: string): string {
    return `${taskId}:${marketplace.toLowerCase()}:${identifier}`;
  }

  /**
   * Get cached marketplace top offer
   */
  public getMarketplaceTopOffer(taskId: string, marketplace: string, identifier: string): MarketplaceTopOffer | undefined {
    const key = this.getTopOfferCacheKey(taskId, marketplace, identifier);
    return this.marketplaceTopOfferCache.get(key);
  }

  /**
   * Set marketplace top offer in cache
   */
  public setMarketplaceTopOffer(
    taskId: string,
    marketplace: string,
    identifier: string,
    topOffer: MarketplaceTopOffer
  ): void {
    const key = this.getTopOfferCacheKey(taskId, marketplace, identifier);
    this.marketplaceTopOfferCache.set(key, topOffer);

    // Maintain task index for cleanup
    if (!this.topOfferTaskIndex.has(taskId)) {
      this.topOfferTaskIndex.set(taskId, new Set());
    }
    this.topOfferTaskIndex.get(taskId)!.add(key);
  }

  /**
   * Update cache when we successfully place a bid (we become top bidder)
   */
  public updateTopOfferAfterOurBid(
    taskId: string,
    marketplace: string,
    identifier: string,
    ourBidAmountEth: number,
    ourBidAmountWei: string,
    ourWalletAddress: string
  ): void {
    this.setMarketplaceTopOffer(taskId, marketplace, identifier, {
      topOfferAmount: ourBidAmountEth,
      topOfferWei: ourBidAmountWei,
      topBidderAddress: ourWalletAddress.toLowerCase(),
      isOurBid: true,
      timestamp: Date.now()
    });
  }

  /**
   * Update cache when incoming WebSocket bid is higher than cached
   */
  public updateTopOfferFromIncomingBid(
    taskId: string,
    marketplace: string,
    identifier: string,
    incomingAmountEth: number,
    incomingAmountWei: string,
    incomingBidderAddress: string,
    ourWalletAddress: string
  ): void {
    const isOurBid = incomingBidderAddress.toLowerCase() === ourWalletAddress.toLowerCase();
    this.setMarketplaceTopOffer(taskId, marketplace, identifier, {
      topOfferAmount: incomingAmountEth,
      topOfferWei: incomingAmountWei,
      topBidderAddress: incomingBidderAddress.toLowerCase(),
      isOurBid,
      timestamp: Date.now()
    });
  }

  /**
   * Clear all cached top offers for a task (called on task stop)
   */
  public clearTopOffersByTask(taskId: string): void {
    const keys = this.topOfferTaskIndex.get(taskId);
    if (keys) {
      let count = 0;
      for (const key of keys) {
        this.marketplaceTopOfferCache.delete(key);
        count++;
      }
      this.topOfferTaskIndex.delete(taskId);
      if (DEBUG) {
        debugLog('TopOfferCache', `CLEAR task=${taskId} | removed ${count} entries`);
      }
    }
  }

  /**
   * Check if we need to counterbid based on cached top offer.
   * Returns: { shouldCounterbid: boolean, reason: string, cachedOffer?: MarketplaceTopOffer }
   */
  public checkShouldCounterbid(
    taskId: string,
    marketplace: string,
    identifier: string,
    incomingBidAmountEth: number,
    incomingBidderAddress: string,
    ourWalletAddress: string
  ): { shouldCounterbid: boolean; reason: string; cachedOffer?: MarketplaceTopOffer } {
    const cached = this.getMarketplaceTopOffer(taskId, marketplace, identifier);

    // No cached data - need to fetch from API
    if (!cached) {
      return { shouldCounterbid: true, reason: 'cache_miss' };
    }

    // Check if incoming bid is from us
    if (incomingBidderAddress.toLowerCase() === ourWalletAddress.toLowerCase()) {
      return { shouldCounterbid: false, reason: 'own_bid', cachedOffer: cached };
    }

    // Check if incoming bid is higher than our cached top offer
    if (incomingBidAmountEth > cached.topOfferAmount) {
      // Someone outbid us (or outbid the previous top bidder)
      return { shouldCounterbid: true, reason: 'outbid', cachedOffer: cached };
    }

    // Incoming bid is lower than or equal to cached top offer
    if (cached.isOurBid) {
      // We're still the top bidder
      return { shouldCounterbid: false, reason: 'still_top_bidder', cachedOffer: cached };
    }

    // Competitor is still top bidder (incoming bid didn't beat them)
    return { shouldCounterbid: false, reason: 'competitor_still_top', cachedOffer: cached };
  }

  // =============================================================================
  // SEEN ITEMS DEDUPLICATION HELPERS
  // =============================================================================

  /**
   * Add an item to a seen items cache
   */
  public addSeenItem(cache: BoundedCache<string, number>, key: string): void {
    const now = Date.now();
    cache.set(key, now);
    // BoundedCache handles cleanup automatically
  }

  /**
   * Check if an item has been seen before
   * Note: Manual .delete() calls after queuing counterbids are intentional.
   * They allow immediate re-processing of the same order/bid if needed.
   * The TTL system acts as a safety net for any items not manually deleted.
   */
  public hasSeenItem(cache: BoundedCache<string, number>, key: string): boolean {
    // BoundedCache.get() handles expiry check automatically
    return cache.has(key);
  }

  /**
   * Atomic check-and-add for deduplication: returns true if this is a NEW item (not seen before),
   * false if it was already seen. This prevents race conditions in concurrent handlers.
   */
  public markSeenIfNew(cache: BoundedCache<string, number>, key: string): boolean {
    const now = Date.now();
    return cache.addIfNotExists(key, now);
  }

  // =============================================================================
  // STATS & DIAGNOSTICS
  // =============================================================================

  /**
   * Get statistics about cache usage
   */
  public getStats(): {
    pendingCounterbids: number;
    topOffers: number;
    trackedTasks: number;
  } {
    return {
      pendingCounterbids: this.pendingCounterbids.size,
      topOffers: this.marketplaceTopOfferCache.size,
      trackedTasks: this.topOfferTaskIndex.size
    };
  }
}

export const counterbidTrackingService = new CounterbidTrackingService();
