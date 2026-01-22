/**
 * Order hash deduplication utilities
 * Prevents duplicate processing of WebSocket events
 */

const recentOrderHashes = new Set<string>();
let recentOrderHashesList: string[] = []; // For FIFO cleanup
const MAX_RECENT_HASHES = 5000;

/**
 * Check if an orderHash was recently processed - O(1) Set lookup
 * This provides early deduplication BEFORE any event processing to prevent event loop blocking
 */
export function isDuplicateOrderHash(orderHash: string): boolean {
  if (!orderHash) return false;

  if (recentOrderHashes.has(orderHash)) {
    return true; // DUPLICATE - skip processing entirely
  }

  // Add to tracking
  recentOrderHashes.add(orderHash);
  recentOrderHashesList.push(orderHash);

  // FIFO cleanup when exceeding limit
  if (recentOrderHashesList.length > MAX_RECENT_HASHES) {
    const oldest = recentOrderHashesList.shift();
    if (oldest) recentOrderHashes.delete(oldest);
  }

  return false; // NOT duplicate - process normally
}
