import { orderCache } from './order-cache';
import { bidStats } from '../state/global-state';
import { debugLog, warnLog } from './debug-file-logger';

/**
 * Reconciles bidStats against Redis cache (source of truth)
 * Runs every 30 seconds for active tasks
 */
export async function reconcileBidStatsWithCache(taskIds: string[]): Promise<void> {
  for (const taskId of taskIds) {
    try {
      const cacheCounts = await orderCache.countGroupedByMarketplace(taskId);
      const statsCounts = bidStats[taskId] || { opensea: 0, magiceden: 0, blur: 0 };

      const marketplaces = ['opensea', 'magiceden', 'blur'] as const;
      let corrected = false;

      for (const mp of marketplaces) {
        const cacheCount = cacheCounts[mp] || 0;
        const statsCount = statsCounts[mp] || 0;

        if (cacheCount !== statsCount) {
          warnLog('Reconciliation',
            `DRIFT_DETECTED | taskId=${taskId} | marketplace=${mp} | ` +
            `cache=${cacheCount} | bidStats=${statsCount}`);

          bidStats[taskId][mp] = cacheCount;
          corrected = true;
        }
      }

      if (corrected) {
        debugLog('Reconciliation',
          `CORRECTED | taskId=${taskId} | ` +
          `newCounts=${JSON.stringify(bidStats[taskId])}`);
      }
    } catch (error) {
      warnLog('Reconciliation',
        `FAILED | taskId=${taskId} | ` +
        `error=${error instanceof Error ? error.message : String(error)}`);
    }
  }
}
