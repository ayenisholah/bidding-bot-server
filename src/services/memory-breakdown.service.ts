import { memorySnapshotService } from './memory-snapshot.service';
import { errorLog } from '../utils/debug-file-logger';
import * as os from 'os';
import * as child_process from 'child_process';
import { promisify } from 'util';

const exec = promisify(child_process.exec);

interface MemoryBreakdown {
  containerUsedMB: number;
  containerLimitMB: number;
  containerPercent: number;

  heapUsedMB: number;
  redisUsedMB: number;
  lruCachesMB: number;
  globalStateMB: number;
  systemResourcesMB: number;

  unaccountedMB: number;
  unaccountedPercent: number;

  activeHandles: number;
  activeRequests: number;
  logBufferSizeKB: number;
  logBufferEntries: number;
  timerCount: number;

  mallocedMemoryMB: number;
  peakMallocedMemoryMB: number;
  numberOfNativeContexts: number;
  numberOfDetachedContexts: number;

  growthRateMBPerHour: number;
  hoursToLimit: number;
}

export class MemoryBreakdownService {
  /**
   * Get container memory from Docker stats or process RSS
   */
  private async getContainerMemory(): Promise<{ usedMB: number; limitMB: number }> {
    try {
      // Try to get container stats via Docker API
      const { stdout } = await exec(
        'docker stats --no-stream --format "{{.MemUsage}}" $(hostname) 2>/dev/null || echo "0B / 0B"'
      );

      const match = stdout.trim().match(/([0-9.]+)([KMG])iB\s*\/\s*([0-9.]+)([KMG])iB/);

      if (match) {
        const [, usedNum, usedUnit, limitNum, limitUnit] = match;
        const usedMB = parseFloat(usedNum) * (usedUnit === 'G' ? 1024 : usedUnit === 'M' ? 1 : 0.001);
        const limitMB = parseFloat(limitNum) * (limitUnit === 'G' ? 1024 : limitUnit === 'M' ? 1 : 0.001);
        return { usedMB, limitMB };
      }
    } catch (err) {
      // Fallback to process memory
    }

    // Fallback: Use process RSS and system total memory
    const mem = process.memoryUsage();
    const totalMem = os.totalmem();
    return {
      usedMB: mem.rss / 1024 / 1024,
      limitMB: totalMem / 1024 / 1024
    };
  }

  /**
   * Calculate memory growth rate from recent snapshots
   */
  private async calculateGrowthRate(): Promise<{ growthRateMBPerHour: number; hoursToLimit: number }> {
    try {
      // Get snapshots from last 30 minutes
      const thirtyMinutesAgo = new Date(Date.now() - 30 * 60 * 1000);
      const MemorySnapshot = (await import('../models/memory-snapshot.model')).MemorySnapshot;

      const snapshots = await MemorySnapshot.find({
        timestamp: { $gte: thirtyMinutesAgo }
      }).sort({ timestamp: 1 }).limit(10).lean();

      if (snapshots.length < 2) {
        return { growthRateMBPerHour: 0, hoursToLimit: 0 };
      }

      // Calculate growth rate from first to last snapshot
      const firstSnap = snapshots[0];
      const lastSnap = snapshots[snapshots.length - 1];

      const heapGrowthMB = (lastSnap.processMemory.heapUsed - firstSnap.processMemory.heapUsed) / 1024 / 1024;
      const timeElapsedHours = (lastSnap.timestamp.getTime() - firstSnap.timestamp.getTime()) / (1000 * 60 * 60);

      const growthRateMBPerHour = timeElapsedHours > 0 ? heapGrowthMB / timeElapsedHours : 0;

      // Calculate hours to limit (if growing)
      let hoursToLimit = 0;
      if (growthRateMBPerHour > 0) {
        const currentHeapMB = lastSnap.processMemory.heapUsed / 1024 / 1024;
        const heapLimitMB = lastSnap.processMemory.heapTotal / 1024 / 1024;
        const remainingMB = heapLimitMB - currentHeapMB;
        hoursToLimit = remainingMB / growthRateMBPerHour;
      }

      return { growthRateMBPerHour, hoursToLimit };
    } catch (error) {
      errorLog('MemoryBreakdown', 'Error calculating growth rate', error);
      return { growthRateMBPerHour: 0, hoursToLimit: 0 };
    }
  }

  /**
   * Get complete memory breakdown
   */
  public async getBreakdown(): Promise<MemoryBreakdown> {
    try {
      // Get latest snapshot
      const snapshot = await memorySnapshotService.takeSnapshot();

      // Get container memory
      const container = await this.getContainerMemory();

      // Get growth rate
      const growth = await this.calculateGrowthRate();

      // Calculate identified memory
      const heapUsedMB = snapshot.processMemory.heapUsed / 1024 / 1024;
      const redisUsedMB = snapshot.redisMemory.totals.totalMemoryMB;
      const lruCachesMB = snapshot.lruCaches.totalEstimatedMemoryBytes / 1024 / 1024;
      const globalStateMB = 0.003; // ~3 KB from global state
      const systemResourcesMB = snapshot.systemResources.logBufferSizeKB / 1024; // Convert KB to MB

      const identifiedMB = heapUsedMB + redisUsedMB + lruCachesMB + globalStateMB + systemResourcesMB;
      const unaccountedMB = Math.max(0, container.usedMB - identifiedMB);
      const unaccountedPercent = (unaccountedMB / container.usedMB) * 100;

      return {
        containerUsedMB: container.usedMB,
        containerLimitMB: container.limitMB,
        containerPercent: (container.usedMB / container.limitMB) * 100,

        heapUsedMB,
        redisUsedMB,
        lruCachesMB,
        globalStateMB,
        systemResourcesMB,

        unaccountedMB,
        unaccountedPercent,

        activeHandles: snapshot.systemResources.activeHandles,
        activeRequests: snapshot.systemResources.activeRequests,
        logBufferSizeKB: snapshot.systemResources.logBufferSizeKB,
        logBufferEntries: snapshot.systemResources.logBufferEntries,
        timerCount: snapshot.systemResources.timerCount,

        mallocedMemoryMB: snapshot.v8HeapStats.mallocedMemory / 1024 / 1024,
        peakMallocedMemoryMB: snapshot.v8HeapStats.peakMallocedMemory / 1024 / 1024,
        numberOfNativeContexts: snapshot.v8HeapStats.numberOfNativeContexts,
        numberOfDetachedContexts: snapshot.v8HeapStats.numberOfDetachedContexts,

        growthRateMBPerHour: growth.growthRateMBPerHour,
        hoursToLimit: growth.hoursToLimit
      };
    } catch (error) {
      errorLog('MemoryBreakdown', 'Error getting memory breakdown', error);
      throw error;
    }
  }
}

export const memoryBreakdownService = new MemoryBreakdownService();
