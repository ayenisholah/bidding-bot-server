import { EventEmitter } from 'events';
import * as os from 'os';
import { logger } from './logger';

export interface MemoryStats {
  heapUsedMB: number;
  heapTotalMB: number;
  heapMaxMB: number;
  heapPercentOfMax: number;
  heapPercentOfTotal: number;
  rssMB: number;
  systemMemoryPercent: number;
  timestamp: number;
}

export interface MemoryThresholds {
  warning: number;  // Percentage of max heap
  critical: number; // Percentage of max heap
  pause: number;    // Percentage to pause operations
  resume: number;   // Percentage to resume operations
}

export class MemoryMonitor extends EventEmitter {
  private maxHeapMB: number;
  private thresholds: MemoryThresholds;
  private monitorInterval: NodeJS.Timer | null = null;
  private history: MemoryStats[] = [];
  private readonly maxHistorySize = 100;

  constructor(thresholds?: Partial<MemoryThresholds>) {
    super();
    
    // Get max heap from NODE_OPTIONS
    const maxHeapMatch = process.execArgv.join(' ').match(/--max-old-space-size=(\d+)/);
    this.maxHeapMB = maxHeapMatch ? parseInt(maxHeapMatch[1]) : 3072; // Default 3GB
    
    // Set thresholds
    this.thresholds = {
      warning: 70,
      critical: 85,
      pause: 80,
      resume: 60,
      ...thresholds
    };
  }

  /**
   * Get current memory statistics
   */
  getStats(): MemoryStats {
    const memUsage = process.memoryUsage();
    const totalSystemMemory = os.totalmem();
    
    const heapUsedMB = memUsage.heapUsed / 1024 / 1024;
    const heapTotalMB = memUsage.heapTotal / 1024 / 1024;
    const rssMB = memUsage.rss / 1024 / 1024;
    
    const heapPercentOfMax = (heapUsedMB / this.maxHeapMB) * 100;
    const heapPercentOfTotal = (memUsage.heapUsed / memUsage.heapTotal) * 100;
    const systemMemoryPercent = (memUsage.rss / totalSystemMemory) * 100;
    
    return {
      heapUsedMB,
      heapTotalMB,
      heapMaxMB: this.maxHeapMB,
      heapPercentOfMax,
      heapPercentOfTotal,
      rssMB,
      systemMemoryPercent,
      timestamp: Date.now()
    };
  }

  /**
   * Check memory status and emit events
   */
  checkMemory(): void {
    const stats = this.getStats();
    
    // Add to history
    this.history.push(stats);
    if (this.history.length > this.maxHistorySize) {
      this.history.shift();
    }
    
    // Emit stats event
    this.emit('stats', stats);
    
    // Check thresholds
    if (stats.heapPercentOfMax >= this.thresholds.critical) {
      this.emit('critical', stats);
      logger.logGeneralError('Critical memory usage detected', {
        heapUsedMB: stats.heapUsedMB,
        heapMaxMB: stats.heapMaxMB,
        percentage: stats.heapPercentOfMax
      });
    } else if (stats.heapPercentOfMax >= this.thresholds.warning) {
      this.emit('warning', stats);
    }
    
    // Check if we should pause/resume
    if (stats.heapPercentOfMax >= this.thresholds.pause) {
      this.emit('pause', stats);
    } else if (stats.heapPercentOfMax <= this.thresholds.resume) {
      this.emit('resume', stats);
    }
  }

  /**
   * Start monitoring memory
   */
  start(intervalMs = 10000): void {
    if (this.monitorInterval) {
      return;
    }
    
    this.monitorInterval = setInterval(() => {
      this.checkMemory();
    }, intervalMs);
    
    // Check immediately
    this.checkMemory();
  }

  /**
   * Stop monitoring
   */
  stop(): void {
    if (this.monitorInterval) {
      clearInterval(this.monitorInterval);
      this.monitorInterval = null;
    }
  }

  /**
   * Get memory trend (increasing/decreasing/stable)
   */
  getTrend(samples = 5): 'increasing' | 'decreasing' | 'stable' {
    if (this.history.length < samples) {
      return 'stable';
    }
    
    const recent = this.history.slice(-samples);
    const first = recent[0].heapUsedMB;
    const last = recent[recent.length - 1].heapUsedMB;
    const diff = last - first;
    const percentChange = (diff / first) * 100;
    
    if (percentChange > 5) return 'increasing';
    if (percentChange < -5) return 'decreasing';
    return 'stable';
  }

  /**
   * Get formatted memory status string
   */
  getStatusString(): string {
    const stats = this.getStats();
    const trend = this.getTrend();
    const trendIcon = trend === 'increasing' ? '📈' : trend === 'decreasing' ? '📉' : '➡️';
    
    return [
      `Memory Status ${trendIcon}`,
      `Heap: ${stats.heapUsedMB.toFixed(0)}MB / ${stats.heapMaxMB}MB (${stats.heapPercentOfMax.toFixed(1)}%)`,
      `Allocated: ${stats.heapTotalMB.toFixed(0)}MB (${stats.heapPercentOfTotal.toFixed(1)}% used)`,
      `RSS: ${stats.rssMB.toFixed(0)}MB (${stats.systemMemoryPercent.toFixed(1)}% of system)`,
    ].join('\n');
  }

  /**
   * Force garbage collection if available
   */
  forceGC(): boolean {
    if (global.gc) {
      const before = process.memoryUsage().heapUsed;
      global.gc();
      const after = process.memoryUsage().heapUsed;
      const freedMB = (before - after) / 1024 / 1024;
      
      logger.logGeneralInfo('Garbage collection completed', {
        freedMB: freedMB.toFixed(2),
        beforeMB: (before / 1024 / 1024).toFixed(2),
        afterMB: (after / 1024 / 1024).toFixed(2)
      });
      
      return true;
    }
    return false;
  }

  /**
   * Get average memory usage over time
   */
  getAverageUsage(minutes = 5): MemoryStats | null {
    const cutoff = Date.now() - (minutes * 60 * 1000);
    const relevantStats = this.history.filter(s => s.timestamp >= cutoff);
    
    if (relevantStats.length === 0) {
      return null;
    }
    
    const avgHeapUsed = relevantStats.reduce((sum, s) => sum + s.heapUsedMB, 0) / relevantStats.length;
    const avgHeapTotal = relevantStats.reduce((sum, s) => sum + s.heapTotalMB, 0) / relevantStats.length;
    const avgRss = relevantStats.reduce((sum, s) => sum + s.rssMB, 0) / relevantStats.length;
    
    return {
      heapUsedMB: avgHeapUsed,
      heapTotalMB: avgHeapTotal,
      heapMaxMB: this.maxHeapMB,
      heapPercentOfMax: (avgHeapUsed / this.maxHeapMB) * 100,
      heapPercentOfTotal: (avgHeapUsed / avgHeapTotal) * 100,
      rssMB: avgRss,
      systemMemoryPercent: (avgRss * 1024 * 1024 / os.totalmem()) * 100,
      timestamp: Date.now()
    };
  }
}

// Create a singleton instance
export const memoryMonitor = new MemoryMonitor({
  warning: 70,
  critical: 85,
  pause: 80,
  resume: 60
});