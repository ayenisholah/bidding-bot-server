import { performance } from 'perf_hooks';
import * as os from 'os';
import * as process from 'process';

interface PerformanceMetrics {
  functionName: string;
  executionTime: number;
  cpuUsageBefore: NodeJS.CpuUsage;
  cpuUsageAfter: NodeJS.CpuUsage;
  cpuUsageDelta: {
    user: number;
    system: number;
  };
  memoryBefore: NodeJS.MemoryUsage;
  memoryAfter: NodeJS.MemoryUsage;
  memoryDelta: {
    rss: number;
    heapTotal: number;
    heapUsed: number;
    external: number;
    arrayBuffers: number;
  };
  timestamp: Date;
}

export class PerformanceMonitor {
  private static metrics: PerformanceMetrics[] = [];
  private static aggregatedMetrics: Map<string, {
    count: number;
    totalTime: number;
    avgTime: number;
    minTime: number;
    maxTime: number;
    avgMemoryDelta: {
      heapUsed: number;
    };
  }> = new Map();

  static async measure<T>(
    functionName: string,
    fn: () => Promise<T>
  ): Promise<T> {
    const startTime = performance.now();
    const cpuUsageBefore = process.cpuUsage();
    const memoryBefore = process.memoryUsage();

    try {
      const result = await fn();
      
      const endTime = performance.now();
      const cpuUsageAfter = process.cpuUsage();
      const memoryAfter = process.memoryUsage();
      
      const executionTime = endTime - startTime;
      const cpuUsageDelta = process.cpuUsage(cpuUsageBefore);
      
      const metrics: PerformanceMetrics = {
        functionName,
        executionTime,
        cpuUsageBefore,
        cpuUsageAfter,
        cpuUsageDelta: {
          user: cpuUsageDelta.user / 1000, // Convert to milliseconds
          system: cpuUsageDelta.system / 1000
        },
        memoryBefore,
        memoryAfter,
        memoryDelta: {
          rss: memoryAfter.rss - memoryBefore.rss,
          heapTotal: memoryAfter.heapTotal - memoryBefore.heapTotal,
          heapUsed: memoryAfter.heapUsed - memoryBefore.heapUsed,
          external: memoryAfter.external - memoryBefore.external,
          arrayBuffers: memoryAfter.arrayBuffers - memoryBefore.arrayBuffers
        },
        timestamp: new Date()
      };
      
      this.metrics.push(metrics);
      this.updateAggregatedMetrics(metrics);
      
      // Log metrics in development
      if (process.env.NODE_ENV === 'development' || process.env.PERF_MONITOR === 'true') {
        // console.log(`[PERF] ${functionName}:`, {
          // executionTime: `${executionTime.toFixed(2)}ms`,
          // cpuUser: `${cpuUsageDelta.user}ms`,
          // cpuSystem: `${cpuUsageDelta.system}ms`,
          // memoryDelta: `${(metrics.memoryDelta.heapUsed / 1024 / 1024).toFixed(2)}MB`
        // });
      }
      
      return result;
    } catch (error) {
      const endTime = performance.now();
      console.error(`[PERF ERROR] ${functionName} failed after ${(endTime - startTime).toFixed(2)}ms:`, error);
      throw error;
    }
  }

  private static updateAggregatedMetrics(metrics: PerformanceMetrics): void {
    const existing = this.aggregatedMetrics.get(metrics.functionName) || {
      count: 0,
      totalTime: 0,
      avgTime: 0,
      minTime: Infinity,
      maxTime: 0,
      avgMemoryDelta: { heapUsed: 0 }
    };

    existing.count++;
    existing.totalTime += metrics.executionTime;
    existing.avgTime = existing.totalTime / existing.count;
    existing.minTime = Math.min(existing.minTime, metrics.executionTime);
    existing.maxTime = Math.max(existing.maxTime, metrics.executionTime);
    existing.avgMemoryDelta.heapUsed = 
      (existing.avgMemoryDelta.heapUsed * (existing.count - 1) + metrics.memoryDelta.heapUsed) / existing.count;

    this.aggregatedMetrics.set(metrics.functionName, existing);
  }

  static getMetrics(functionName?: string): PerformanceMetrics[] | PerformanceMetrics {
    if (functionName) {
      return this.metrics.filter(m => m.functionName === functionName);
    }
    return this.metrics;
  }

  static getAggregatedMetrics(functionName?: string) {
    if (functionName) {
      return this.aggregatedMetrics.get(functionName);
    }
    return Object.fromEntries(this.aggregatedMetrics);
  }

  static clearMetrics(): void {
    this.metrics = [];
    this.aggregatedMetrics.clear();
  }

  static getSystemInfo() {
    return {
      cpus: os.cpus(),
      totalMemory: os.totalmem(),
      freeMemory: os.freemem(),
      loadAvg: os.loadavg(),
      uptime: os.uptime(),
      platform: os.platform(),
      nodeVersion: process.version,
      processUptime: process.uptime(),
      processMemory: process.memoryUsage()
    };
  }

  static printReport(functionName?: string): void {
    // console.log('\n========== PERFORMANCE REPORT ==========');
    
    if (functionName) {
      const metrics = this.aggregatedMetrics.get(functionName);
      if (metrics) {
        // console.log(`\nFunction: ${functionName}`);
        // console.log(`  Calls: ${metrics.count}`);
        // console.log(`  Avg Time: ${metrics.avgTime.toFixed(2)}ms`);
        // console.log(`  Min Time: ${metrics.minTime.toFixed(2)}ms`);
        // console.log(`  Max Time: ${metrics.maxTime.toFixed(2)}ms`);
        // console.log(`  Avg Memory Delta: ${(metrics.avgMemoryDelta.heapUsed / 1024 / 1024).toFixed(2)}MB`);
      }
    } else {
      this.aggregatedMetrics.forEach((metrics, name) => {
        // console.log(`\nFunction: ${name}`);
        // console.log(`  Calls: ${metrics.count}`);
        // console.log(`  Avg Time: ${metrics.avgTime.toFixed(2)}ms`);
        // console.log(`  Min Time: ${metrics.minTime.toFixed(2)}ms`);
        // console.log(`  Max Time: ${metrics.maxTime.toFixed(2)}ms`);
        // console.log(`  Avg Memory Delta: ${(metrics.avgMemoryDelta.heapUsed / 1024 / 1024).toFixed(2)}MB`);
      });
    }
    
    const systemInfo = this.getSystemInfo();
    // console.log('\n========== SYSTEM INFO ==========');
    // console.log(`Platform: ${systemInfo.platform}`);
    // console.log(`Node Version: ${systemInfo.nodeVersion}`);
    // console.log(`Process Uptime: ${(systemInfo.processUptime / 60).toFixed(2)} minutes`);
    // console.log(`Total Memory: ${(systemInfo.totalMemory / 1024 / 1024 / 1024).toFixed(2)}GB`);
    // console.log(`Free Memory: ${(systemInfo.freeMemory / 1024 / 1024 / 1024).toFixed(2)}GB`);
    // console.log(`Load Average (1m, 5m, 15m): ${systemInfo.loadAvg.map(l => l.toFixed(2)).join(', ')}`);
    // console.log(`Process Memory: RSS: ${(systemInfo.processMemory.rss / 1024 / 1024).toFixed(2)}MB, Heap: ${(systemInfo.processMemory.heapUsed / 1024 / 1024).toFixed(2)}MB`);
    // console.log('======================================\n');
  }

  // Export metrics to CSV
  static exportToCSV(filename: string = 'performance-metrics.csv'): void {
    const fs = require('fs');
    const headers = 'functionName,executionTime,cpuUser,cpuSystem,memoryDelta,timestamp\n';
    const rows = this.metrics.map(m => 
      `${m.functionName},${m.executionTime.toFixed(2)},${m.cpuUsageDelta.user.toFixed(2)},${m.cpuUsageDelta.system.toFixed(2)},${m.memoryDelta.heapUsed},${m.timestamp.toISOString()}`
    ).join('\n');
    
    fs.writeFileSync(filename, headers + rows);
    // console.log(`Performance metrics exported to ${filename}`);
  }
}

// Create a decorator for easier use
export function measurePerformance(target: any, propertyKey: string, descriptor: PropertyDescriptor) {
  const originalMethod = descriptor.value;

  descriptor.value = async function (...args: any[]) {
    return PerformanceMonitor.measure(
      `${target.constructor.name}.${propertyKey}`,
      async () => originalMethod.apply(this, args)
    );
  };

  return descriptor;
}

// Export a simplified wrapper for manual use
export async function withPerformanceMonitoring<T>(
  name: string,
  fn: () => Promise<T>
): Promise<T> {
  return PerformanceMonitor.measure(name, fn);
}