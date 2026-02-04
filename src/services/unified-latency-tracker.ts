/**
 * Unified Latency Tracking Service
 *
 * Consolidates forwarding and execution latency tracking into a single system.
 * Samples evolve through two stages:
 *
 * Stage 1 (Forwarding): Message arrives from proxy → server
 * Stage 2 (Execution): Job completes (success/failure)
 *
 * Key Insight: execution_time = forwarding_latency + job_processing_time
 * Both measurements start from proxy's receivedAt timestamp.
 */

interface UnifiedLatencySample {
  sampleId: string;
  marketplace: 'opensea' | 'blur' | 'magiceden';

  // Stage 1: Forwarding (message arrival)
  proxyReceivedAt: number;
  serverReceivedAt: number;
  forwardingLatency: number;

  // Stage 2: Execution (job completion) - optional
  jobCompletedAt?: number;
  executionLatency?: number;
  success?: boolean;
  jobId?: string;

  createdAt: number;
}

interface LatencyStats {
  p50: number;
  p95: number;
  p99: number;
  avg: number;
  max: number;
  sampleCount: number;
}

interface ExecutionStats extends LatencyStats {
  successRate: number;
}

interface ExecutionStatsByMarketplace {
  overall: ExecutionStats;
  byMarketplace: {
    opensea: ExecutionStats;
    blur: ExecutionStats;
    magiceden: ExecutionStats;
  };
}

// Storage: Dual Map structure for O(1) lookup
const samplesByJobId = new Map<string, UnifiedLatencySample>();
const samplesById = new Map<string, UnifiedLatencySample>();

// Success/failure counters per marketplace
const counters = {
  opensea: { success: 0, failure: 0 },
  blur: { success: 0, failure: 0 },
  magiceden: { success: 0, failure: 0 }
};

// Configuration
const MAX_SAMPLES = 1000;
const SAMPLE_TTL_MS = 60000; // 60 seconds
const CLEANUP_INTERVAL_MS = 60000; // 60 seconds

let cleanupInterval: NodeJS.Timeout | null = null;
let sampleCounter = 0;

/**
 * Stage 1: Record forwarding latency when message arrives
 */
export function recordForwarding(
  marketplace: 'opensea' | 'blur' | 'magiceden',
  proxyReceivedAt: number,
  serverReceivedAt: number,
  jobId?: string
): void {
  const forwardingLatency = serverReceivedAt - proxyReceivedAt;
  const sampleId = `${marketplace}-${Date.now()}-${++sampleCounter}`;

  // Enforce max samples BEFORE insertion (FIFO eviction)
  // Remove oldest samples until we're under the limit
  while (samplesById.size >= MAX_SAMPLES) {
    const oldestId = samplesById.keys().next().value as string;
    if (oldestId) {
      const oldestSample = samplesById.get(oldestId);
      samplesById.delete(oldestId);
      if (oldestSample?.jobId) {
        samplesByJobId.delete(oldestSample.jobId);
      }
    } else {
      break; // Safety: prevent infinite loop
    }
  }

  const sample: UnifiedLatencySample = {
    sampleId,
    marketplace,
    proxyReceivedAt,
    serverReceivedAt,
    forwardingLatency,
    jobId,
    createdAt: Date.now()
  };

  // Store in both Maps
  samplesById.set(sampleId, sample);
  if (jobId) {
    samplesByJobId.set(jobId, sample);
  }
}

/**
 * Stage 2: Record execution completion when job finishes
 */
export function recordExecution(
  marketplace: 'opensea' | 'blur' | 'magiceden',
  jobId: string,
  success: boolean
): void {
  const jobCompletedAt = Date.now();

  // Look up existing sample by job ID
  const sample = samplesByJobId.get(jobId);

  if (sample) {
    // Evolve existing sample with execution data
    sample.jobCompletedAt = jobCompletedAt;
    sample.executionLatency = jobCompletedAt - sample.proxyReceivedAt;
    sample.success = success;

    // Update in both Maps (reference is same object)
    samplesById.set(sample.sampleId, sample);
    samplesByJobId.set(jobId, sample);
  } else {
    // Don't create execution-only sample - just log warning
    console.warn(`[UnifiedLatency] Execution record without forwarding data | jobId=${jobId} | marketplace=${marketplace}`);
    // This indicates a jobId mismatch that needs to be fixed at the call site
  }

  // Update counters (keep this - tracks all executions regardless of sample availability)
  if (success) {
    counters[marketplace].success++;
  } else {
    counters[marketplace].failure++;
  }
}

/**
 * Calculate percentile from sorted array
 */
function calculatePercentile(sortedValues: number[], percentile: number): number {
  if (sortedValues.length === 0) return 0;
  const index = Math.ceil((percentile / 100) * sortedValues.length) - 1;
  return sortedValues[Math.max(0, index)];
}

/**
 * Calculate statistics from array of numbers
 */
function calculateStats(values: number[]): LatencyStats {
  if (values.length === 0) {
    return { p50: 0, p95: 0, p99: 0, avg: 0, max: 0, sampleCount: 0 };
  }

  const sorted = [...values].sort((a, b) => a - b);
  const sum = sorted.reduce((acc, val) => acc + val, 0);

  return {
    p50: calculatePercentile(sorted, 50),
    p95: calculatePercentile(sorted, 95),
    p99: calculatePercentile(sorted, 99),
    avg: Math.round(sum / sorted.length),
    max: sorted[sorted.length - 1],
    sampleCount: sorted.length
  };
}

/**
 * Get forwarding latency statistics
 */
export function getForwardingStats(marketplace?: 'opensea' | 'blur' | 'magiceden'): LatencyStats {
  const samples = Array.from(samplesById.values());

  const filtered = marketplace
    ? samples.filter(s => s.marketplace === marketplace && s.forwardingLatency > 0)
    : samples.filter(s => s.forwardingLatency > 0);

  const latencies = filtered.map(s => s.forwardingLatency);

  return calculateStats(latencies);
}

/**
 * Get execution latency statistics with success rate
 */
export function getExecutionStats(marketplace?: 'opensea' | 'blur' | 'magiceden'): ExecutionStats {
  const samples = Array.from(samplesById.values());

  const filtered = marketplace
    ? samples.filter(s => s.marketplace === marketplace && s.executionLatency !== undefined && s.executionLatency > 0)
    : samples.filter(s => s.executionLatency !== undefined && s.executionLatency > 0);

  const latencies = filtered.map(s => s.executionLatency!);
  const successCount = filtered.filter(s => s.success === true).length;

  const stats = calculateStats(latencies);

  return {
    ...stats,
    successRate: filtered.length > 0 ? (successCount / filtered.length) * 100 : 0
  };
}

/**
 * Get execution statistics for all marketplaces
 */
export function getAllExecutionStats(): ExecutionStatsByMarketplace {
  return {
    overall: getExecutionStats(),
    byMarketplace: {
      opensea: getExecutionStats('opensea'),
      blur: getExecutionStats('blur'),
      magiceden: getExecutionStats('magiceden')
    }
  };
}

/**
 * Get success/failure counters
 */
export function getCounters() {
  return {
    opensea: { ...counters.opensea },
    blur: { ...counters.blur },
    magiceden: { ...counters.magiceden }
  };
}

/**
 * Periodic cleanup of expired samples
 */
function cleanup(): void {
  const now = Date.now();
  let expiredCount = 0;
  let orphanedCount = 0;

  for (const [sampleId, sample] of samplesById.entries()) {
    // Remove expired samples (TTL-based)
    if (now - sample.createdAt > SAMPLE_TTL_MS) {
      samplesById.delete(sampleId);
      if (sample.jobId) {
        samplesByJobId.delete(sample.jobId);
      }
      expiredCount++;
    }
    // Remove execution-only samples (created when lookup failed - should be rare after Part 3)
    else if (sample.executionLatency === 0 && sample.forwardingLatency === 0) {
      samplesById.delete(sampleId);
      if (sample.jobId) {
        samplesByJobId.delete(sample.jobId);
      }
      orphanedCount++;
    }
  }

  if (expiredCount > 0 || orphanedCount > 0) {
    console.log(`[UnifiedLatency] Cleanup | expired=${expiredCount} | orphaned=${orphanedCount} | remaining=${samplesById.size}`);
  }
}

/**
 * Start periodic cleanup
 */
export function startCleanup(): void {
  if (cleanupInterval) {
    console.warn('[UnifiedLatency] Cleanup already running');
    return;
  }

  cleanupInterval = setInterval(cleanup, CLEANUP_INTERVAL_MS);
  console.log(`[UnifiedLatency] Cleanup started | interval=${CLEANUP_INTERVAL_MS}ms | maxSamples=${MAX_SAMPLES} | ttl=${SAMPLE_TTL_MS}ms`);
}

/**
 * Stop periodic cleanup
 */
export function stopCleanup(): void {
  if (cleanupInterval) {
    clearInterval(cleanupInterval);
    cleanupInterval = null;
    console.log('[UnifiedLatency] Cleanup stopped');
  }
}

/**
 * Get current sample counts (debug)
 */
export function getSampleCounts() {
  const samples = Array.from(samplesById.values());
  const forwardingOnly = samples.filter(s => !s.executionLatency).length;
  const complete = samples.filter(s => s.executionLatency).length;

  return {
    total: samplesById.size,
    forwardingOnly,
    complete,
    byMarketplace: {
      opensea: samples.filter(s => s.marketplace === 'opensea').length,
      blur: samples.filter(s => s.marketplace === 'blur').length,
      magiceden: samples.filter(s => s.marketplace === 'magiceden').length
    }
  };
}

/**
 * Clear all samples (debug/testing)
 */
export function clearAllSamples(): void {
  samplesById.clear();
  samplesByJobId.clear();
  counters.opensea = { success: 0, failure: 0 };
  counters.blur = { success: 0, failure: 0 };
  counters.magiceden = { success: 0, failure: 0 };
  console.log('[UnifiedLatency] All samples cleared');
}
