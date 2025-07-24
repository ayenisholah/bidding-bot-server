import fs from 'fs';
import path from 'path';
import { format } from 'date-fns';

interface TimeoutLogEntry {
  timestamp: Date;
  jobId: string;
  jobName: string;
  taskId: string;
  contractSlug: string;
  duration: number;
  phase: string;
  details: any;
  apiCalls: ApiCallLog[];
  memorySnapshot: MemorySnapshot;
  queueStats: QueueStats;
  errorStack?: string;
}

interface ApiCallLog {
  timestamp: Date;
  endpoint: string;
  method: string;
  duration: number;
  status: number | 'timeout' | 'error';
  responseSize?: number;
  error?: string;
}

interface MemorySnapshot {
  heapUsed: number;
  heapTotal: number;
  external: number;
  rss: number;
}

interface QueueStats {
  waiting: number;
  active: number;
  completed: number;
  failed: number;
  delayed: number;
}

export class TimeoutLogger {
  private logDir: string;
  private currentLogFile: string;
  private jobLogs: Map<string, TimeoutLogEntry>;
  private apiCallBuffer: Map<string, ApiCallLog[]>;

  constructor() {
    this.logDir = path.join(process.cwd(), 'timeout-logs');
    this.ensureLogDirectory();
    this.currentLogFile = this.getLogFileName();
    this.jobLogs = new Map();
    this.apiCallBuffer = new Map();
  }

  private ensureLogDirectory() {
    if (!fs.existsSync(this.logDir)) {
      fs.mkdirSync(this.logDir, { recursive: true });
    }
  }

  private getLogFileName(): string {
    const date = format(new Date(), 'yyyy-MM-dd');
    return path.join(this.logDir, `timeout-analysis-${date}.log`);
  }

  startJobTracking(jobId: string, jobName: string, taskId: string, contractSlug: string) {
    const entry: TimeoutLogEntry = {
      timestamp: new Date(),
      jobId,
      jobName,
      taskId,
      contractSlug,
      duration: 0,
      phase: 'STARTED',
      details: {},
      apiCalls: [],
      memorySnapshot: this.captureMemorySnapshot(),
      queueStats: { waiting: 0, active: 0, completed: 0, failed: 0, delayed: 0 }
    };

    this.jobLogs.set(jobId, entry);
    this.apiCallBuffer.set(jobId, []);
    this.writeToLog(`[JOB START] ${jobName} - Job ID: ${jobId}, Task: ${taskId}, Contract: ${contractSlug}`);
  }

  updatePhase(jobId: string, phase: string, details?: any) {
    const entry = this.jobLogs.get(jobId);
    if (entry) {
      entry.phase = phase;
      entry.duration = Date.now() - entry.timestamp.getTime();
      if (details) {
        entry.details[phase] = details;
      }
      this.writeToLog(`[PHASE] Job ${jobId} - ${phase} (${entry.duration}ms) ${details ? JSON.stringify(details) : ''}`);
    }
  }

  logApiCall(jobId: string, endpoint: string, method: string, startTime: number, status: number | 'timeout' | 'error', responseSize?: number, error?: string) {
    const duration = Date.now() - startTime;
    const apiCall: ApiCallLog = {
      timestamp: new Date(),
      endpoint,
      method,
      duration,
      status,
      responseSize,
      error
    };

    const buffer = this.apiCallBuffer.get(jobId);
    if (buffer) {
      buffer.push(apiCall);
    }

    this.writeToLog(`[API] Job ${jobId} - ${method} ${endpoint} - Status: ${status}, Duration: ${duration}ms${error ? `, Error: ${error}` : ''}`);
  }

  logTimeout(jobId: string, timeoutDuration: number, queueStats: QueueStats, error: Error) {
    const entry = this.jobLogs.get(jobId);
    if (entry) {
      entry.duration = timeoutDuration;
      entry.phase = 'TIMEOUT';
      entry.queueStats = queueStats;
      entry.errorStack = error.stack;
      entry.apiCalls = this.apiCallBuffer.get(jobId) || [];
      entry.memorySnapshot = this.captureMemorySnapshot();

      // Write detailed timeout analysis
      this.writeTimeoutAnalysis(entry);
    }
  }

  private captureMemorySnapshot(): MemorySnapshot {
    const mem = process.memoryUsage();
    return {
      heapUsed: Math.round(mem.heapUsed / 1024 / 1024),
      heapTotal: Math.round(mem.heapTotal / 1024 / 1024),
      external: Math.round(mem.external / 1024 / 1024),
      rss: Math.round(mem.rss / 1024 / 1024)
    };
  }

  private writeTimeoutAnalysis(entry: TimeoutLogEntry) {
    const analysis = {
      summary: {
        jobId: entry.jobId,
        jobName: entry.jobName,
        taskId: entry.taskId,
        contractSlug: entry.contractSlug,
        totalDuration: entry.duration,
        timestamp: entry.timestamp,
        lastPhase: entry.phase
      },
      phases: entry.details,
      apiCalls: {
        total: entry.apiCalls.length,
        byEndpoint: this.groupApiCallsByEndpoint(entry.apiCalls),
        slowest: this.getSlowestApiCalls(entry.apiCalls, 5),
        failed: entry.apiCalls.filter(c => c.status === 'error' || c.status === 'timeout')
      },
      memory: {
        start: entry.memorySnapshot,
        end: this.captureMemorySnapshot(),
        delta: {
          heapUsed: this.captureMemorySnapshot().heapUsed - entry.memorySnapshot.heapUsed,
          heapTotal: this.captureMemorySnapshot().heapTotal - entry.memorySnapshot.heapTotal
        }
      },
      queue: entry.queueStats,
      possibleCauses: this.analyzePossibleCauses(entry)
    };

    const analysisFile = path.join(this.logDir, `timeout-${entry.jobId}-${format(new Date(), 'HHmmss')}.json`);
    fs.writeFileSync(analysisFile, JSON.stringify(analysis, null, 2));
    
    this.writeToLog(`[TIMEOUT ANALYSIS] Written to ${analysisFile}`);
    this.writeToLog(`[TIMEOUT SUMMARY] ${entry.jobName} - Duration: ${entry.duration}ms, API Calls: ${entry.apiCalls.length}, Last Phase: ${entry.phase}`);
  }

  private groupApiCallsByEndpoint(apiCalls: ApiCallLog[]): Record<string, { count: number; totalDuration: number; avgDuration: number }> {
    const groups: Record<string, { count: number; totalDuration: number }> = {};
    
    apiCalls.forEach(call => {
      const key = `${call.method} ${call.endpoint}`;
      if (!groups[key]) {
        groups[key] = { count: 0, totalDuration: 0 };
      }
      groups[key].count++;
      groups[key].totalDuration += call.duration;
    });

    const result: Record<string, any> = {};
    Object.entries(groups).forEach(([key, value]) => {
      result[key] = {
        ...value,
        avgDuration: Math.round(value.totalDuration / value.count)
      };
    });

    return result;
  }

  private getSlowestApiCalls(apiCalls: ApiCallLog[], count: number): ApiCallLog[] {
    return [...apiCalls]
      .sort((a, b) => b.duration - a.duration)
      .slice(0, count);
  }

  private analyzePossibleCauses(entry: TimeoutLogEntry): string[] {
    const causes = [];

    // Check for API bottlenecks
    const slowApiCalls = entry.apiCalls.filter(c => c.duration > 10000);
    if (slowApiCalls.length > 0) {
      causes.push(`${slowApiCalls.length} API calls took over 10 seconds`);
    }

    // Check for failed API calls
    const failedCalls = entry.apiCalls.filter(c => c.status === 'error' || c.status === 'timeout');
    if (failedCalls.length > 0) {
      causes.push(`${failedCalls.length} API calls failed or timed out`);
    }

    // Check queue congestion
    if (entry.queueStats.waiting > 1000) {
      causes.push(`High queue congestion: ${entry.queueStats.waiting} jobs waiting`);
    }

    // Check memory pressure
    const memDelta = this.captureMemorySnapshot().heapUsed - entry.memorySnapshot.heapUsed;
    if (memDelta > 500) {
      causes.push(`High memory allocation: ${memDelta}MB increase during job`);
    }

    // Check specific phases
    const phaseTimings = Object.entries(entry.details)
      .map(([phase, details]: [string, any]) => ({
        phase,
        duration: details.duration || 0
      }))
      .filter(p => p.duration > 30000);
    
    phaseTimings.forEach(p => {
      causes.push(`Phase '${p.phase}' took ${p.duration}ms`);
    });

    return causes;
  }

  private writeToLog(message: string) {
    const timestamp = format(new Date(), 'yyyy-MM-dd HH:mm:ss.SSS');
    const logLine = `[${timestamp}] ${message}\n`;
    
    fs.appendFileSync(this.currentLogFile, logLine);
    console.log(logLine.trim());
  }

  cleanup(jobId: string) {
    this.jobLogs.delete(jobId);
    this.apiCallBuffer.delete(jobId);
  }
}

// Singleton instance
export const timeoutLogger = new TimeoutLogger();