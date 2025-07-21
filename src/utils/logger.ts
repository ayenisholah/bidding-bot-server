// Simple logger replacement with console.log
export enum LogLevel {
  ERROR = 'error',
  WARN = 'warn',
  INFO = 'info',
  DEBUG = 'debug'
}

export enum ErrorCategory {
  WEBSOCKET = 'websocket',
  MARKETPLACE = 'marketplace',
  DATABASE = 'database',
  BLOCKCHAIN = 'blockchain',
  QUEUE = 'queue',
  API = 'api',
  GENERAL = 'general',
  AUTH = 'auth',
  VALIDATION = 'validation',
  NETWORK = 'network',
  PERFORMANCE = 'performance',
  SECURITY = 'security',
  BUSINESS_LOGIC = 'business_logic',
  OPENSEA_BID = 'opensea_bid'
}

export enum MarketplaceSubCategory {
  OPENSEA = 'opensea',
  BLUR = 'blur',
  MAGICEDEN = 'magiceden',
  BIDDING = 'bidding',
  LISTING = 'listing',
  COLLECTION = 'collection',
  TRAIT = 'trait',
  TOKEN = 'token',
  BALANCE = 'balance',
  PRICING = 'pricing',
  APPROVAL = 'approval',
  CANCELLATION = 'cancellation'
}

export enum DatabaseOperation {
  CREATE = 'create',
  READ = 'read',
  UPDATE = 'update',
  DELETE = 'delete',
  AGGREGATE = 'aggregate',
  INDEX = 'index',
  MIGRATION = 'migration',
  CONNECTION = 'connection'
}

export enum QueueOperation {
  ADD_JOB = 'add_job',
  PROCESS_JOB = 'process_job',
  COMPLETE_JOB = 'complete_job',
  FAIL_JOB = 'fail_job',
  RETRY_JOB = 'retry_job',
  CLEAN_JOBS = 'clean_jobs',
  PAUSE_QUEUE = 'pause_queue',
  RESUME_QUEUE = 'resume_queue'
}

interface LogContext {
  [key: string]: any;
}

interface ErrorMetrics {
  timestamp: number;
  level: LogLevel;
  category: ErrorCategory;
  message: string;
  context: LogContext;
}

export class AxiomLogger {
  private static instance: AxiomLogger;

  public static getInstance(): AxiomLogger {
    if (!AxiomLogger.instance) {
      AxiomLogger.instance = new AxiomLogger();
    }
    return AxiomLogger.instance;
  }

  async logWebSocketError(message: string, context?: any) {
    console.error('[WEBSOCKET ERROR]', message, context);
  }

  async logMarketplaceError(message: string, context?: any) {
    console.error('[MARKETPLACE ERROR]', message, context);
  }

  async logDatabaseError(message: string, context?: any) {
    console.error('[DATABASE ERROR]', message, context);
  }

  async logBlockchainError(message: string, context?: any) {
    console.error('[BLOCKCHAIN ERROR]', message, context);
  }

  async logQueueError(message: string, context?: any) {
    console.error('[QUEUE ERROR]', message, context);
  }

  async logApiError(message: string, context?: any) {
    console.error('[API ERROR]', message, context);
  }

  async logGeneralError(message: string, context?: any) {
    console.error('[GENERAL ERROR]', message, context);
  }

  async logWarning(category: ErrorCategory, message: string, context?: any) {
    console.warn(`[${category.toUpperCase()} WARNING]`, message, context);
  }

  async logInfo(category: ErrorCategory, message: string, context?: any) {
    console.log(`[${category.toUpperCase()} INFO]`, message, context);
  }

  async logDebug(category: ErrorCategory, message: string, context?: any) {
    console.log(`[${category.toUpperCase()} DEBUG]`, message, context);
  }

  // OpenSea specific logging
  async logOpenseaBidError(message: string, context?: any) {
    console.error('[OPENSEA BID ERROR]', message, context);
  }

  async logOpenseaListingError(message: string, context?: any) {
    console.error('[OPENSEA LISTING ERROR]', message, context);
  }

  async logOpenseaCollectionError(message: string, context?: any) {
    console.error('[OPENSEA COLLECTION ERROR]', message, context);
  }

  // Blur specific logging
  async logBlurBidError(message: string, context?: any) {
    console.error('[BLUR BID ERROR]', message, context);
  }

  async logBlurBalanceError(message: string, context?: any) {
    console.error('[BLUR BALANCE ERROR]', message, context);
  }

  // Magic Eden specific logging
  async logMagicEdenBidError(message: string, context?: any) {
    console.error('[MAGICEDEN BID ERROR]', message, context);
  }

  async logMagicEdenTokenError(message: string, context?: any) {
    console.error('[MAGICEDEN TOKEN ERROR]', message, context);
  }

  async logMagicEdenCollectionError(message: string, context?: any) {
    console.error('[MAGICEDEN COLLECTION ERROR]', message, context);
  }

  async logMagicEdenListingError(message: string, context?: any) {
    console.error('[MAGICEDEN LISTING ERROR]', message, context);
  }

  // Database methods
  async logDatabaseQuery(operation: DatabaseOperation, message: string, context?: any) {
    console.log(`[DATABASE ${operation.toUpperCase()}]`, message, context);
  }

  async logDatabaseSlowQuery(operation: DatabaseOperation, message: string, context?: any) {
    console.warn(`[DATABASE SLOW ${operation.toUpperCase()}]`, message, context);
  }

  async logDatabaseConnectionError(message: string, context?: any) {
    console.error('[DATABASE CONNECTION ERROR]', message, context);
  }

  async logDatabaseMigrationError(message: string, context?: any) {
    console.error('[DATABASE MIGRATION ERROR]', message, context);
  }

  // Queue methods
  async logQueueJobAdded(jobName: string, context?: any) {
    console.log('[QUEUE JOB ADDED]', jobName, context);
  }

  async logQueueJobFailed(jobName: string, context?: any) {
    console.error('[QUEUE JOB FAILED]', jobName, context);
  }

  async logQueueJobRetry(jobName: string, context?: any) {
    console.warn('[QUEUE JOB RETRY]', jobName, context);
  }

  // Performance methods
  async logPerformanceIssue(message: string, context?: any) {
    console.warn('[PERFORMANCE ISSUE]', message, context);
  }

  async logSlowOperation(operation: string, duration: number, context?: any) {
    console.warn('[SLOW OPERATION]', operation, `${duration}ms`, context);
  }

  async logMemoryWarning(message: string, memoryUsage: NodeJS.MemoryUsage, context?: any) {
    console.warn('[MEMORY WARNING]', message, memoryUsage, context);
  }

  // Validation methods
  async logValidationError(field: string, value: any, reason: string, context?: any) {
    console.error('[VALIDATION ERROR]', `${field}: ${reason}`, value, context);
  }

  async logBusinessLogicError(rule: string, message: string, context?: any) {
    console.error('[BUSINESS LOGIC ERROR]', rule, message, context);
  }

  // Security methods
  async logSecurityEvent(event: string, message: string, context?: any) {
    console.warn('[SECURITY EVENT]', event, message, context);
  }

  async logAuthenticationError(message: string, context?: any) {
    console.error('[AUTH ERROR]', message, context);
  }

  async logUnauthorizedAccess(resource: string, context?: any) {
    console.warn('[UNAUTHORIZED ACCESS]', resource, context);
  }

  // Network methods
  async logNetworkError(endpoint: string, message: string, context?: any) {
    console.error('[NETWORK ERROR]', endpoint, message, context);
  }

  async logRateLimitHit(service: string, limit: number, context?: any) {
    console.warn('[RATE LIMIT HIT]', service, `limit: ${limit}`, context);
  }

  async logApiTimeout(endpoint: string, timeout: number, context?: any) {
    console.error('[API TIMEOUT]', endpoint, `${timeout}ms`, context);
  }

  // Task methods
  async logTaskStarted(taskId: string, taskType: string, context?: any) {
    console.log('[TASK STARTED]', taskType, taskId, context);
  }

  async logTaskCompleted(taskId: string, taskType: string, duration: number, context?: any) {
    console.log('[TASK COMPLETED]', taskType, taskId, `${duration}ms`, context);
  }

  async logTaskFailed(taskId: string, taskType: string, reason: string, context?: any) {
    console.error('[TASK FAILED]', taskType, taskId, reason, context);
  }


  async logBidCancelled(marketplace: string, tokenId: string, reason: string, context?: any) {
    console.log('[BID CANCELLED]', marketplace, tokenId, reason, context);
  }

  // Error metrics stub
  getErrorMetrics() {
    return {
      recent: [],
      summary: { total: 0, last24Hours: 0, lastHour: 0, errorRate: 0 },
      byCategory: {},
      byLevel: {}
    };
  }

  // Generic error logging
  async logError(category: ErrorCategory, message: string, error: Error | LogContext = {}) {
    if (error instanceof Error) {
      console.error(`[${category.toUpperCase()} ERROR]`, message, error.message);
    } else {
      console.error(`[${category.toUpperCase()} ERROR]`, message, error);
    }
  }

  async flush() {
    // No-op for console logger
  }
}

// Export singleton instance
export const logger = AxiomLogger.getInstance();

// Export types
export type { LogContext, ErrorMetrics };