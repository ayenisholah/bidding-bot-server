import axios from 'axios';
import { config } from 'dotenv';

config();

const USERNAME = process.env.USERNAME || 'UNKNOWN';
const AXIOM_TOKEN = process.env.AXIOM_TOKEN;
const AXIOM_DATASET = process.env.AXIOM_DATASET || 'nft-bidding-bot';
const AXIOM_URL = 'https://api.axiom.co/v1/datasets';
const DISABLE_AXIOM = process.env.NODE_ENV === 'production'; // Disable in production, enable in development

// Batch logging for performance
const logBatch: any[] = [];
const BATCH_SIZE = 10;
const BATCH_TIMEOUT = 5000; // 5 seconds

// Initialize Axiom
if (AXIOM_TOKEN && !DISABLE_AXIOM) {
  // console.log('✅ Axiom logging initialized');

  // Flush batch periodically
  setInterval(flushLogs, BATCH_TIMEOUT);

  // Flush on process exit
  process.on('beforeExit', flushLogs);
  process.on('SIGINT', () => {
    flushLogs().finally(() => process.exit(0));
  });
  process.on('SIGTERM', () => {
    flushLogs().finally(() => process.exit(0));
  });
} else if (DISABLE_AXIOM) {
  // console.log('⚠️ Axiom logging disabled in production environment');
} else {
  console.warn('⚠️ AXIOM_TOKEN not found. Axiom logging disabled.');
}

// Flush logs to Axiom
async function flushLogs() {
  if (!AXIOM_TOKEN || DISABLE_AXIOM || logBatch.length === 0) return;

  try {
    const logsToSend = [...logBatch];
    logBatch.length = 0; // Clear batch

    // Validate log data structure
    const validatedLogs = logsToSend.map(log => {
      // Ensure _time is properly formatted
      if (!log._time || typeof log._time !== 'string') {
        log._time = new Date().toISOString();
      }
      // Ensure required fields are present
      if (!log.message || !log.level) {
        console.warn('Invalid log entry missing required fields:', log);
      }
      return log;
    }).filter(log => log.message && log.level); // Only send valid logs

    if (validatedLogs.length === 0) {
      return; // Nothing to send
    }

    await axios.post(
      `${AXIOM_URL}/${AXIOM_DATASET}/ingest`,
      validatedLogs,
      {
        headers: {
          'Authorization': `Bearer ${AXIOM_TOKEN}`,
          'Content-Type': 'application/json'
        },
        timeout: 30000 // Increased from 10s to 30s to prevent timeout errors
      }
    );
  } catch (error) {
    if (axios.isAxiosError(error)) {
      console.error('Failed to send logs to Axiom:', {
        message: error.message,
        status: error.response?.status,
        data: error.response?.data,
        headers: error.response?.headers
      });
    } else {
      console.error('Failed to send logs to Axiom:', error instanceof Error ? error.message : String(error));
    }
    // Don't re-add failed logs to avoid infinite loops
  }
}

// Error severity levels
export enum LogLevel {
  ERROR = 'error',
  WARN = 'warn',
  INFO = 'info',
  DEBUG = 'debug'
}

// Error categories for better organization
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
  OPENSEA_BID = 'opensea_bid',
  TASK = 'task',
  TASK_MANAGEMENT = 'task_management',
  CACHE = 'cache',
  STATE_MANAGEMENT = 'state_management'
}

// Marketplace subcategories for granular tracking
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

// Database operation types
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

// Queue operation types
export enum QueueOperation {
  ADD_JOB = 'add_job',
  PROCESS_JOB = 'process_job',
  COMPLETE_JOB = 'complete_job',
  FAIL_JOB = 'fail_job',
  RETRY_JOB = 'retry_job',
  CLEAN_JOBS = 'clean_jobs',
  PAUSE_QUEUE = 'pause_queue',
  RESUME_QUEUE = 'resume_queue',
  BULK_ADD = 'bulk_add'
}

interface LogContext {
  // Task context
  taskId?: string;
  taskName?: string;
  taskType?: string;

  // Marketplace context
  marketplace?: string;
  marketplaceSubCategory?: MarketplaceSubCategory;
  operation?: string;

  // NFT context
  walletAddress?: string;
  contractAddress?: string;
  tokenId?: string;
  collection?: string;
  collectionSlug?: string;
  traitType?: string;
  traitValue?: string;

  // Financial context
  bidAmount?: string | number;
  floorPrice?: string | number;
  currency?: string;
  gasPrice?: string | number;
  gasFee?: string | number;

  // User context
  userId?: string;
  userAddress?: string;

  // Request context
  requestId?: string;
  endpoint?: string;
  method?: string;
  statusCode?: number;
  responseTime?: number;

  // Connection context
  connectionId?: string;
  retryCount?: number;
  reconnectAttempts?: number;

  // Database context
  databaseOperation?: DatabaseOperation;
  collection_name?: string;
  query?: any;
  affectedRows?: number;
  executionTime?: number;

  // Queue context
  queueOperation?: QueueOperation;
  jobId?: string;
  jobName?: string;
  jobAttempts?: number;
  jobDelay?: number;
  queueName?: string;

  // Error context
  error?: Error;
  errorCode?: string;
  errorMessage?: string;
  stackTrace?: string;

  // Performance context
  startTime?: number;
  endTime?: number;
  duration?: number;
  memoryUsage?: NodeJS.MemoryUsage;
  cpuUsage?: NodeJS.CpuUsage;

  // Security context
  ipAddress?: string;
  userAgent?: string;
  authMethod?: string;

  // Additional metadata
  environment?: string;
  version?: string;
  buildId?: string;
  correlationId?: string;

  [key: string]: any;
}

interface ErrorMetrics {
  timestamp: number;
  level: LogLevel;
  category: ErrorCategory;
  message: string;
  context: LogContext;
}

// In-memory error metrics for dashboard
const errorMetrics: ErrorMetrics[] = [];
const MAX_ERROR_HISTORY = 1000;

export class AxiomLogger {
  private static instance: AxiomLogger;

  public static getInstance(): AxiomLogger {
    if (!AxiomLogger.instance) {
      AxiomLogger.instance = new AxiomLogger();
    }
    return AxiomLogger.instance;
  }

  private formatMessage(category: ErrorCategory, message: string, context?: LogContext): string {
    const parts = [USERNAME, 'SERVER', category.toUpperCase()];

    // Add marketplace specificity
    if (context?.marketplace) {
      parts.push(context.marketplace.toUpperCase());
    }

    // Add subcategory if available
    if (context?.marketplaceSubCategory) {
      parts.push(context.marketplaceSubCategory.toUpperCase());
    }

    // Add operation specificity
    if (context?.databaseOperation) {
      parts.push(context.databaseOperation.toUpperCase());
    }

    if (context?.queueOperation) {
      parts.push(context.queueOperation.toUpperCase());
    }

    // Add error detail
    const errorDetail = context?.error?.name ||
      context?.errorCode ||
      message.split(' ')[0].replace(/[^A-Z0-9_]/gi, '').toUpperCase() ||
      'ERROR';

    parts.push(errorDetail);

    return parts.join('_');
  }

  private addToMetrics(level: LogLevel, category: ErrorCategory, message: string, context: LogContext = {}) {
    const metric: ErrorMetrics = {
      timestamp: Date.now(),
      level,
      category,
      message,
      context
    };

    errorMetrics.push(metric);

    // Keep only last MAX_ERROR_HISTORY errors
    if (errorMetrics.length > MAX_ERROR_HISTORY) {
      errorMetrics.shift();
    }
  }

  private async sendLog(level: LogLevel, category: ErrorCategory, message: string, context: LogContext = {}) {
    // Format error message if it's an object
    if (context.errorMessage && typeof context.errorMessage === 'object' && 'message' in context.errorMessage) {
      context.errorMessage = (context.errorMessage as any).message;
    }
    
    const formattedMessage = this.formatMessage(category, message, context);

    // Add to local metrics
    this.addToMetrics(level, category, formattedMessage, context);

    // Categories to exclude from Axiom
    const excludedCategories = [ErrorCategory.QUEUE, ErrorCategory.DATABASE, ErrorCategory.SECURITY, ErrorCategory.AUTH];

    // Only send ERRORS to Axiom (not info, debug, or warn)
    if (AXIOM_TOKEN && !DISABLE_AXIOM && level === LogLevel.ERROR && !excludedCategories.includes(category)) {
      try {
        // Create a minimal log data object with only the most essential fields
        const logData: any = {
          _time: new Date().toISOString(),
          level: level.toUpperCase(),
          message: formattedMessage,
          category,
          username: USERNAME,
          server_type: 'nft_bidding_bot',
          environment: process.env.NODE_ENV || 'development'
        };

        // Add only the most critical context fields
        if (context.taskId) logData.taskId = context.taskId;
        if (context.marketplace) logData.marketplace = context.marketplace;
        if (context.operation) logData.operation = context.operation;
        if (context.collection) logData.collection = context.collection;
        
        // Add error information (flattened, no error name to save columns)
        if (context.errorMessage) {
          logData.errorMessage = context.errorMessage;
        } else if (context.error && context.error instanceof Error) {
          logData.errorMessage = context.error.message;
        }
        if (context.errorCode) logData.errorCode = context.errorCode;
        
        // Add marketplace subcategory if present
        if (context.marketplaceSubCategory) logData.marketplaceSubCategory = context.marketplaceSubCategory;

        // Remove undefined values
        Object.keys(logData).forEach(key => {
          if ((logData as any)[key] === undefined) {
            delete (logData as any)[key];
          }
        });

        // Add to batch
        logBatch.push(logData);

        // Flush if batch is full
        if (logBatch.length >= BATCH_SIZE) {
          setImmediate(flushLogs);
        }
      } catch (error) {
        console.error('Failed to prepare log for Axiom:', error instanceof Error ? error.message : String(error));
      }
    }

    // Skip console output for noisy logs
    const skipConsoleCategories = [ErrorCategory.MARKETPLACE, ErrorCategory.QUEUE, ErrorCategory.GENERAL];
    const skipConsoleLevels = [LogLevel.INFO, LogLevel.DEBUG];
    
    if (skipConsoleCategories.includes(category) && skipConsoleLevels.includes(level)) {
      // Skip console output for these categories at INFO/DEBUG level
      return;
    }
    
    // Format console output for better readability
    const consoleMessage = `[${level.toUpperCase()}] ${formattedMessage}`;
    
    // Create a cleaner context for console output
    const cleanContext = { ...context };
    
    // Extract error message from nested object
    if (cleanContext.errorMessage && typeof cleanContext.errorMessage === 'object' && 'message' in cleanContext.errorMessage) {
      cleanContext.errorMessage = (cleanContext.errorMessage as any).message;
    }
    
    // Clean up error stack traces for console
    if (cleanContext.error && cleanContext.error instanceof Error) {
      cleanContext.error = {
        name: cleanContext.error.name,
        message: cleanContext.error.message
      };
    }
    
    switch (level) {
      case LogLevel.ERROR:
        console.error(consoleMessage);
        if (cleanContext.errorMessage) {
          console.error(`  Error: ${cleanContext.errorMessage}`);
        }
        break;
      case LogLevel.WARN:
        console.warn(consoleMessage);
        break;
      case LogLevel.INFO:
        console.info(consoleMessage);
        break;
      case LogLevel.DEBUG:
        console.debug(consoleMessage);
        break;
    }
  }

  // Specific logging methods for different categories
  async logWebSocketError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.WEBSOCKET, message, context);
  }

  async logMarketplaceError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.MARKETPLACE, message, context);
  }

  async logDatabaseError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.DATABASE, message, context);
  }

  async logBlockchainError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.BLOCKCHAIN, message, context);
  }

  async logQueueError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.QUEUE, message, context);
  }

  async logApiError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.API, message, context);
  }

  async logGeneralError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.GENERAL, message, context);
  }

  // Warning methods
  async logWarning(category: ErrorCategory, message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.WARN, category, message, context);
  }

  // Info methods
  async logInfo(category: ErrorCategory, message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.INFO, category, message, context);
  }

  // Debug methods
  async logDebug(category: ErrorCategory, message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.DEBUG, category, message, context);
  }

  // === GRANULAR MARKETPLACE LOGGING METHODS ===

  // OpenSea specific logging
  async logOpenseaBidError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.OPENSEA_BID, message, {
      ...context,
      marketplace: 'opensea',
      marketplaceSubCategory: MarketplaceSubCategory.BIDDING
    });
  }

  async logOpenseaListingError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.MARKETPLACE, message, {
      ...context,
      marketplace: 'opensea',
      marketplaceSubCategory: MarketplaceSubCategory.LISTING
    });
  }

  async logOpenseaCollectionError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.MARKETPLACE, message, {
      ...context,
      marketplace: 'opensea',
      marketplaceSubCategory: MarketplaceSubCategory.COLLECTION
    });
  }

  // Blur specific logging
  async logBlurBidError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.MARKETPLACE, message, {
      ...context,
      marketplace: 'blur',
      marketplaceSubCategory: MarketplaceSubCategory.BIDDING
    });
  }

  async logBlurBalanceError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.MARKETPLACE, message, {
      ...context,
      marketplace: 'blur',
      marketplaceSubCategory: MarketplaceSubCategory.BALANCE
    });
  }

  // Magic Eden specific logging
  async logMagicEdenBidError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.MARKETPLACE, message, {
      ...context,
      marketplace: 'magiceden',
      marketplaceSubCategory: MarketplaceSubCategory.BIDDING
    });
  }

  async logMagicEdenTokenError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.MARKETPLACE, message, {
      ...context,
      marketplace: 'magiceden',
      marketplaceSubCategory: MarketplaceSubCategory.TOKEN
    });
  }

  async logMagicEdenCollectionError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.MARKETPLACE, message, {
      ...context,
      marketplace: 'magiceden',
      marketplaceSubCategory: MarketplaceSubCategory.COLLECTION
    });
  }

  async logMagicEdenListingError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.MARKETPLACE, message, {
      ...context,
      marketplace: 'magiceden',
      marketplaceSubCategory: MarketplaceSubCategory.LISTING
    });
  }

  // === GRANULAR DATABASE LOGGING METHODS ===

  async logDatabaseQuery(operation: DatabaseOperation, message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.INFO, ErrorCategory.DATABASE, message, {
      ...context,
      databaseOperation: operation
    });
  }

  async logDatabaseSlowQuery(operation: DatabaseOperation, message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.WARN, ErrorCategory.DATABASE, message, {
      ...context,
      databaseOperation: operation
    });
  }

  async logDatabaseConnectionError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.DATABASE, message, {
      ...context,
      databaseOperation: DatabaseOperation.CONNECTION
    });
  }

  async logDatabaseMigrationError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.DATABASE, message, {
      ...context,
      databaseOperation: DatabaseOperation.MIGRATION
    });
  }

  // === GRANULAR QUEUE LOGGING METHODS ===

  async logQueueJobAdded(jobName: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.INFO, ErrorCategory.QUEUE, `Job added: ${jobName}`, {
      ...context,
      queueOperation: QueueOperation.ADD_JOB,
      jobName
    });
  }
  async logQueueJobFailed(jobName: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.QUEUE, `Job failed: ${jobName}`, {
      ...context,
      queueOperation: QueueOperation.FAIL_JOB,
      jobName
    });
  }

  async logQueueJobRetry(jobName: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.WARN, ErrorCategory.QUEUE, `Job retry: ${jobName}`, {
      ...context,
      queueOperation: QueueOperation.RETRY_JOB,
      jobName
    });
  }

  async logQueueOperation(operation: QueueOperation, context: LogContext = {}) {
    await this.sendLog(LogLevel.INFO, ErrorCategory.QUEUE, `Queue operation: ${operation}`, {
      ...context,
      queueOperation: operation
    });
  }

  // === GRANULAR PERFORMANCE LOGGING METHODS ===

  async logPerformanceIssue(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.WARN, ErrorCategory.PERFORMANCE, message, context);
  }

  async logSlowOperation(operation: string, duration: number, context: LogContext = {}) {
    await this.sendLog(LogLevel.WARN, ErrorCategory.PERFORMANCE, `Slow operation: ${operation}`, {
      ...context,
      operation,
      duration,
      endTime: Date.now(),
      startTime: Date.now() - duration
    });
  }

  async logMemoryWarning(message: string, memoryUsage: NodeJS.MemoryUsage, context: LogContext = {}) {
    await this.sendLog(LogLevel.WARN, ErrorCategory.PERFORMANCE, message, {
      ...context,
      memoryUsage
    });
  }

  // === GRANULAR VALIDATION LOGGING METHODS ===

  async logValidationError(field: string, value: any, reason: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.VALIDATION, `Validation failed for ${field}: ${reason}`, {
      ...context,
      field,
      value: typeof value === 'object' ? JSON.stringify(value) : value,
      reason
    });
  }

  async logBusinessLogicError(rule: string, message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.BUSINESS_LOGIC, message, {
      ...context,
      rule
    });
  }

  // === GRANULAR SECURITY LOGGING METHODS ===

  async logSecurityEvent(event: string, message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.WARN, ErrorCategory.SECURITY, message, {
      ...context,
      securityEvent: event
    });
  }

  async logAuthenticationError(message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.AUTH, message, context);
  }

  async logUnauthorizedAccess(resource: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.WARN, ErrorCategory.SECURITY, `Unauthorized access attempt to: ${resource}`, {
      ...context,
      resource
    });
  }

  // === GRANULAR NETWORK LOGGING METHODS ===

  async logNetworkError(endpoint: string, message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.NETWORK, message, {
      ...context,
      endpoint
    });
  }

  async logRateLimitHit(service: string, limit: number, context: LogContext = {}) {
    await this.sendLog(LogLevel.WARN, ErrorCategory.NETWORK, `Rate limit hit for ${service}`, {
      ...context,
      service,
      rateLimit: limit
    });
  }

  async logApiTimeout(endpoint: string, timeout: number, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.NETWORK, `API timeout for ${endpoint}`, {
      ...context,
      endpoint,
      timeout
    });
  }

  // === GRANULAR TASK LOGGING METHODS ===

  async logTaskStarted(taskId: string, taskType: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.INFO, ErrorCategory.GENERAL, `Task started: ${taskType}`, {
      ...context,
      taskId,
      taskType
    });
  }

  async logTaskCompleted(taskId: string, taskType: string, duration: number, context: LogContext = {}) {
    await this.sendLog(LogLevel.INFO, ErrorCategory.GENERAL, `Task completed: ${taskType}`, {
      ...context,
      taskId,
      taskType,
      duration
    });
  }

  async logTaskFailed(taskId: string, taskType: string, reason: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.GENERAL, `Task failed: ${taskType} - ${reason}`, {
      ...context,
      taskId,
      taskType,
      failureReason: reason
    });
  }

  async logBidPlaced(_marketplace: string, _amount: number, _tokenId: string, _context: LogContext = {}) {
    // Skip logging bid placements to reduce console noise
    return;
  }

  async logBidCancelled(marketplace: string, tokenId: string, reason: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.INFO, ErrorCategory.MARKETPLACE, `Bid cancelled on ${marketplace}: ${reason}`, {
      ...context,
      marketplace,
      tokenId,
      cancellationReason: reason,
      marketplaceSubCategory: MarketplaceSubCategory.CANCELLATION
    });
  }

  // === CACHE OPERATION LOGGING ===
  
  async logCacheError(operation: 'read' | 'write' | 'delete', cacheType: string, message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.DATABASE, message, {
      ...context,
      cacheOperation: operation,
      cacheType,
      databaseOperation: operation === 'read' ? DatabaseOperation.READ : operation === 'write' ? DatabaseOperation.CREATE : DatabaseOperation.DELETE
    });
  }

  async logCancellationError(marketplace: string, message: string, context: LogContext = {}) {
    await this.sendLog(LogLevel.ERROR, ErrorCategory.MARKETPLACE, message, {
      ...context,
      marketplace,
      marketplaceSubCategory: MarketplaceSubCategory.CANCELLATION
    });
  }

  // Get error metrics for dashboard
  getErrorMetrics() {
    return {
      recent: errorMetrics.slice(-50), // Last 50 errors
      summary: this.getErrorSummary(),
      byCategory: this.getErrorsByCategory(),
      byLevel: this.getErrorsByLevel()
    };
  }

  private getErrorSummary() {
    const last24Hours = Date.now() - (24 * 60 * 60 * 1000);
    const recentErrors = errorMetrics.filter(error => error.timestamp > last24Hours);

    return {
      total: errorMetrics.length,
      last24Hours: recentErrors.length,
      lastHour: recentErrors.filter(error => error.timestamp > Date.now() - (60 * 60 * 1000)).length,
      errorRate: recentErrors.length / 24 // errors per hour
    };
  }

  private getErrorsByCategory() {
    const categories: Record<string, number> = {};
    errorMetrics.forEach(error => {
      categories[error.category] = (categories[error.category] || 0) + 1;
    });
    return categories;
  }

  private getErrorsByLevel() {
    const levels: Record<string, number> = {};
    errorMetrics.forEach(error => {
      levels[error.level] = (levels[error.level] || 0) + 1;
    });
    return levels;
  }

  // Generic error logging with formatting
  async logError(category: ErrorCategory, message: string, context: LogContext = {}) {
    // Format OpenSea errors for better readability
    if (category === ErrorCategory.OPENSEA_BID && context.error) {
      // If error is an object with response.data.errors, extract the message
      if (typeof context.error === 'object' && 
          context.error !== null &&
          'response' in context.error) {
        const errorObj = context.error as any;
        if (errorObj.response?.data?.errors?.[0]?.message) {
          context.errorMessage = errorObj.response.data.errors[0].message;
          // Create a new Error object with the message instead of assigning a string
          context.error = new Error(context.errorMessage);
        }
      }
    }
    await this.sendLog(LogLevel.ERROR, category, message, context);
  }

  // Flush logs to Axiom
  async flush() {
    await flushLogs();
  }
}

// Export singleton instance
export const logger = AxiomLogger.getInstance();

// Export all types for use in other files
export type { LogContext, ErrorMetrics };
// Note: Enums are already exported above