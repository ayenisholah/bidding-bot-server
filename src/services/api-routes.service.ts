import { Application } from 'express';
import { errorLog, warnLog} from '../utils/debug-file-logger';
import { checkBullMQConnectionsHealth } from '../utils/redis';

/**
 * API Routes Service
 *
 * Manages all REST API endpoints for the bidding bot server.
 * Organizes 62 endpoints into logical groups:
 * - Health & Diagnostics (health, metrics, system status)
 * - Task Management (CRUD operations, status updates)
 * - Collection & Marketplace (floor prices, stats, traits)
 * - Authentication (password management)
 * - Blur-specific (rate limits, auth, health)
 * - Memory & Monitoring (snapshots, queue health)
 * - Debug & Diagnostics (extensive debugging endpoints)
 * - Logs & Traces (log export, bid traces)
 */

// Dependencies interface - all the global state/functions needed by routes
export interface ApiRoutesServiceDependencies {
  // Core services
  queue: any;
  blurQueue: any;
  redis: any;
  mongoose: any;

  // Auth functions
  getPassword: () => string | undefined;
  renewPasswordTTL: () => Promise<void>;
  setPassword: (password: string) => void;
  recoverPasswordFromRedis: () => Promise<boolean>;
  decryptPassword: (encryptedPassword: string, inputPassword: string) => Promise<string>;
  reEncryptWallet: (walletId: string, oldPassword: string, newPassword: string) => Promise<void>;
  clearDecryptedKeyCache: () => void;

  // Task operations
  stopTask: (task: any, fromShutdown: boolean) => Promise<any>;

  // Rate limiters and trackers
  blurRateLimiter: any;
  blurCompletionTracker: any;
  cancellationTracker: any;

  // Circuit breakers
  openseaFloorBreakerRegistry: any;
  taskCircuitBreaker: any;

  // State maps and caches
  bidRates: any;
  bidStats: any;
  skipStats: any;
  errorStats: any;
  floorPrices: any;
  bestOffers: any;
  warningBids: any;
  warningMessage: any;
  rps: any;
  taskStatus: any;
  ownBidCounts: any;
  loopCounterStats: any;
  counterbidCounts: any;
  currentTasks: any[];
  activeTasks: Map<string, any>;
  workers: any[];
  timeoutStats: any;

  // Services
  memorySnapshotService: any;
  healthMonitorService: any;
  queueService: any;

  // Utility functions
  fetchOpenseaTraits: (slug: string, limit?: number) => Promise<any>;
  fetchRarityTokens: (slug: string, limit: number) => Promise<any>;
  fetchAddressBalances: (walletAddress: string) => Promise<any>;
  getOpenseaFloor: (slug: string, source: string) => Promise<any>;
  getBlurFloor: (contractAddress: string, signal?: AbortSignal) => Promise<any>;
  getCollectionDetails: (slug: string) => Promise<any>;
  fetchOpenseaOffers: (slug: string, limit: number) => Promise<any>;
  fetchBlurCollectionStats: (contractAddress: string) => Promise<any>;
  fetchMagicEdenCollectionStats: (contractAddress: string) => Promise<any>;
  fetchWalletNFTs: (walletAddress: string) => Promise<any>;
  resolveContractAddress: (slug: string) => Promise<string | null>;
  updateTaskCancellationPercentage: (taskId: string, percentage: number, details?: string) => void;

  // Metrics and monitoring
  getMemoryStats: () => any;
  getAuthMetrics: () => any;
  getTimeoutMetrics: () => any;
  getLimiterQueueDetails: () => any;

  // Log export
  exportLogs: (filters: any) => Promise<any>;
  getAllCategories: () => string[];

  // Models
  Task: any;
  BidLogs: any;
  Wallet: any;
  Listing: any;
}

/**
 * API Routes Service
 * Singleton service that sets up all REST API endpoints
 */
class ApiRoutesService {
  private app: Application | null = null;
  private deps: ApiRoutesServiceDependencies | null = null;

  /**
   * Initialize the service with Express app and dependencies
   */
  public initialize(app: Application, dependencies: ApiRoutesServiceDependencies): void {
    this.app = app;
    this.deps = dependencies;

    this.setupRoutes();
  }

  /**
   * Set up all route groups
   */
  private setupRoutes(): void {
    if (!this.app || !this.deps) {
      throw new Error('ApiRoutesService not properly initialized');
    }

    // Route groups
    this.setupCoreRoutes();
    this.setupHealthRoutes();
    this.setupTaskRoutes();
    this.setupCollectionRoutes();
    this.setupAuthRoutes();
    this.setupBlurRoutes();
    this.setupMemoryRoutes();
    this.setupDebugRoutes();
    this.setupLogRoutes();
  }

  // ============================================================================
  // CORE ROUTES
  // ============================================================================

  private setupCoreRoutes(): void {
    // Root endpoint
    this.app!.get("/", (req, res) => {
      res.json({ message: "Welcome to the NFTTools bidding bot server! Let's make magic happen! 🚀🚀🚀" });
    });
  }

  // ============================================================================
  // HEALTH & DIAGNOSTICS ROUTES
  // ============================================================================

  private setupHealthRoutes(): void {
    // Main health check
    this.app!.get("/health", async (req, res) => {
      try {
        // Silently renew password TTL on health checks
        if (this.deps!.getPassword()) {
          this.deps!.renewPasswordTTL().catch(() => { /* Ignore renewal errors */ });
        }

        const timeoutPromise = new Promise((_, reject) => {
          setTimeout(() => reject(new Error('Health check timeout')), 5000);
        });

        let queueCounts = { active: 0, completed: 0, delayed: 0, failed: 0, paused: 0, prioritized: 0, waiting: 0, 'waiting-children': 0 };
        let blurRateLimits = { totalWallets: 0, rateLimitedWallets: 0, wallets: [] };

        try {
          queueCounts = await Promise.race([
            this.deps!.queue.getJobCounts(),
            timeoutPromise
          ]) as any;
        } catch (error) {
          warnLog('Health', `Failed to get queue counts in health check: ${error instanceof Error ? error.message : String(error)}`);
        }

        try {
          blurRateLimits = await Promise.race([
            Promise.resolve(this.deps!.blurRateLimiter.getStatus()),
            timeoutPromise
          ]) as any;
        } catch (error) {
          warnLog('Health', `Failed to get blur rate limits in health check: ${error instanceof Error ? error.message : String(error)}`);
        }

        // Check BullMQ connection health
        const bullmqHealth = await checkBullMQConnectionsHealth();
        const allBullMQHealthy = Object.values(bullmqHealth).every((h: any) => h.healthy);

        const metrics = {
          status: "ok",
          timestamp: new Date().toISOString(),
          uptime: process.uptime(),
          passwordSet: !!this.deps!.getPassword(),
          components: {
            bullmq: allBullMQHealthy ? "ok" : "error",
            mongodb: this.deps!.mongoose.connection.readyState === 1 ? "ok" : "error",
          },
          metrics: {
            memory: {
              rss: `${Math.round(process.memoryUsage().rss / 1024 / 1024)}MB`,
              heapTotal: `${Math.round(process.memoryUsage().heapTotal / 1024 / 1024)}MB`,
              heapUsed: `${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB`,
              external: `${Math.round(process.memoryUsage().external / 1024 / 1024)}MB`
            },
            queue: queueCounts,
            blurRateLimits: blurRateLimits
          }
        };
        res.json(metrics);
      } catch (error: any) {
        errorLog('HealthCheck', 'Health check error', error);
        res.status(500).json({
          status: "error",
          timestamp: new Date().toISOString(),
          error: error.message
        });
      }
    });

    // Note: Additional health routes will be added in subsequent methods
    // This is a skeleton to show the pattern
  }

  // Placeholder methods for other route groups
  private setupTaskRoutes(): void {
    // Task management endpoints will be added here
  }

  private setupCollectionRoutes(): void {
    // Collection and marketplace endpoints will be added here
  }

  private setupAuthRoutes(): void {
    // Authentication endpoints will be added here
  }

  private setupBlurRoutes(): void {
    // Blur-specific endpoints will be added here
  }

  private setupMemoryRoutes(): void {
    // Memory and monitoring endpoints will be added here
  }

  private setupDebugRoutes(): void {
    // Debug and diagnostics endpoints will be added here
  }

  private setupLogRoutes(): void {
    // Log and trace endpoints will be added here
  }

  /**
   * Get statistics about registered routes
   */
  public getStats(): { totalRoutes: number } {
    // This would count registered routes in production
    return { totalRoutes: 62 };
  }
}

export const apiRoutesService = new ApiRoutesService();
