import axios from 'axios';
import { warnLog, errorLog } from '../utils/debug-file-logger';

/**
 * Connectivity Monitor Service
 *
 * Monitors internet connectivity and handles connection loss/restoration.
 *
 * Responsibilities:
 * - Periodic connectivity checks using multiple redundant endpoints
 * - Automatic retry with exponential backoff on failures
 * - Connectivity restoration handling (queue restart, WebSocket reconnect)
 * - Downtime tracking and logging
 *
 * Features:
 * - Multi-endpoint redundancy (Google, Cloudflare, Cloudflare DNS)
 * - Automatic recovery actions on connectivity restore
 * - Configurable check intervals
 * - Graceful degradation during network issues
 */

// Constants
const STANDARD_INTERVAL = 60000; // 60 seconds
const CONNECTIVITY_ENDPOINTS = [
  { name: 'Google', url: 'https://www.google.com', expectedStatus: 200 },
  { name: 'Cloudflare', url: 'https://1.1.1.1', expectedStatus: 200 },
  { name: 'Cloudflare DNS', url: 'https://cloudflare-dns.com', expectedStatus: 200 }
];
const MAX_RETRIES = 3;
const RETRY_DELAY = 2000; // 2 seconds between retries

/**
 * Dependencies required by ConnectivityMonitorService
 */
interface ConnectivityMonitorDependencies {
  clearQueueAndRestartJobs: () => Promise<void>;
  connectWebSocket: () => Promise<void>;
  initializeListingScheduler: (wsManager: any) => Promise<void>;
  getWsManager: () => any;
}

class ConnectivityMonitorService {
  private deps: ConnectivityMonitorDependencies | null = null;

  // State
  private isConnected = true;
  private connectivityLostAt: number | null = null;
  private connectivityCheckInterval: NodeJS.Timeout | null = null;

  /**
   * Initialize the service with dependencies
   */
  public initialize(deps: ConnectivityMonitorDependencies): void {
    this.deps = deps;
  }

  /**
   * Check internet connectivity by testing multiple endpoints
   * Returns true if ANY endpoint responds successfully
   */
  public async checkInternetConnectivity(): Promise<boolean> {
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {

      // Try each endpoint - success if ANY endpoint responds
      for (const endpoint of CONNECTIVITY_ENDPOINTS) {
        try {
          const response = await axios.get(endpoint.url, { timeout: 5000 });

          if (response.status === endpoint.expectedStatus || response.status === 204) {
            return true; // Internet is available
          }
        } catch (error: any) {
          // Endpoint failed, try next one
          const errorMsg = error?.message || String(error);
        }
      }

      // All endpoints failed for this attempt
      if (attempt < MAX_RETRIES) {
        warnLog('Health', `[ConnectivityCheck] All endpoints failed, retrying in ${RETRY_DELAY}ms (${attempt}/${MAX_RETRIES})`);
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
      }
    }

    // All retries exhausted and all endpoints failed
    errorLog('ConnectivityMonitor', 'Internet connectivity lost after retry attempts');
    return false;
  }

  /**
   * Handle connectivity restoration
   * - Restarts queues if downtime > 1 minute
   * - Reconnects WebSocket
   * - Reinitializes listing scheduler
   */
  private async handleConnectivityRestored(): Promise<void> {
    if (!this.deps) {
      errorLog('ConnectivityMonitor', 'Cannot handle connectivity restore - service not initialized');
      return;
    }

    // Calculate how long connectivity was lost
    const downtime = this.connectivityLostAt ? Date.now() - this.connectivityLostAt : 0;
    const downtimeMinutes = Math.floor(downtime / 60000);

    if (downtime > 0) {
    }

    // No need to resume queues - BullMQ handles retries automatically

    // Only clear and restart jobs if connectivity was lost for more than 1 minute
    if (downtime > 60000) {
      await this.deps.clearQueueAndRestartJobs();
    }

    // Always reconnect WebSocket
    const wsManager = this.deps.getWsManager();
    if (wsManager) {
      await this.deps.connectWebSocket();
    }

    // Always reinitialize listing scheduler
    if (wsManager) {
      await this.deps.initializeListingScheduler(wsManager);
    }

    // Reset connectivity lost timestamp
    this.connectivityLostAt = null;
  }

  /**
   * Start periodic connectivity monitoring
   * Checks every 60 seconds and handles connection loss/restoration
   */
  public startMonitoring(): void {
    if (this.connectivityCheckInterval) {
      warnLog('ConnectivityMonitor', 'Monitoring already started');
      return;
    }

    // Check connectivity every 60 seconds
    this.connectivityCheckInterval = setInterval(async () => {
      const currentlyConnected = await this.checkInternetConnectivity();

      if (!this.isConnected && currentlyConnected) {
        // Connection restored
        this.isConnected = true;
        await this.handleConnectivityRestored();
      } else if (this.isConnected && !currentlyConnected) {
        // Connection lost
        this.isConnected = false;
        this.connectivityLostAt = Date.now();
        errorLog('ConnectivityMonitor', 'Internet connectivity lost!');
      }
    }, STANDARD_INTERVAL);

  }

  /**
   * Stop connectivity monitoring
   */
  public stopMonitoring(): void {
    if (this.connectivityCheckInterval) {
      clearInterval(this.connectivityCheckInterval);
      this.connectivityCheckInterval = null;
    }
  }

  /**
   * Get current connectivity status
   */
  public getStatus() {
    return {
      isConnected: this.isConnected,
      connectivityLostAt: this.connectivityLostAt,
      downtime: this.connectivityLostAt ? Date.now() - this.connectivityLostAt : 0,
      isMonitoring: this.connectivityCheckInterval !== null
    };
  }

  /**
   * Get service statistics
   */
  public getStats() {
    return {
      isConnected: this.isConnected,
      isMonitoring: this.connectivityCheckInterval !== null,
      downtimeMs: this.connectivityLostAt ? Date.now() - this.connectivityLostAt : 0
    };
  }
}

export const connectivityMonitorService = new ConnectivityMonitorService();
