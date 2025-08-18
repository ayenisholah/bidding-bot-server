import { Cluster } from 'ioredis';
import { getRedisConnection, forceRedisSlotRefresh } from './redis';
import { logger, DatabaseOperation, ErrorCategory } from './logger';
import { EventEmitter } from 'events';

export interface ClusterHealthStatus {
  healthy: boolean;
  totalNodes: number;
  activeNodes: number;
  failedNodes: string[];
  clusterState: string;
  splitBrain: boolean;
  lastCheck: number;
}

export class RedisClusterMonitor extends EventEmitter {
  private monitorInterval: NodeJS.Timeout | null = null;
  private lastHealthStatus: ClusterHealthStatus | null = null;
  private consecutiveFailures: number = 0;
  private readonly maxConsecutiveFailures = 3;
  private clearQueueAndRestartJobsCallback: (() => Promise<void>) | null = null;
  private isRecoveringCluster: boolean = false;

  constructor(
    private checkInterval: number = 30000 // 30 seconds
  ) {
    super();
  }

  public start(): void {
    if (this.monitorInterval) {
      console.log('[RedisMonitor] Monitor already running');
      return;
    }

    console.log(`[RedisMonitor] Starting cluster health monitoring (interval: ${this.checkInterval}ms)`);
    
    // Initial check
    this.checkClusterHealth();
    
    // Set up periodic monitoring
    this.monitorInterval = setInterval(() => {
      this.checkClusterHealth();
    }, this.checkInterval);
  }

  public stop(): void {
    if (this.monitorInterval) {
      clearInterval(this.monitorInterval);
      this.monitorInterval = null;
      console.log('[RedisMonitor] Cluster monitoring stopped');
    }
  }

  private async checkClusterHealth(): Promise<void> {
    try {
      const cluster = getRedisConnection();
      const healthStatus = await this.getClusterHealth(cluster);
      
      // Log health status
      if (healthStatus.healthy) {
        if (this.consecutiveFailures > 0) {
          console.log(`[RedisMonitor] Cluster recovered after ${this.consecutiveFailures} failures`);
          this.emit('clusterRecovered', healthStatus);
        }
        this.consecutiveFailures = 0;
      } else {
        this.consecutiveFailures++;
        console.error(`[RedisMonitor] Cluster unhealthy (failure ${this.consecutiveFailures}/${this.maxConsecutiveFailures})`);
        
        // Log specific issues
        if (healthStatus.failedNodes.length > 0) {
          console.error(`[RedisMonitor] Failed nodes: ${healthStatus.failedNodes.join(', ')}`);
        }
        if (healthStatus.splitBrain) {
          console.error('[RedisMonitor] CRITICAL: Split-brain condition detected!');
        }
        
        this.emit('clusterUnhealthy', healthStatus);
        
        // Take action after consecutive failures
        if (this.consecutiveFailures >= this.maxConsecutiveFailures) {
          await this.handleClusterFailure(healthStatus);
        }
      }
      
      this.lastHealthStatus = healthStatus;
      
      // Log to Better Stack
      await logger.logInfo(ErrorCategory.DATABASE, 'Redis cluster health check', {
        databaseOperation: DatabaseOperation.CONNECTION,
        collection_name: 'redis_cluster_monitor',
        duration: Date.now() - healthStatus.lastCheck,
        metadata: {
          healthy: healthStatus.healthy,
          activeNodes: healthStatus.activeNodes,
          totalNodes: healthStatus.totalNodes,
          clusterState: healthStatus.clusterState,
          failedNodes: healthStatus.failedNodes,
          splitBrain: healthStatus.splitBrain
        }
      });
      
    } catch (error) {
      this.consecutiveFailures++;
      console.error(`[RedisMonitor] Error checking cluster health:`, error);
      
      await logger.logDatabaseError('Redis cluster monitor error', {
        databaseOperation: DatabaseOperation.CONNECTION,
        collection_name: 'redis_cluster_monitor',
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error),
        consecutiveFailures: this.consecutiveFailures
      });
      
      if (this.consecutiveFailures >= this.maxConsecutiveFailures) {
        this.emit('monitorError', error);
      }
    }
  }

  private async getClusterHealth(cluster: Cluster): Promise<ClusterHealthStatus> {
    const checkStartTime = Date.now();
    const status: ClusterHealthStatus = {
      healthy: false,
      totalNodes: 0,
      activeNodes: 0,
      failedNodes: [],
      clusterState: 'unknown',
      splitBrain: false,
      lastCheck: checkStartTime
    };

    try {
      // Get cluster info
      const clusterInfo = await cluster.cluster('INFO');
      const infoLines = clusterInfo.split('\n');
      
      for (const line of infoLines) {
        if (line.startsWith('cluster_state:')) {
          status.clusterState = line.split(':')[1].trim();
        } else if (line.startsWith('cluster_known_nodes:')) {
          status.totalNodes = parseInt(line.split(':')[1].trim());
        }
      }

      // Get nodes info
      const nodes = await cluster.cluster('NODES') as string;
      const nodeLines = nodes.split('\n').filter((line: string) => line.trim());
      
      const masterNodes = new Map<string, Set<string>>(); // master -> Set of slot ranges
      
      for (const nodeLine of nodeLines) {
        const parts = nodeLine.split(' ');
        if (parts.length < 8) continue;
        
        const nodeId = parts[0];
        const address = parts[1];
        const flags = parts[2];
        const masterId = parts[3];
        const state = parts[7];
        
        // Check if node is connected
        if (state === 'connected') {
          status.activeNodes++;
        } else {
          status.failedNodes.push(`${address} (${state})`);
        }

        // Track slave nodes and their masters
        if (flags.includes('slave') && masterId !== '-') {
          console.log(`[RedisMonitor] Slave node ${nodeId} at ${address} is connected to master ${masterId}`);
        }
        
        // Check for master nodes and their slots
        if (flags.includes('master') && parts.length > 8) {
          const slots = new Set<string>();
          for (let i = 8; i < parts.length; i++) {
            slots.add(parts[i]);
          }
          
          // Check if this master already exists with different slots (split-brain)
          for (const [existingMaster, existingSlots] of masterNodes.entries()) {
            // Check for overlapping slots
            for (const slot of slots) {
              if (existingSlots.has(slot)) {
                status.splitBrain = true;
                console.error(`[RedisMonitor] Split-brain detected: slot ${slot} claimed by both ${nodeId} and ${existingMaster}`);
                break;
              }
            }
          }
          
          masterNodes.set(nodeId, slots);
        }
      }

      // Determine overall health
      status.healthy = status.clusterState === 'ok' && 
                      status.activeNodes >= status.totalNodes * 0.5 && // At least 50% nodes active
                      !status.splitBrain &&
                      status.failedNodes.length === 0;

      return status;
      
    } catch (error) {
      console.error('[RedisMonitor] Failed to get cluster health:', error);
      return status;
    }
  }

  private async handleClusterFailure(healthStatus: ClusterHealthStatus): Promise<void> {
    console.error('[RedisMonitor] Handling cluster failure...');
    
    try {
      // Force slot refresh first
      await forceRedisSlotRefresh();
      console.log('[RedisMonitor] Forced slot refresh completed');
      
      // Check if split-brain or critical failure
      if (healthStatus.splitBrain || healthStatus.clusterState !== 'ok' || healthStatus.activeNodes < healthStatus.totalNodes / 2) {
        console.error('[RedisMonitor] Critical cluster failure detected - triggering recovery');
        
        // Call clearQueueAndRestartJobs if callback is set and not already recovering
        if (this.clearQueueAndRestartJobsCallback && !this.isRecoveringCluster) {
          this.isRecoveringCluster = true;
          console.log('[RedisMonitor] Calling clearQueueAndRestartJobs to recover cluster');
          
          try {
            await this.clearQueueAndRestartJobsCallback();
            console.log('[RedisMonitor] clearQueueAndRestartJobs completed successfully');
          } catch (recoveryError) {
            console.error('[RedisMonitor] Error during clearQueueAndRestartJobs:', recoveryError);
          } finally {
            // Reset recovery flag after some delay
            setTimeout(() => {
              this.isRecoveringCluster = false;
            }, 60000); // Wait 1 minute before allowing another recovery
          }
        } else if (this.isRecoveringCluster) {
          console.log('[RedisMonitor] Cluster recovery already in progress, skipping');
        }
      }
      
      // Emit critical failure event
      this.emit('clusterCriticalFailure', healthStatus);
      
      // Log critical event
      await logger.logDatabaseError('Redis cluster critical failure', {
        databaseOperation: DatabaseOperation.CONNECTION,
        collection_name: 'redis_cluster_critical',
        error: new Error('Cluster health check failed multiple times'),
        errorMessage: `Cluster state: ${healthStatus.clusterState}, Active nodes: ${healthStatus.activeNodes}/${healthStatus.totalNodes}`,
        metadata: healthStatus
      });
      
    } catch (error) {
      console.error('[RedisMonitor] Error handling cluster failure:', error);
    }
  }

  public getLastHealthStatus(): ClusterHealthStatus | null {
    return this.lastHealthStatus;
  }

  public isHealthy(): boolean {
    return this.lastHealthStatus?.healthy || false;
  }
  
  public setClearQueueAndRestartJobsCallback(callback: () => Promise<void>): void {
    this.clearQueueAndRestartJobsCallback = callback;
    console.log('[RedisMonitor] clearQueueAndRestartJobs callback registered');
  }
}

// Global instance
export const redisClusterMonitor = new RedisClusterMonitor();

// Helper function to start monitoring
export function startRedisClusterMonitoring(clearQueueAndRestartJobsCallback?: () => Promise<void>): void {
  // Set the callback if provided
  if (clearQueueAndRestartJobsCallback) {
    redisClusterMonitor.setClearQueueAndRestartJobsCallback(clearQueueAndRestartJobsCallback);
  }
  
  redisClusterMonitor.start();
  
  // Set up event handlers
  redisClusterMonitor.on('clusterUnhealthy', (status: ClusterHealthStatus) => {
    console.warn('[RedisMonitor] Cluster unhealthy event:', status);
  });
  
  redisClusterMonitor.on('clusterRecovered', (status: ClusterHealthStatus) => {
    console.log('[RedisMonitor] Cluster recovered event:', status);
  });
  
  redisClusterMonitor.on('clusterCriticalFailure', (status: ClusterHealthStatus) => {
    console.error('[RedisMonitor] CRITICAL: Cluster failure event:', status);
    // clearQueueAndRestartJobs will be called automatically if callback is set
  });
  
  redisClusterMonitor.on('monitorError', (error: Error) => {
    console.error('[RedisMonitor] Monitor error event:', error);
  });
}

// Helper function to stop monitoring
export function stopRedisClusterMonitoring(): void {
  redisClusterMonitor.stop();
}