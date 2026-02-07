import { IWallet } from '../models/wallet.model';
import { decryptPrivateKey, clearDecryptedKey } from './crypto';
import { errorLog} from './debug-file-logger';
import Wallet from '../models/wallet.model';
import { LRUCache } from 'lru-cache';

interface CachedWallet {
  _id: any;
  address: string;
  encryptedPrivateKey: string;
  name: string;
  user: string;
  tasks?: string[];
  decryptedPrivateKey?: string;
}

class WalletCache {
  // LRU cache with no TTL - only size-based eviction (max 1000 entries)
  private walletDataLRU: LRUCache<string, CachedWallet>;
  private readonly QUERY_TIMEOUT = 30000; // 30 seconds
  private password: string | null = null;

  // In-flight request tracking to prevent thundering herd problem
  // When multiple parallel requests come for the same wallet, only one MongoDB query is made
  private inFlightById: Map<string, Promise<IWallet | null>> = new Map();
  private inFlightByAddress: Map<string, Promise<IWallet | null>> = new Map();
  private inFlightByTaskId: Map<string, Promise<IWallet[]>> = new Map();

  constructor() {
    // Initialize wallet metadata cache (NO TTL, size-based eviction only)
    this.walletDataLRU = new LRUCache<string, CachedWallet>({
      max: 1000,  // Max 1000 wallets (standardized minimum)
      ttl: 24 * 60 * 60 * 1000, // 24 hours TTL (ensure metadata freshness)
      updateAgeOnGet: false,  // Don't reset TTL on reads
      allowStale: false,
      dispose: (value, key) => {
      }
    });

  }

  /**
   * Wrap MongoDB query with timeout to prevent hung requests
   */
  private async withTimeout<T>(promise: Promise<T>, timeoutMs: number, operation: string): Promise<T> {
    const timeoutPromise = new Promise<never>((_, reject) => {
      setTimeout(() => reject(new Error(`MongoDB timeout: ${operation}`)), timeoutMs);
    });
    return Promise.race([promise, timeoutPromise]);
  }

  async initialize(password: string) {
    this.password = password;
    await this.loadAllWallets();
  }

  private async loadAllWallets() {
    try {
      const wallets = await Wallet.find({}).lean();

      for (const wallet of wallets) {
        const address = wallet.address.toLowerCase();
        this.walletDataLRU.set(address, {
          _id: wallet._id,
          address: wallet.address,
          encryptedPrivateKey: wallet.encryptedPrivateKey,
          name: wallet.name,
          user: String(wallet.user),
          tasks: (wallet as any).tasks
        });
      }

    } catch (error) {
      errorLog('[Database]', 'Failed to initialize wallet cache', error as Error);
    }
  }

  async getWallet(address: string): Promise<IWallet | null> {
    const normalizedAddress = address.toLowerCase();

    // Check cache first
    const cached = this.walletDataLRU.get(normalizedAddress);
    if (cached) {
      return cached as unknown as IWallet;
    }


    // Check if there's already an in-flight request for this address
    const inFlight = this.inFlightByAddress.get(normalizedAddress);
    if (inFlight) {
      return inFlight;
    }

    // Create new request and track it to prevent thundering herd
    const request = (async (): Promise<IWallet | null> => {
      try {
        const wallet = await this.withTimeout(
          Wallet.findOne({ address: normalizedAddress }).lean(),
          this.QUERY_TIMEOUT,
          `getWalletByAddress(${normalizedAddress})`
        );
        if (wallet) {
          this.walletDataLRU.set(normalizedAddress, {
            _id: wallet._id,
            address: wallet.address,
            encryptedPrivateKey: wallet.encryptedPrivateKey,
            name: wallet.name,
            user: String(wallet.user),
            tasks: (wallet as any).tasks
          });
          return wallet;
        }
        return null;
      } catch (error) {
        errorLog('[Database]', `Failed to fetch wallet from database - address=${address}`, error as Error);
        return null;
      } finally {
        // Cleanup in-flight tracking
        this.inFlightByAddress.delete(normalizedAddress);
      }
    })();

    this.inFlightByAddress.set(normalizedAddress, request);
    return request;
  }

  async getDecryptedPrivateKey(wallet: IWallet): Promise<string | null> {
    // Use crypto.ts decryptPrivateKey() directly (it has its own LRU cache)
    try {
      const decryptedKey = decryptPrivateKey(wallet.encryptedPrivateKey);
      return decryptedKey;
    } catch (error) {
      errorLog('[Database]', `Failed to decrypt private key for wallet - address=${wallet.address}`, error as Error);
      return null;
    }
  }

  async findWalletsByTaskId(taskId: string): Promise<IWallet[]> {
    // First check if we have all wallets in cache
    const allWallets = Array.from(this.walletDataLRU.values());
    if (allWallets.length > 0) {
      return allWallets.filter(w => w.tasks && Array.isArray(w.tasks) && w.tasks.includes(taskId)) as unknown as IWallet[];
    }

    // Check if there's already an in-flight request for this task ID
    const inFlight = this.inFlightByTaskId.get(taskId);
    if (inFlight) {
      return inFlight;
    }

    // Create new request and track it to prevent thundering herd
    const request = (async (): Promise<IWallet[]> => {
      try {
        const wallets = await this.withTimeout(
          Wallet.find({ tasks: taskId }).lean(),
          this.QUERY_TIMEOUT,
          `findWalletsByTaskId(${taskId})`
        );
        // Update cache
        for (const wallet of wallets) {
          const address = wallet.address.toLowerCase();
          this.walletDataLRU.set(address, {
            _id: wallet._id,
            address: wallet.address,
            encryptedPrivateKey: wallet.encryptedPrivateKey,
            name: wallet.name,
            user: String(wallet.user),
            tasks: (wallet as any).tasks
          });
        }
        return wallets;
      } catch (error) {
        errorLog('[Database]', `Failed to find wallets for task - taskId=${taskId}`, error as Error);
        return [];
      } finally {
        // Cleanup in-flight tracking
        this.inFlightByTaskId.delete(taskId);
      }
    })();

    this.inFlightByTaskId.set(taskId, request);
    return request;
  }

  async getAllWallets(): Promise<IWallet[]> {
    // If cache is empty, load from database
    if (this.walletDataLRU.size === 0) {
      await this.loadAllWallets();
    }

    return Array.from(this.walletDataLRU.values()) as unknown as IWallet[];
  }

  async getWalletByAddress(address: string): Promise<IWallet | null> {
    return this.getWallet(address);
  }

  async getWalletById(walletId: string): Promise<IWallet | null> {
    // First check if we have it in cache by scanning
    for (const cached of this.walletDataLRU.values()) {
      if (cached._id.toString() === walletId) {
        // Cache hit - no need to log on every hit (too verbose)
        return cached as unknown as IWallet;
      }
    }


    // Check if there's already an in-flight request for this wallet ID
    const inFlight = this.inFlightById.get(walletId);
    if (inFlight) {
      return inFlight;
    }

    // Create new request and track it to prevent thundering herd
    const request = (async (): Promise<IWallet | null> => {
      try {
        const wallet = await this.withTimeout(
          Wallet.findById(walletId).lean(),
          this.QUERY_TIMEOUT,
          `getWalletById(${walletId})`
        );
        if (wallet) {
          const address = wallet.address.toLowerCase();
          this.walletDataLRU.set(address, {
            _id: wallet._id,
            address: wallet.address,
            encryptedPrivateKey: wallet.encryptedPrivateKey,
            name: wallet.name,
            user: String(wallet.user),
            tasks: (wallet as any).tasks
          });
          return wallet;
        }
        return null;
      } catch (error) {
        errorLog('[Database]', `Failed to fetch wallet by ID from database - walletId=${walletId}`, error as Error);
        return null;
      } finally {
        // Cleanup in-flight tracking
        this.inFlightById.delete(walletId);
      }
    })();

    this.inFlightById.set(walletId, request);
    return request;
  }

  invalidateWallet(address: string) {
    const normalizedAddress = address.toLowerCase();

    // Get cached wallet to access encryptedPrivateKey before deletion
    const cached = this.walletDataLRU.get(normalizedAddress);
    if (cached) {
      // Clear decrypted key from crypto.ts cache
      clearDecryptedKey(cached.encryptedPrivateKey);
    }

    // Clear wallet metadata from this cache
    this.walletDataLRU.delete(normalizedAddress);
  }

  invalidateAll() {
    const walletCount = this.walletDataLRU.size;
    this.walletDataLRU.clear();
  }

  destroy() {
    const walletCount = this.walletDataLRU.size;
    this.walletDataLRU.clear();
  }

  /**
   * Get the internal LRU cache for debugging/monitoring
   * @internal - For use by lru-cache-sampler.ts only
   */
  public getCache(): LRUCache<string, CachedWallet> {
    return this.walletDataLRU;
  }
}

// Export singleton instance
export const walletCache = new WalletCache();