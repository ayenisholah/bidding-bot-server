import { IWallet } from '../models/wallet.model';
import { decryptPrivateKey, getPassword, clearDecryptedKey } from './crypto';
import { errorLog, debugLog } from './debug-file-logger';
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
  // LRU cache with no TTL - only size-based eviction (max 100 entries)
  private cache: LRUCache<string, CachedWallet>;
  private readonly QUERY_TIMEOUT = 30000; // 30 seconds
  private password: string | null = null;

  // In-flight request tracking to prevent thundering herd problem
  // When multiple parallel requests come for the same wallet, only one MongoDB query is made
  private inFlightById: Map<string, Promise<IWallet | null>> = new Map();
  private inFlightByAddress: Map<string, Promise<IWallet | null>> = new Map();
  private inFlightByTaskId: Map<string, Promise<IWallet[]>> = new Map();

  constructor() {
    // Initialize wallet metadata cache (NO TTL, size-based eviction only)
    this.cache = new LRUCache<string, CachedWallet>({
      max: 100,  // Max 100 wallets
      updateAgeOnGet: false,  // Don't track access time (not needed without TTL)
      allowStale: false,
      dispose: (value, key) => {
        debugLog('WalletCache', `LRU_EVICT_WALLET | address=${key} | reason=cache full | cacheSize=${this.cache.size}/100`);
      }
    });

    debugLog('WalletCache', 'LRU cache initialized | walletMax=100 | TTL=NONE | eviction=LRU_only');
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
    debugLog('WalletCache', 'Initializing wallet cache with password');
    await this.loadAllWallets();
    debugLog('WalletCache', `Wallet cache initialized | wallets=${this.cache.size} | max=100`);
  }

  private async loadAllWallets() {
    try {
      const wallets = await Wallet.find({}).lean();
      debugLog('WalletCache', `Loading ${wallets.length} wallets from database`);

      for (const wallet of wallets) {
        const address = wallet.address.toLowerCase();
        this.cache.set(address, {
          _id: wallet._id,
          address: wallet.address,
          encryptedPrivateKey: wallet.encryptedPrivateKey,
          name: wallet.name,
          user: String(wallet.user),
          tasks: (wallet as any).tasks
        });
      }

      debugLog('WalletCache', `Loaded ${this.cache.size} wallets into LRU cache (NO TTL, max=100)`);
    } catch (error) {
      errorLog('[Database]', 'Failed to initialize wallet cache', error as Error);
    }
  }

  async getWallet(address: string): Promise<IWallet | null> {
    const normalizedAddress = address.toLowerCase();

    // Check cache first
    const cached = this.cache.get(normalizedAddress);
    if (cached) {
      debugLog('WalletCache', `CACHE_HIT | wallet=${normalizedAddress} | cacheSize=${this.cache.size}/100`);
      return cached as unknown as IWallet;
    }

    debugLog('WalletCache', `CACHE_MISS | wallet=${normalizedAddress} | fetching from DB`);

    // Check if there's already an in-flight request for this address
    const inFlight = this.inFlightByAddress.get(normalizedAddress);
    if (inFlight) {
      debugLog('WalletCache', `IN_FLIGHT | wallet=${normalizedAddress} | waiting for existing request`);
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
          this.cache.set(normalizedAddress, {
            _id: wallet._id,
            address: wallet.address,
            encryptedPrivateKey: wallet.encryptedPrivateKey,
            name: wallet.name,
            user: String(wallet.user),
            tasks: (wallet as any).tasks
          });
          debugLog('WalletCache', `DB_FETCH_SUCCESS | wallet=${normalizedAddress} | cached | cacheSize=${this.cache.size}/100`);
          return wallet;
        }
        debugLog('WalletCache', `DB_FETCH_NOT_FOUND | wallet=${normalizedAddress}`);
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
    const allWallets = Array.from(this.cache.values());
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
          this.cache.set(address, {
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
    if (this.cache.size === 0) {
      await this.loadAllWallets();
    }
    
    return Array.from(this.cache.values()) as unknown as IWallet[];
  }

  async getWalletByAddress(address: string): Promise<IWallet | null> {
    return this.getWallet(address);
  }

  async getWalletById(walletId: string): Promise<IWallet | null> {
    // First check if we have it in cache by scanning
    for (const cached of this.cache.values()) {
      if (cached._id.toString() === walletId) {
        debugLog('WalletCache', `CACHE_HIT_BY_ID | walletId=${walletId} | cacheSize=${this.cache.size}/100`);
        return cached as unknown as IWallet;
      }
    }

    debugLog('WalletCache', `CACHE_MISS_BY_ID | walletId=${walletId} | fetching from DB`);

    // Check if there's already an in-flight request for this wallet ID
    const inFlight = this.inFlightById.get(walletId);
    if (inFlight) {
      debugLog('WalletCache', `IN_FLIGHT_BY_ID | walletId=${walletId} | waiting for existing request`);
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
          this.cache.set(address, {
            _id: wallet._id,
            address: wallet.address,
            encryptedPrivateKey: wallet.encryptedPrivateKey,
            name: wallet.name,
            user: String(wallet.user),
            tasks: (wallet as any).tasks
          });
          debugLog('WalletCache', `DB_FETCH_SUCCESS_BY_ID | walletId=${walletId} | address=${address} | cached | cacheSize=${this.cache.size}/100`);
          return wallet;
        }
        debugLog('WalletCache', `DB_FETCH_NOT_FOUND_BY_ID | walletId=${walletId}`);
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
    const cached = this.cache.get(normalizedAddress);
    if (cached) {
      // Clear decrypted key from crypto.ts cache
      clearDecryptedKey(cached.encryptedPrivateKey);
    }

    // Clear wallet metadata from this cache
    this.cache.delete(normalizedAddress);
    debugLog('WalletCache', `INVALIDATE_WALLET | address=${normalizedAddress} | cacheSize=${this.cache.size}/100`);
  }

  invalidateAll() {
    const walletCount = this.cache.size;
    this.cache.clear();
    debugLog('WalletCache', `INVALIDATE_ALL | cleared=${walletCount} wallets | cacheSize=0/100`);
  }

  destroy() {
    const walletCount = this.cache.size;
    this.cache.clear();
    debugLog('WalletCache', `DESTROY | cleared=${walletCount} wallets | cache destroyed`);
  }
}

// Export singleton instance
export const walletCache = new WalletCache();