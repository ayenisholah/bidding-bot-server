import { IWallet } from '../models/wallet.model';
import { decryptPrivateKey, getPassword } from './crypto';
import { errorLog } from './debug-file-logger';
import Wallet from '../models/wallet.model';

interface CachedWallet {
  _id: any;
  address: string;
  encryptedPrivateKey: string;
  name: string;
  user: string;
  tasks?: string[];
  decryptedPrivateKey?: string;
  lastUsed: number;
}

class WalletCache {
  private cache: Map<string, CachedWallet> = new Map();
  private decryptedKeys: Map<string, { key: string; timestamp: number }> = new Map();
  private readonly CACHE_TTL = 30 * 60 * 1000; // 30 minutes
  private readonly KEY_TTL = 5 * 60 * 1000; // 5 minutes for decrypted keys
  private readonly QUERY_TIMEOUT = 30000; // 30 seconds
  private password: string | null = null;
  private cleanupInterval: NodeJS.Timeout | null = null;

  // In-flight request tracking to prevent thundering herd problem
  // When multiple parallel requests come for the same wallet, only one MongoDB query is made
  private inFlightById: Map<string, Promise<IWallet | null>> = new Map();
  private inFlightByAddress: Map<string, Promise<IWallet | null>> = new Map();
  private inFlightByTaskId: Map<string, Promise<IWallet[]>> = new Map();

  constructor() {
    // Start cleanup interval
    this.cleanupInterval = setInterval(() => this.cleanup(), 60 * 1000); // Run every minute
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
        this.cache.set(address, {
          _id: wallet._id,
          address: wallet.address,
          encryptedPrivateKey: wallet.encryptedPrivateKey,
          name: wallet.name,
          user: String(wallet.user),
          tasks: (wallet as any).tasks,
          lastUsed: Date.now()
        });
      }

    } catch (error) {
      errorLog('[Database]', 'Failed to initialize wallet cache', error as Error);
    }
  }

  async getWallet(address: string): Promise<IWallet | null> {
    const normalizedAddress = address.toLowerCase();

    // Check cache first
    const cached = this.cache.get(normalizedAddress);
    if (cached) {
      cached.lastUsed = Date.now();
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
          this.cache.set(normalizedAddress, {
            _id: wallet._id,
            address: wallet.address,
            encryptedPrivateKey: wallet.encryptedPrivateKey,
            name: wallet.name,
            user: String(wallet.user),
            tasks: (wallet as any).tasks,
            lastUsed: Date.now()
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
    const address = wallet.address.toLowerCase();
    
    // Check decrypted key cache
    const cached = this.decryptedKeys.get(address);
    if (cached && Date.now() - cached.timestamp < this.KEY_TTL) {
      return cached.key;
    }

    // Decrypt the key
    try {
      if (!this.password) {
        this.password = getPassword() || null;
      }
      
      const decryptedKey = decryptPrivateKey(wallet.encryptedPrivateKey);
      
      // Cache the decrypted key
      this.decryptedKeys.set(address, {
        key: decryptedKey,
        timestamp: Date.now()
      });

      return decryptedKey;
    } catch (error) {
      errorLog('[Database]', `Failed to decrypt private key for wallet - address=${address}`, error as Error);
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
            tasks: (wallet as any).tasks,
            lastUsed: Date.now()
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
        cached.lastUsed = Date.now();
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
          this.cache.set(address, {
            _id: wallet._id,
            address: wallet.address,
            encryptedPrivateKey: wallet.encryptedPrivateKey,
            name: wallet.name,
            user: String(wallet.user),
            tasks: (wallet as any).tasks,
            lastUsed: Date.now()
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
    this.cache.delete(normalizedAddress);
    this.decryptedKeys.delete(normalizedAddress);
  }

  invalidateAll() {
    this.cache.clear();
    this.decryptedKeys.clear();
  }

  private cleanup() {
    const now = Date.now();
    
    // Clean up old cached wallets
    for (const [address, wallet] of this.cache.entries()) {
      if (now - wallet.lastUsed > this.CACHE_TTL) {
        this.cache.delete(address);
      }
    }
    
    // Clean up old decrypted keys
    for (const [address, data] of this.decryptedKeys.entries()) {
      if (now - data.timestamp > this.KEY_TTL) {
        this.decryptedKeys.delete(address);
      }
    }
  }

  destroy() {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
    this.cache.clear();
    this.decryptedKeys.clear();
  }
}

// Export singleton instance
export const walletCache = new WalletCache();