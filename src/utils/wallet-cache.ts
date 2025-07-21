import { IWallet } from '../models/wallet.model';
import { decryptPrivateKey, getPassword } from './crypto';
import { logger, ErrorCategory } from './logger';
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
  private password: string | null = null;
  private cleanupInterval: NodeJS.Timeout | null = null;

  constructor() {
    // Start cleanup interval
    this.cleanupInterval = setInterval(() => this.cleanup(), 60 * 1000); // Run every minute
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
          user: wallet.user,
          tasks: wallet.tasks,
          lastUsed: Date.now()
        });
      }

    } catch (error) {
      await logger.logError(ErrorCategory.DATABASE, 'Failed to initialize wallet cache', error as Error);
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

    // Cache miss - fetch from database
    try {
      const wallet = await Wallet.findOne({ address: normalizedAddress }).lean();
      if (wallet) {
        this.cache.set(normalizedAddress, {
          ...wallet,
          lastUsed: Date.now()
        });
        return wallet;
      }
    } catch (error) {
      await logger.logError(ErrorCategory.DATABASE, `Failed to fetch wallet ${address} from database`, error as Error);
    }

    return null;
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
      
      const decryptedKey = await decryptPrivateKey(wallet.encryptedPrivateKey);
      
      // Cache the decrypted key
      this.decryptedKeys.set(address, {
        key: decryptedKey,
        timestamp: Date.now()
      });
      
      return decryptedKey;
    } catch (error) {
      await logger.logError(ErrorCategory.SECURITY, `Failed to decrypt private key for wallet ${address}`, error as Error);
      return null;
    }
  }

  async findWalletsByTaskId(taskId: string): Promise<IWallet[]> {
    // First check if we have all wallets in cache
    const allWallets = Array.from(this.cache.values());
    if (allWallets.length > 0) {
      return allWallets.filter(w => w.tasks && Array.isArray(w.tasks) && w.tasks.includes(taskId)) as unknown as IWallet[];
    }

    // Fallback to database
    try {
      const wallets = await Wallet.find({ tasks: taskId }).lean();
      // Update cache
      for (const wallet of wallets) {
        const address = wallet.address.toLowerCase();
        this.cache.set(address, {
          ...wallet,
          lastUsed: Date.now()
        });
      }
      return wallets;
    } catch (error) {
      await logger.logError(ErrorCategory.DATABASE, `Failed to find wallets for task ${taskId}`, error as Error);
      return [];
    }
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