import crypto from 'crypto';
import { config } from "dotenv"
import { errorLog, warnLog } from './debug-file-logger'
import { LRUCache } from 'lru-cache';

config()

const ALGORITHM = 'aes-256-gcm';
const DETERMINISTIC_ALGORITHM = "aes-256-ecb";

// Password persistence configuration (development only)
const PASSWORD_TTL = 604800; // Default 7 days (7 * 24 * 60 * 60)

// In-memory password storage
let PASSWORD: string | undefined = undefined
let PASSWORD_TIMESTAMP = 0; // Track when password was set

// Rate limiter for no password error logging
let lastNoPasswordLogTime = 0;
const NO_PASSWORD_LOG_INTERVAL = 60000; // Log once per minute max

// =============================================================================
// DECRYPTED PRIVATE KEY CACHE (LRU, No TTL)
// =============================================================================

// Cache decrypted private keys (no TTL, persists until server restart)
// Max 1000 wallets (standardized minimum cache size, LRU eviction on capacity)
const decryptedKeyCache = new LRUCache<string, string>({
  max: 1000,              // Max 1000 unique wallets (standardized)
  ttl: 24 * 60 * 60 * 1000, // 24 hours TTL (security - limit plaintext key exposure)
  allowStale: false,
  updateAgeOnGet: true    // LRU behavior - keep hot keys fresh
});

// Stats tracking
let cacheStats = {
  hits: 0,
  misses: 0,
  sets: 0
};

// =============================================================================
// IN-MEMORY PASSWORD STORAGE (Development)
// =============================================================================

// Check if password has expired (development only)
function isPasswordExpired(): boolean {
  if (process.env.NODE_ENV === 'production') return false;
  if (!PASSWORD || PASSWORD_TIMESTAMP === 0) return true;

  const elapsed = Date.now() - PASSWORD_TIMESTAMP;
  return elapsed > (PASSWORD_TTL * 1000);
}

// Try to recover password from memory on startup (development only)
export async function recoverPasswordFromRedis(): Promise<boolean> {
  // Only attempt recovery in development
  if (process.env.NODE_ENV === 'production') {
    return false;
  }

  // Check if we have a non-expired password in memory
  if (PASSWORD && !isPasswordExpired()) {
    const remainingMs = (PASSWORD_TTL * 1000) - (Date.now() - PASSWORD_TIMESTAMP);
    const days = Math.floor(remainingMs / (1000 * 60 * 60 * 24));
    const hours = Math.floor((remainingMs % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
    return true;
  }

  // Password expired or not set
  return false;
}

export async function setPassword(password: string): Promise<void> {
  PASSWORD = password;
  PASSWORD_TIMESTAMP = Date.now();

  // Clear decrypted key cache when password changes
  clearAllDecryptedKeys();

  // Clear wallet cache when password changes (handled by wallet-cache.ts)
  // Note: Caller should call walletCache.invalidateAll() after this

  // Development only: password stored in memory with timestamp
  if (process.env.NODE_ENV !== 'production') {
    const days = Math.floor(PASSWORD_TTL / 86400);
    const hours = Math.floor((PASSWORD_TTL % 86400) / 3600);
  }
  // Production: password only stored in memory (no persistence)
}

export function getPassword(): string | undefined {
  return PASSWORD;
}

// Renew password TTL without changing the password (development only)
export async function renewPasswordTTL(): Promise<void> {
  if (!PASSWORD || PASSWORD_TTL <= 0 || process.env.NODE_ENV === 'production') return;

  // Check if password is expired
  if (isPasswordExpired()) {
    warnLog('Crypto', '[DEV] Cannot renew TTL - password has expired');
    return;
  }

  // Refresh timestamp
  PASSWORD_TIMESTAMP = Date.now();
  const days = Math.floor(PASSWORD_TTL / 86400);
  const hours = Math.floor((PASSWORD_TTL % 86400) / 3600);
}

// Clear password from all storage
export async function clearPassword(): Promise<void> {
  PASSWORD = undefined;
  PASSWORD_TIMESTAMP = 0;

  // Clear decrypted key cache when password is cleared
  clearAllDecryptedKeys();

  // Clear wallet cache when password is cleared (handled by wallet-cache.ts)
  // Note: Caller should call walletCache.invalidateAll() after this

  // Development only: already cleared from memory
  if (process.env.NODE_ENV !== 'production') {
  }
  // Production: password only in memory, already cleared above
}

function getEncryptionKey(): Buffer {
  // console.log('🔑 [getEncryptionKey] Getting encryption key...');
  let password = PASSWORD; // Current request
  // console.log(`🔑 [getEncryptionKey] Password is ${password ? 'SET' : 'NOT SET'}`);

  // If we have a password, use it for encryption
  if (password) {
    // console.log('🔑 [getEncryptionKey] Password found, deriving key...');
    const salt = crypto
      .createHash("sha256")
      .update("NFT_BIDDING_BOT_USER_KEY")
      .update(process.env.JWT_SECRET || "default")
      .digest();

    const key = crypto.scryptSync(password, salt as any, 32);
    // console.log('🔑 [getEncryptionKey] Key derived successfully');
    return key;
  }

  // No password available - throw error
  // Rate-limited logging to prevent spam
  const now = Date.now();
  if (now - lastNoPasswordLogTime > NO_PASSWORD_LOG_INTERVAL) {
    warnLog('Crypto', 'No password available - wallet encryption operations paused. Provide password via API to resume.');
    lastNoPasswordLogTime = now;
  }

  const error = new Error("No password available. Please provide a password for encryption.");
  (error as any).code = 'NO_PASSWORD';
  throw error;

}

// Deterministic password encryption/decryption functions
// Uses ENCRYPTION_KEY (unique per installation) for better security
function getPasswordEncryptionKey(): Buffer {
  const encryptionKey = process.env.ENCRYPTION_KEY;

  if (!encryptionKey) {
    // Fall back to JWT_SECRET for backwards compatibility during migration
    const fallback = process.env.JWT_SECRET || "default-jwt-secret";
    warnLog('Crypto', 'ENCRYPTION_KEY not set, using JWT_SECRET fallback (less secure)');
    return crypto
      .createHash("sha256")
      .update("password-encryption-key")
      .update(fallback)
      .digest();
  }

  // Use ENCRYPTION_KEY with additional key derivation for security
  const intermediate = crypto
    .createHash("sha512")
    .update(encryptionKey)
    .update("nft-bidding-bot-password-key-v1")
    .digest();

  return crypto
    .createHash("sha256")
    .update(intermediate)
    .digest();
}

export function encryptPrivateKey(privateKey: string): string {
  const key = getEncryptionKey();
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv);

  let encrypted = cipher.update(privateKey, 'utf8', 'hex');
  encrypted += cipher.final('hex');

  const authTag = cipher.getAuthTag();

  // Combine iv:authTag:encrypted into single string
  return `${iv.toString('hex')}:${authTag.toString('hex')}:${encrypted}`;
}

export function decryptPrivateKey(encryptedData: string): string {
  // Check cache first (INSTANT if hit)
  const cached = decryptedKeyCache.get(encryptedData);
  if (cached) {
    cacheStats.hits++;
    return cached;
  }

  cacheStats.misses++;

  // Validate input data
  if (encryptedData === null || encryptedData === undefined) {
    errorLog('Crypto', 'decryptPrivateKey: encryptedData is null or undefined');
    throw new Error('Encrypted data is null or undefined');
  }

  if (typeof encryptedData !== 'string') {
    errorLog('Crypto', `decryptPrivateKey: encryptedData is not a string | type=${typeof encryptedData}`);
    throw new Error(`Encrypted data must be a string, received: ${typeof encryptedData}`);
  }

  if (encryptedData.trim() === '') {
    errorLog('Crypto', 'decryptPrivateKey: encryptedData is empty string');
    throw new Error('Encrypted data is empty');
  }

  // Get encryption key
  let key: Buffer;
  try {
    key = getEncryptionKey();
  } catch (error) {
    errorLog('Crypto', 'decryptPrivateKey: Failed to get encryption key', error);
    throw error;
  }

  const parts = encryptedData.split(':');

  if (parts.length !== 3) {
    errorLog('Crypto', `decryptPrivateKey: Invalid format | expected=3 parts | got=${parts.length}`);
    throw new Error('Invalid encrypted data format');
  }

  const [ivHex, authTagHex, encrypted] = parts;

  try {
    const iv = Buffer.from(ivHex, 'hex');
    const authTag = Buffer.from(authTagHex, 'hex');
    const decipher = crypto.createDecipheriv(ALGORITHM, key, iv);

    decipher.setAuthTag(authTag);

    let decrypted = decipher.update(encrypted, 'hex', 'utf8');
    decrypted += decipher.final('utf8');

    // Cache the decrypted key (infinite TTL)
    decryptedKeyCache.set(encryptedData, decrypted);
    cacheStats.sets++;

    return decrypted;
  } catch (error) {
    errorLog('Crypto', 'decryptPrivateKey: Decryption failed', error);
    throw error;
  }
}

// Re-encrypt a wallet's private key with a new password
export async function reEncryptWallet(
  encryptedPrivateKey: string,
  oldPassword: string,
  newPassword: string
): Promise<string> {
  // console.log('🔐 [reEncryptWallet] Starting wallet re-encryption...');

  try {
    // Temporarily set the old password to decrypt
    const currentPassword = PASSWORD;
    PASSWORD = oldPassword;

    // Decrypt with old password
    const decryptedKey = await decryptPrivateKey(encryptedPrivateKey);
    // console.log('🔐 [reEncryptWallet] Successfully decrypted with old password');

    // Set new password and encrypt
    PASSWORD = newPassword;
    const newEncryptedKey = await encryptPrivateKey(decryptedKey);
    // console.log('🔐 [reEncryptWallet] Successfully encrypted with new password');

    // Restore the current password (will be the new one after change)
    PASSWORD = currentPassword;

    return newEncryptedKey;
  } catch (error) {
    errorLog('Crypto', 'reEncryptWallet: Re-encryption failed', error);
    // Restore the current password on error
    PASSWORD = getPassword();
    throw error;
  }
}

/**
 * Clear decrypted key from cache (call when wallet is deleted/updated)
 */
export function clearDecryptedKey(encryptedPrivateKeyHex: string): void {
  const deleted = decryptedKeyCache.delete(encryptedPrivateKeyHex);
  if (deleted) {
  }
}

/**
 * Clear all decrypted keys from cache (call on password change)
 */
export function clearAllDecryptedKeys(): void {
  const size = decryptedKeyCache.size;
  decryptedKeyCache.clear();
  cacheStats = { hits: 0, misses: 0, sets: 0 };
}

/**
 * Get cache statistics for monitoring
 */
export function getDecryptedKeyCacheStats() {
  const total = cacheStats.hits + cacheStats.misses;
  return {
    size: decryptedKeyCache.size,
    max: 1000,
    hits: cacheStats.hits,
    misses: cacheStats.misses,
    sets: cacheStats.sets,
    hitRate: total > 0 ? ((cacheStats.hits / total) * 100).toFixed(1) + '%' : '0%',
    ttl: 'infinite (until server restart)'
  };
}

// Legacy function names for backward compatibility
export function clearDecryptedKeyCache(): void {
  clearAllDecryptedKeys();
}

export function clearDecryptionCache(): void {
  clearAllDecryptedKeys();
}

export function getDecryptionCacheStats() {
  return getDecryptedKeyCacheStats();
}

// Password encryption using GCM mode (secure, non-deterministic)
// Format: "gcm:iv:authTag:encrypted" for new GCM encrypted passwords
// Legacy ECB format: just hex string (no colons) - for backward compatibility
export function encryptPassword(password: string): string {

  const key = getPasswordEncryptionKey();
  const iv = crypto.randomBytes(16);

  // Use GCM mode for secure encryption
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv);

  let encrypted = cipher.update(password, "utf8", "hex");
  encrypted += cipher.final("hex");

  const authTag = cipher.getAuthTag();

  // Prefix with "gcm:" to identify new format
  const result = `gcm:${iv.toString('hex')}:${authTag.toString('hex')}:${encrypted}`;

  return result;
}

export function decryptPassword(encryptedPassword: string): string {
  const isGcmFormat = encryptedPassword.startsWith('gcm:');

  const key = getPasswordEncryptionKey();

  try {
    // Check if this is new GCM format (starts with "gcm:")
    if (isGcmFormat) {

      // New GCM format: gcm:iv:authTag:encrypted
      const parts = encryptedPassword.slice(4).split(':'); // Remove "gcm:" prefix
      if (parts.length !== 3) {
        throw new Error('Invalid GCM encrypted password format');
      }

      const [ivHex, authTagHex, encrypted] = parts;
      const iv = Buffer.from(ivHex, 'hex');
      const authTag = Buffer.from(authTagHex, 'hex');

      const decipher = crypto.createDecipheriv(ALGORITHM, key, iv);
      decipher.setAuthTag(authTag);

      let decrypted = decipher.update(encrypted, 'hex', 'utf8');
      decrypted += decipher.final('utf8');

      return decrypted;
    }

    // Legacy ECB format (no colons or doesn't start with "gcm:")
    // This provides backward compatibility during migration
    warnLog('Crypto', 'Decrypting password with legacy ECB mode - will be re-encrypted with GCM on next use');

    const decipher = crypto.createDecipheriv(DETERMINISTIC_ALGORITHM, key, null);

    let decrypted = decipher.update(encryptedPassword, "hex", "utf8");
    decrypted += decipher.final("utf8");

    return decrypted;
  } catch (error) {
    errorLog('Crypto', 'Failed to decrypt password', error);
    throw new Error("Failed to decrypt password");
  }
}

// =============================================================================
// CACHE STATS LOGGING
// =============================================================================

// Log cache stats every 5 minutes
setInterval(() => {
  const stats = getDecryptedKeyCacheStats();
}, 5 * 60 * 1000);
/**
 * Get decrypted key cache for debugging/monitoring
 * @internal - For use by lru-cache-sampler.ts only
 */
export function getDecryptedKeyCache(): LRUCache<string, string> {
  return decryptedKeyCache;
}
