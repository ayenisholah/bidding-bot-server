import crypto from 'crypto';
import { config } from "dotenv"
import { getRedisConnection } from '../utils/redis'
import { debugLog, errorLog, warnLog } from './debug-file-logger'

config()

const ALGORITHM = 'aes-256-gcm';
const DETERMINISTIC_ALGORITHM = "aes-256-ecb";

// Cache for decrypted private keys
const decryptedKeyCache = new Map<string, { key: string; timestamp: number }>();
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes cache TTL
const MAX_CACHE_SIZE = 100; // Maximum number of cached keys

// Password persistence configuration (development only)
const PASSWORD_REDIS_KEY = 'nft-bot:server:password'; // Single Redis node key
const PASSWORD_TTL = 604800; // Default 7 days (7 * 24 * 60 * 60)

// In-memory password storage
let PASSWORD: string | undefined = undefined

// Rate limiter for no password error logging
let lastNoPasswordLogTime = 0;
const NO_PASSWORD_LOG_INTERVAL = 60000; // Log once per minute max

// =============================================================================
// REDIS-BASED PASSWORD STORAGE (Development)
// =============================================================================

// Try to recover password from Redis on startup (development only)
export async function recoverPasswordFromRedis(): Promise<boolean> {
  // Only attempt recovery in development
  if (process.env.NODE_ENV === 'production') {
    // console.log('🔑 [PRODUCTION] Password recovery from Redis disabled');
    return false;
  }

  try {
    // console.log(`🔍 [DEVELOPMENT] Attempting to recover password from Redis key: ${PASSWORD_REDIS_KEY}`);
    const redis = getRedisConnection();

    // Check if key exists
    const exists = await redis.exists(PASSWORD_REDIS_KEY);
    // console.log(`🔍 Redis key exists: ${exists === 1 ? 'YES' : 'NO'}`);

    if (!exists) {
      // console.log('🔍 No password found in Redis');
      return false;
    }

    const storedPassword = await redis.get(PASSWORD_REDIS_KEY);
    if (storedPassword) {
      PASSWORD = storedPassword;
      // console.log('✅ Password successfully retrieved from Redis');

      // Get remaining TTL
      const ttl = await redis.ttl(PASSWORD_REDIS_KEY);
      if (ttl > 0) {
        // const days = Math.floor(ttl / 86400);
        // const hours = Math.floor((ttl % 86400) / 3600);
        // console.log(`🔑 Password recovered from Redis (expires in ${days} days, ${hours} hours)`);
      } else if (ttl === -1) {
        // console.log('🔑 Password recovered from Redis (no expiration set)');
      } else {
        // console.log('🔑 Password recovered from Redis storage');
      }

      // Refresh TTL to full duration on recovery
      if (PASSWORD_TTL > 0) {
        await redis.expire(PASSWORD_REDIS_KEY, PASSWORD_TTL);
        // const days = Math.floor(PASSWORD_TTL / 86400);
        // const hours = Math.floor((PASSWORD_TTL % 86400) / 3600);
        // console.log(`🔑 Password TTL refreshed to ${days} days, ${hours} hours`);
      }
      return true;
    } else {
      // console.log('⚠️ Redis key exists but value is empty');
      return false;
    }
  } catch (error) {
    errorLog('Crypto', 'Failed to recover password from Redis', error);
    return false;
  }
}

export async function setPassword(password: string): Promise<void> {
  PASSWORD = password;
  // Clear the cache when password changes
  decryptedKeyCache.clear();

  // Development only: persist to Redis for convenience
  if (process.env.NODE_ENV !== 'production') {
    try {
      const redis = getRedisConnection();
      if (PASSWORD_TTL > 0) {
        await redis.setex(PASSWORD_REDIS_KEY, PASSWORD_TTL, password);
      } else {
        await redis.set(PASSWORD_REDIS_KEY, password);
      }
    } catch {
      warnLog('Crypto', '[DEV] Failed to store password in Redis - using memory only');
    }
  }
  // Production: password only stored in memory (no persistence)
}

export function getPassword(): string | undefined {
  return PASSWORD;
}

// Renew password TTL without changing the password (development only)
export async function renewPasswordTTL(): Promise<void> {
  if (!PASSWORD || PASSWORD_TTL <= 0 || process.env.NODE_ENV === 'production') return;

  try {
    const redis = getRedisConnection();
    const exists = await redis.exists(PASSWORD_REDIS_KEY);
    if (exists) {
      await redis.expire(PASSWORD_REDIS_KEY, PASSWORD_TTL);
      // const days = Math.floor(PASSWORD_TTL / 86400);
      // const hours = Math.floor((PASSWORD_TTL % 86400) / 3600);
      // console.log(`🔑 [DEVELOPMENT] Password TTL renewed for ${days} days, ${hours} hours`);
    }
  } catch {
    warnLog('Crypto', '[DEV] Failed to renew password TTL');
  }
}

// Clear password from all storage
export async function clearPassword(): Promise<void> {
  PASSWORD = undefined;
  decryptedKeyCache.clear();

  // Development only: clear from Redis
  if (process.env.NODE_ENV !== 'production') {
    try {
      const redis = getRedisConnection();
      await redis.del(PASSWORD_REDIS_KEY);
    } catch {
      warnLog('Crypto', '[DEV] Failed to clear password from Redis - cleared from memory only');
    }
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
  // console.log('🔐 [decryptPrivateKey] Starting decryption...');

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

  // console.log(`🔐 [decryptPrivateKey] Input validation passed, data length: ${encryptedData.length}`);

  // Check if password is available
  // const hasPassword = !!getPassword();
  // console.log(`🔐 [decryptPrivateKey] Password available: ${hasPassword}`);

  // Check cache first
  const cached = decryptedKeyCache.get(encryptedData);
  if (cached) {
    const age = Date.now() - cached.timestamp;
    // console.log(`🔐 [decryptPrivateKey] Cache hit - age: ${age}ms, TTL: ${CACHE_TTL}ms`);
    if (age < CACHE_TTL) {
      // Cache hit - return cached value
      // console.log('🔐 [decryptPrivateKey] Returning cached key');
      return cached.key;
    } else {
      // Cache expired - remove it
      // console.log('🔐 [decryptPrivateKey] Cache expired, removing...');
      decryptedKeyCache.delete(encryptedData);
    }
  } else {
    // console.log('🔐 [decryptPrivateKey] Cache miss');
  }

  // Cache miss or expired - perform decryption
  // console.log('🔐 [decryptPrivateKey] Getting encryption key...');
  let key: Buffer;
  try {
    key = getEncryptionKey();
    // console.log('🔐 [decryptPrivateKey] Encryption key obtained successfully');
  } catch (error) {
    errorLog('Crypto', 'decryptPrivateKey: Failed to get encryption key', error);
    throw error;
  }

  const parts = encryptedData.split(':');
  // console.log(`🔐 [decryptPrivateKey] Encrypted data parts: ${parts.length}`);

  if (parts.length !== 3) {
    errorLog('Crypto', `decryptPrivateKey: Invalid format | expected=3 parts | got=${parts.length}`);
    throw new Error('Invalid encrypted data format');
  }

  const [ivHex, authTagHex, encrypted] = parts;
  // console.log(`🔐 [decryptPrivateKey] IV length: ${ivHex.length}, AuthTag length: ${authTagHex.length}, Encrypted length: ${encrypted.length}`);

  try {
    const iv = Buffer.from(ivHex, 'hex');
    const authTag = Buffer.from(authTagHex, 'hex');
    const decipher = crypto.createDecipheriv(ALGORITHM, key, iv);

    decipher.setAuthTag(authTag);

    let decrypted = decipher.update(encrypted, 'hex', 'utf8');
    decrypted += decipher.final('utf8');

    // console.log('🔐 [decryptPrivateKey] Decryption successful');

    // Cache the result
    cacheDecryptedKey(encryptedData, decrypted);
    // console.log(`🔐 [decryptPrivateKey] Result cached. Cache size: ${decryptedKeyCache.size}/${MAX_CACHE_SIZE}`);

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

// Clear the decrypted key cache (useful when changing passwords)
export function clearDecryptedKeyCache(): void {
  // const previousSize = decryptedKeyCache.size;
  decryptedKeyCache.clear();
  // console.log(`🔐 [clearDecryptedKeyCache] Cleared ${previousSize} cached keys`);
}

// Helper function to manage cache size and add new entries
function cacheDecryptedKey(encryptedData: string, decryptedKey: string): void {
  // Check cache size and remove oldest entries if needed
  if (decryptedKeyCache.size >= MAX_CACHE_SIZE) {
    // Find and remove the oldest entry
    let oldestKey: string | null = null;
    let oldestTime = Date.now();

    for (const [key, value] of decryptedKeyCache.entries()) {
      if (value.timestamp < oldestTime) {
        oldestTime = value.timestamp;
        oldestKey = key;
      }
    }

    if (oldestKey) {
      decryptedKeyCache.delete(oldestKey);
    }
  }

  // Add to cache
  decryptedKeyCache.set(encryptedData, {
    key: decryptedKey,
    timestamp: Date.now()
  });
}

// Function to clear the decryption cache (useful for security or memory management)
export function clearDecryptionCache(): void {
  decryptedKeyCache.clear();
}

// Function to get cache statistics
export function getDecryptionCacheStats(): { size: number; maxSize: number; ttl: number } {
  return {
    size: decryptedKeyCache.size,
    maxSize: MAX_CACHE_SIZE,
    ttl: CACHE_TTL
  };
}

// Password encryption using GCM mode (secure, non-deterministic)
// Format: "gcm:iv:authTag:encrypted" for new GCM encrypted passwords
// Legacy ECB format: just hex string (no colons) - for backward compatibility
export function encryptPassword(password: string): string {
  debugLog('Crypto', 'ENCRYPT_PASSWORD | mode=GCM | ivLength=16');

  const key = getPasswordEncryptionKey();
  const iv = crypto.randomBytes(16);

  // Use GCM mode for secure encryption
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv);

  let encrypted = cipher.update(password, "utf8", "hex");
  encrypted += cipher.final("hex");

  const authTag = cipher.getAuthTag();

  // Prefix with "gcm:" to identify new format
  const result = `gcm:${iv.toString('hex')}:${authTag.toString('hex')}:${encrypted}`;

  debugLog('Crypto', `ENCRYPT_PASSWORD | SUCCESS | format=gcm:iv:authTag:encrypted | outputLength=${result.length}`);
  return result;
}

export function decryptPassword(encryptedPassword: string): string {
  const isGcmFormat = encryptedPassword.startsWith('gcm:');
  debugLog('Crypto', `DECRYPT_PASSWORD | inputLength=${encryptedPassword.length} | format=${isGcmFormat ? 'GCM' : 'LEGACY_ECB'}`);

  const key = getPasswordEncryptionKey();

  try {
    // Check if this is new GCM format (starts with "gcm:")
    if (isGcmFormat) {
      debugLog('Crypto', 'DECRYPT_PASSWORD | mode=GCM | parsing iv:authTag:encrypted');

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

      debugLog('Crypto', 'DECRYPT_PASSWORD | SUCCESS');
      return decrypted;
    }

    // Legacy ECB format (no colons or doesn't start with "gcm:")
    // This provides backward compatibility during migration
    warnLog('Crypto', 'Decrypting password with legacy ECB mode - will be re-encrypted with GCM on next use');
    debugLog('Crypto', 'DECRYPT_PASSWORD | LEGACY_ECB_FALLBACK | migrating to GCM on next use');

    const decipher = crypto.createDecipheriv(DETERMINISTIC_ALGORITHM, key, null);

    let decrypted = decipher.update(encryptedPassword, "hex", "utf8");
    decrypted += decipher.final("utf8");

    debugLog('Crypto', 'DECRYPT_PASSWORD | SUCCESS');
    return decrypted;
  } catch (error) {
    errorLog('Crypto', 'Failed to decrypt password', error);
    throw new Error("Failed to decrypt password");
  }
}