import crypto from 'crypto';
import { config } from "dotenv"

config()

const ALGORITHM = 'aes-256-gcm';
const DETERMINISTIC_ALGORITHM = "aes-256-ecb";

// Cache for decrypted private keys
const decryptedKeyCache = new Map<string, { key: string; timestamp: number }>();
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes cache TTL
const MAX_CACHE_SIZE = 100; // Maximum number of cached keys

// In development mode, allow password from environment variable
let PASSWORD: string | undefined = undefined

// Initialize password from environment variable in development
if (process.env.NODE_ENV === 'development' && process.env.DEV_PASSWORD) {
  PASSWORD = process.env.DEV_PASSWORD;
  console.log('🔑 Password loaded from DEV_PASSWORD environment variable');
}

export function setPassword(password: string) {
  PASSWORD = password;
  // Clear the cache when password changes
  decryptedKeyCache.clear();
  console.log('🔑 Password has been set in memory');
}

export function getPassword(): string | undefined {
  return PASSWORD;
}

function getEncryptionKey(): Buffer {
  let password = PASSWORD; // Current request

  // If we have a password, use it for encryption
  if (password) {
    const salt = crypto
      .createHash("sha256")
      .update("NFT_BIDDING_BOT_USER_KEY")
      .update(process.env.JWT_SECRET || "default")
      .digest();

    return crypto.scryptSync(password, salt as any, 32);
  }

  // No password available - throw error
  throw new Error("No password available. Please provide a password for encryption.");

}

// Deterministic password encryption/decryption functions
// Uses a fixed key derived from JWT_SECRET for deterministic encryption
function getPasswordEncryptionKey(): Buffer {
  const secret = process.env.JWT_SECRET || "default-jwt-secret";

  // Create a deterministic key from JWT_SECRET
  return crypto
    .createHash("sha256")
    .update("password-encryption-key")
    .update(secret)
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
  // Check cache first
  const cached = decryptedKeyCache.get(encryptedData);
  if (cached) {
    const age = Date.now() - cached.timestamp;
    if (age < CACHE_TTL) {
      // Cache hit - return cached value
      return cached.key;
    } else {
      // Cache expired - remove it
      decryptedKeyCache.delete(encryptedData);
    }
  }

  // Cache miss or expired - perform decryption
  const key = getEncryptionKey();
  const parts = encryptedData.split(':');

  if (parts.length !== 3) {
    throw new Error('Invalid encrypted data format');
  }

  const [ivHex, authTagHex, encrypted] = parts;

  const iv = Buffer.from(ivHex, 'hex');
  const authTag = Buffer.from(authTagHex, 'hex');
  const decipher = crypto.createDecipheriv(ALGORITHM, key, iv);

  decipher.setAuthTag(authTag);

  let decrypted = decipher.update(encrypted, 'hex', 'utf8');
  decrypted += decipher.final('utf8');

  // Cache the result
  cacheDecryptedKey(encryptedData, decrypted);

  return decrypted;
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

export function encryptPassword(password: string): string {
  const key = getPasswordEncryptionKey();

  // Use ECB mode for deterministic encryption (same input = same output)
  const cipher = crypto.createCipheriv(DETERMINISTIC_ALGORITHM, key, null);

  let encrypted = cipher.update(password, "utf8", "hex");
  encrypted += cipher.final("hex");

  return encrypted;
}

export function decryptPassword(encryptedPassword: string): string {
  const key = getPasswordEncryptionKey();

  try {
    const decipher = crypto.createDecipheriv(DETERMINISTIC_ALGORITHM, key, null);

    let decrypted = decipher.update(encryptedPassword, "hex", "utf8");
    decrypted += decipher.final("utf8");

    return decrypted;
  } catch {
    throw new Error("Failed to decrypt password");
  }
}