import crypto from 'crypto';
import { config } from "dotenv"

config()

const ALGORITHM = 'aes-256-gcm';
const DETERMINISTIC_ALGORITHM = "aes-256-ecb";

// In development mode, allow password from environment variable
let PASSWORD: string | undefined = undefined

// Initialize password from environment variable in development
if (process.env.NODE_ENV === 'development' && process.env.DEV_PASSWORD) {
  PASSWORD = process.env.DEV_PASSWORD;
  console.log('🔑 Password loaded from DEV_PASSWORD environment variable');
}

export function setPassword(password: string) {
  PASSWORD = password;
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

  return decrypted;
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