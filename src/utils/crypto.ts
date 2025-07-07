import crypto from 'crypto';
import { config } from "dotenv"
import { logger } from './logger';

config()

const ALGORITHM = 'aes-256-gcm';
const DETERMINISTIC_ALGORITHM = "aes-256-ecb";

// In development mode, allow password from environment variable
let PASSWORD: string | undefined = process.env.NODE_ENV === 'development' ? process.env.PASSWORD : undefined

export function setPassword(password: string) {
  PASSWORD = password;
}

export function getPassword(): string | undefined {
  return PASSWORD;
}

async function getEncryptionKey(): Promise<Buffer> {
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
  await logger.logSecurityEvent(
    "MISSING_PASSWORD",
    "No password available for encryption",
    {
      operation: "get_encryption_key",
    }
  );
  throw new Error("No password available. Please provide a password for encryption.");

}



// Deterministic password encryption/decryption functions
// Uses a fixed key derived from JWT_SECRET for deterministic encryption
async function getPasswordEncryptionKey(): Promise<Buffer> {
  const secret = process.env.JWT_SECRET || "default-jwt-secret";

  // Create a deterministic key from JWT_SECRET
  return crypto
    .createHash("sha256")
    .update("password-encryption-key")
    .update(secret)
    .digest();
}


export async function encryptPrivateKey(privateKey: string): Promise<string> {
  const key = await getEncryptionKey();
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv);

  let encrypted = cipher.update(privateKey, 'utf8', 'hex');
  encrypted += cipher.final('hex');

  const authTag = cipher.getAuthTag();

  // Combine iv:authTag:encrypted into single string
  return `${iv.toString('hex')}:${authTag.toString('hex')}:${encrypted}`;
}

export async function decryptPrivateKey(encryptedData: string): Promise<string> {
  const key = await getEncryptionKey();
  const parts = encryptedData.split(':');

  if (parts.length !== 3) {
    await logger.logSecurityEvent('INVALID_ENCRYPTED_FORMAT', 'Invalid encrypted data format', {
      operation: 'decrypt_private_key',
      partsLength: parts.length
    });
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

export async function encryptPassword(password: string): Promise<string> {
  const key = await getPasswordEncryptionKey();

  // Use ECB mode for deterministic encryption (same input = same output)
  const cipher = crypto.createCipheriv(DETERMINISTIC_ALGORITHM, key, null);

  let encrypted = cipher.update(password, "utf8", "hex");
  encrypted += cipher.final("hex");

  return encrypted;
}

export async function decryptPassword(encryptedPassword: string): Promise<string> {
  const key = await getPasswordEncryptionKey();

  try {
    const decipher = crypto.createDecipheriv(DETERMINISTIC_ALGORITHM, key, null);

    let decrypted = decipher.update(encryptedPassword, "hex", "utf8");
    decrypted += decipher.final("utf8");

    return decrypted;
  } catch (error) {
    await logger.logSecurityEvent(
      "PASSWORD_DECRYPTION_FAILED",
      "Failed to decrypt password",
      {
        operation: "decrypt_password",
        error: error instanceof Error ? error.message : "Unknown error",
      } as any
    );
    throw new Error("Failed to decrypt password");
  }
}