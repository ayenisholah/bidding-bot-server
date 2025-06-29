import crypto from 'crypto';
import { config } from "dotenv"
import { logger } from './logger';

config()

const ALGORITHM = 'aes-256-gcm';

// Import PASSWORD from index.ts
let PASSWORD: string | undefined = process.env.PASSWORD

export function setPassword(password: string) {
  PASSWORD = password;
}

export function getPassword(): string | undefined {
  return PASSWORD;
}

async function getEncryptionKey(): Promise<Buffer> {
  // Only use the in-memory password, no fallback
  if (!PASSWORD) {
    await logger.logSecurityEvent('MISSING_ENCRYPTION_KEY', 'No encryption key available. Please sign in to set the password.', {
      operation: 'get_encryption_key'
    });
    throw new Error('No encryption key available. Please sign in to set the password.');
  }

  // Hash the password to get a consistent 32-byte key  
  return crypto.scryptSync(PASSWORD, "ENCRYPTION_KEY" as string, 32);
}

export async function encryptPrivateKey(privateKey: string): Promise<string> {
  const key = await getEncryptionKey();

  console.log({ key });

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