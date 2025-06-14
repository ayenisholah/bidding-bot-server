import crypto from 'crypto';
import { config } from "dotenv"

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

function getEncryptionKey(): Buffer {
  // Only use the in-memory password, no fallback
  if (!PASSWORD) {
    throw new Error('No encryption key available. Please sign in to set the password.');
  }

  // Hash the password to get a consistent 32-byte key  
  return crypto.scryptSync(PASSWORD, "ENCRYPTION_KEY" as string, 32);
}

export function encryptPrivateKey(privateKey: string): string {
  const key = getEncryptionKey();

  console.log({ key });

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