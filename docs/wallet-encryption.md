# Wallet Encryption and Decryption Strategy

## Overview

The NFT Bidding Bot uses AES-256-GCM encryption to securely store wallet private keys. This ensures that sensitive wallet data is protected at rest while still allowing the bot to access private keys when needed for signing transactions.

## Encryption Details

### Algorithm
- **Cipher**: AES-256-GCM (Galois/Counter Mode)
- **Key Size**: 256 bits (32 bytes)
- **IV Size**: 128 bits (16 bytes)
- **Authentication Tag**: Included for integrity verification

### Key Management
- Encryption key is stored in the `ENCRYPTION_KEY` environment variable
- Key must be exactly 32 bytes (64 hex characters)
- The same key is used across all wallet encryptions in the system

### Encrypted Data Format
Encrypted private keys are stored as a colon-separated string:
```
<iv>:<authTag>:<encryptedData>
```
- `iv`: Initialization vector (hex encoded)
- `authTag`: Authentication tag for GCM mode (hex encoded)
- `encryptedData`: The encrypted private key (hex encoded)

## Implementation

### Encryption Process (`encryptPrivateKey`)
1. Generate a random 16-byte initialization vector (IV)
2. Create cipher using AES-256-GCM with the encryption key and IV
3. Encrypt the private key
4. Extract the authentication tag
5. Combine IV, auth tag, and encrypted data into a single string

### Decryption Process (`decryptPrivateKey`)
1. Split the encrypted string into its three components
2. Convert hex-encoded parts back to buffers
3. Create decipher with the same algorithm, key, and IV
4. Set the authentication tag for verification
5. Decrypt and return the original private key

## Usage in the System

### Wallet Model
- Wallets store private keys in the `encryptedPrivateKey` field
- Private keys are encrypted before storage in MongoDB
- The encryption happens at the application level, not database level

### Task Processing
- When a task needs to sign transactions, it retrieves the wallet's encrypted private key
- The private key is decrypted in memory only when needed
- Decrypted keys are never logged or persisted

## Security Considerations

1. **Environment Variable**: The `ENCRYPTION_KEY` must be kept secure and never committed to version control
2. **Key Rotation**: If key rotation is needed, all existing encrypted private keys must be re-encrypted
3. **Memory Safety**: Decrypted private keys exist in memory only during transaction signing
4. **Authentication**: GCM mode provides both confidentiality and authenticity, preventing tampering

## Error Handling

The encryption utilities include error handling for:
- Missing encryption key environment variable
- Invalid key length
- Corrupted encrypted data format
- Authentication tag verification failures

## Example Usage

```javascript
// Encrypting a private key
const encryptedKey = encryptPrivateKey(privateKey);
// Result: "a1b2c3...:<authTag>:<encryptedData>"

// Decrypting a private key
const privateKey = decryptPrivateKey(encryptedKey);
// Result: Original private key string
```