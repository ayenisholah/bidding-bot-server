# Password-Based Encryption System for NFT Bidding Bot

## Executive Summary

The NFT Bidding Bot implements a sophisticated password-based encryption system that prioritizes security through ephemeral key management. Unlike traditional approaches that rely on environment variables or configuration files, this system derives encryption keys dynamically from user passwords, ensuring that sensitive wallet data remains protected even in the event of server compromise. The system enforces a zero-persistence policy on the server side, requiring re-authentication after any server restart, thereby creating a robust security boundary around wallet operations.

## System Architecture Overview

### Dual-Layer Password Management

The system employs a carefully designed dual-layer approach to password management that separates concerns between the client and server components:

**Client-Side Password Storage**

- Passwords are hashed using bcrypt with a salt factor of 10 before storage in MongoDB
- The hashed password serves purely for authentication purposes
- Original passwords are never stored in any persistent storage on the client side
- This approach follows industry best practices for password storage and verification

**Server-Side Password Handling**

- Raw passwords are temporarily stored in server memory using a module-scoped variable
- The password exists only in the Node.js process memory space
- No disk writes, log entries, or persistent storage of any kind
- Memory-only storage ensures the password is automatically cleared on process termination

### Encryption Key Derivation

The system uses a sophisticated key derivation process to transform user passwords into encryption keys:

1. **Key Derivation Function**: Utilizes Node.js's native `crypto.scryptSync()` function
2. **Salt**: Uses a consistent salt value ENCRYPTION_KEY for deterministic key generation
3. **Output**: Generates a 32-byte (256-bit) key suitable for AES-256-GCM encryption
4. **Deterministic**: Same password always produces the same encryption key

This approach ensures that wallet private keys can be reliably encrypted and decrypted as long as the correct password is available in memory.

### Encryption Algorithm

The system employs AES-256-GCM (Galois/Counter Mode) for encrypting wallet private keys:

- **Algorithm**: AES-256-GCM provides both confidentiality and authenticity
- **IV Generation**: Random 16-byte initialization vectors for each encryption operation
- **Auth Tag**: GCM mode provides built-in authentication tags to detect tampering
- **Storage Format**: Encrypted data stored as `iv:authTag:encryptedData` for easy parsing

## Server Restart Behavior and Impact

### The Restart Sequence

When the server undergoes a restart (whether planned or unplanned), a specific sequence of events occurs:

1. **Process Termination**

   - Node.js process ends, clearing all memory
   - Password variable becomes undefined
   - All in-memory state is lost

2. **Server Initialization**

   - Server starts with no password in memory
   - MongoDB and Redis connections are established
   - Express server begins accepting requests

3. **Security Checks**
   - Password existence check during startup
   - If no password found, queue and worker processes are paused
   - Server enters a "locked" state awaiting authentication

### Queue and Worker Management

The bidding bot relies on BullMQ for task queue management. The password system directly controls these components:

**When Password is Absent:**

- `queue.pause()` is called to prevent new jobs from being processed
- `workers.pause()` stops all worker threads from picking up jobs
- Existing jobs remain in Redis but won't be processed
- WebSocket connections remain active but limited

**When Password is Set:**

- `queue.resume()` allows job processing to continue
- `workers.resume()` reactivates worker threads
- Queued jobs begin processing immediately
- Full system functionality is restored

### Client-Side Impact

The client implements automatic detection and response to server password state:

1. **PasswordCheck Component**

   - Mounted in dashboard layouts
   - Polls `/api/auth/password-status` endpoint
   - Detects when server lacks password

2. **Automatic Redirect**

   - If no password detected, initiates signout
   - Clears authentication cookies
   - Redirects user to signin page
   - Provides seamless re-authentication flow

3. **Error Handling**
   - Wallet operations return 401 status with `requiresAuth: true`
   - Client can distinguish between auth errors and other failures
   - Appropriate error messages guide users to sign in

## Authentication and Recovery Flow

### Initial Authentication

The authentication process involves multiple steps across client and server:

1. **User Login**

   - User enters password on signin page
   - Password validated against bcrypt hash in MongoDB
   - JWT token generated and set as httpOnly cookie

2. **Password Transmission**

   - After successful authentication, password sent to server
   - Uses dedicated `/api/auth/password` endpoint
   - Transmitted over HTTPS in production environments

3. **Server Activation**
   - Server stores password in memory
   - Encryption key derivation becomes possible
   - Queue and workers resume if previously paused

### Continuous Operation

Once authenticated, the system operates seamlessly:

- All wallet encryption/decryption operations function normally
- Bidding tasks process according to configured rules
- WebSocket connections provide real-time updates
- No additional authentication required until restart

### Recovery After Restart

The recovery process is designed to be user-friendly while maintaining security:

1. **Automatic Detection**

   - Client detects 401 errors or password status checks
   - User presented with clear messaging about re-authentication need

2. **Streamlined Re-authentication**

   - User directed to signin page
   - Same credentials work immediately
   - No need to reset or change passwords

3. **State Restoration**
   - Upon signin, password transmitted to server
   - All operations resume automatically
   - Minimal disruption to user workflow

## Security Considerations and Benefits

### Advantages Over Traditional Approaches

**No Persistent Keys**

- Traditional systems often store encryption keys in environment variables
- These can be exposed through configuration management systems
- Our approach ensures keys exist only when actively needed

**Forced Re-authentication**

- Server restarts create natural security checkpoints
- Requires human intervention to resume operations
- Prevents automated attacks from persisting across restarts

**Reduced Attack Surface**

- No configuration files with sensitive data
- No environment variables to leak
- Memory-only storage limits exposure window

### Threat Mitigation

The system addresses several security threats:

1. **Configuration Exposure**

   - No sensitive data in configuration files
   - Environment variables contain only non-sensitive settings
   - Eliminates risk from misconfigured deployment systems

2. **Memory Dumps**

   - Password exists only during active sessions
   - Server restarts clear any potential memory artifacts
   - Limited window for memory-based attacks

3. **Unauthorized Access**
   - Wallet operations fail without valid password
   - Queue suspension prevents automated bidding
   - Clear audit trail through authentication logs

### Compliance and Best Practices

The implementation follows several security best practices:

- **OWASP Guidelines**: Proper password hashing with bcrypt
- **Principle of Least Persistence**: Minimal data retention
- **Defense in Depth**: Multiple security layers
- **Fail-Secure**: System fails to a secure state (paused) when password absent

## Implementation Details

### Server-Side Components

**Password Management Module** (`/server/src/utils/crypto.ts`)

```typescript
- setPassword(password: string): Stores password in module scope
- getPassword(): Returns current password or undefined
- getEncryptionKey(): Derives key from password using scrypt
```

**Queue Control** (`/server/src/index.ts`)

```typescript
- pauseQueueAndWorkers(): Suspends all processing
- resumeQueueAndWorkers(): Restores normal operation
- Integrated into server startup and password endpoints
```

**API Endpoints**

- `POST /api/auth/password`: Receives and stores password
- `GET /api/auth/password-status`: Returns password availability
- Enhanced error responses for wallet operations

### Client-Side Components

**PasswordCheck Component**

- React component using hooks
- Monitors server password state
- Handles automatic redirects

**Authentication Routes**

- Modified signin/signup to transmit passwords
- Error handling for failed password transmission
- Graceful fallback behaviors

## Operational Considerations

### Monitoring and Maintenance

System administrators should be aware of:

1. **Restart Planning**: Coordinate restarts during low-activity periods
2. **Monitoring**: Watch for increased 401 errors indicating password issues
3. **User Communication**: Notify users before planned restarts

### Performance Impact

The password-based system has minimal performance overhead:

- Key derivation is computationally efficient
- Memory usage negligible (single string variable)
- No additional database queries or network calls

### Scaling Considerations

For multi-instance deployments:

- Each instance maintains its own password state
- Load balancers should use sticky sessions
- Consider shared session management for larger deployments

## Conclusion

The password-based encryption system represents a thoughtful approach to securing sensitive wallet data in the NFT Bidding Bot. By eschewing traditional persistent key storage in favor of ephemeral, password-derived keys, the system achieves a superior security posture while maintaining operational simplicity. The automatic queue suspension and forced re-authentication after restarts create natural security boundaries that protect against both accidental exposure and malicious attacks. This design demonstrates that enhanced security need not come at the cost of usability, providing a seamless experience for authorized users while maintaining robust protection against unauthorized access.
