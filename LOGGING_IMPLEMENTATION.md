# Comprehensive Logging Implementation

This document summarizes the robust error and info debug logging that has been added throughout the NFT Bidding Bot server project using the existing `@src/utils/logger.ts`.

## Key Implementation Details

- **Only ERROR logs are sent to Axiom** (configured in the logger to exclude certain categories)
- **INFO and DEBUG logs** are kept locally for debugging
- **Comprehensive context** is included with all logs for better debugging

## Areas Where Logging Has Been Added

### 1. Server Initialization and Startup
- MongoDB connection establishment/failures
- Server startup with system information
- WebSocket connection monitoring initialization
- Listing scheduler initialization
- Expiration monitor initialization
- Memory usage monitoring (every 60 seconds)

### 2. Queue Operations
- **Job Lifecycle**: Added, Started, Completed, Failed, Retry
- Each job includes context: taskId, marketplace, duration, error details
- Queue worker initialization and processing

### 3. Marketplace Operations

#### OpenSea
- Bid placement success/failure
- Insufficient balance errors
- Task not active/not found
- Order building failures

#### Blur
- Access token failures
- Insufficient BETH balance
- Bid below minimum price
- Request cancellations

#### Magic Eden
- Bid placement success/failure
- Insufficient balance errors
- Task not found
- Signature generation failures
- Order submission errors

### 4. WebSocket Operations
- Connection established
- Disconnection with duration metrics
- Message handling errors
- Reconnection attempts
- Max retries exceeded

### 5. Database Operations
- Query execution with timing
- Slow query detection (>1000ms)
- Connection errors
- Operation types tracked

### 6. API Endpoints
- Request/response logging with timing
- Task cancellation tracking
- Error responses with context

### 7. Performance Monitoring
- Memory usage warnings (>90% heap)
- Periodic memory metrics (every 5 minutes)
- Slow operation detection
- Queue metrics

## Log Categories and Methods Used

### Error Categories
- `WEBSOCKET` - WebSocket connection issues
- `MARKETPLACE` - Bidding/listing operations
- `DATABASE` - Database operations
- `QUEUE` - Job queue operations
- `API` - API endpoint operations
- `GENERAL` - General application operations
- `PERFORMANCE` - Performance issues

### Marketplace Subcategories
- `BIDDING` - Bid placement operations
- `LISTING` - NFT listing operations
- `CANCELLATION` - Order cancellations
- `COLLECTION` - Collection data operations
- `BALANCE` - Balance checks

### Specific Logger Methods
- `logOpenseaBidError`, `logBlurBidError`, `logMagicEdenBidError`
- `logBidPlaced`, `logBidCancelled`
- `logQueueJobAdded`, `logQueueJobStarted`, `logQueueJobCompleted`, `logQueueJobFailed`
- `logWebSocketError`, `logDatabaseError`, `logApiError`
- `logMemoryWarning`, `logSlowOperation`
- `logInfo`, `logDebug` for non-error tracking

## Best Practices Implemented

1. **Consistent Context**: All logs include relevant identifiers (taskId, userId, marketplace)
2. **Timing Information**: Operations include startTime, endTime, duration
3. **Error Details**: Full error objects with codes and messages
4. **Sensitive Data Protection**: Credentials are masked in logs
5. **Performance Tracking**: Slow queries and operations are flagged

## Monitoring and Debugging

With this logging implementation:
- **Errors** are automatically sent to Axiom for centralized monitoring
- **Info/Debug logs** help trace application flow locally
- **Performance issues** can be identified through slow operation logs
- **Memory leaks** can be detected through periodic memory monitoring
- **Queue health** can be tracked through job lifecycle logs