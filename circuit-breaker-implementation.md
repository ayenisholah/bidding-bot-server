# Circuit Breaker Implementation Summary

## Overview
Implemented a comprehensive circuit breaker pattern with job recovery mechanisms to prevent system deadlock when external services fail.

## Key Components

### 1. Enhanced Circuit Breaker (`/src/utils/enhanced-circuit-breaker.ts`)
- **Three States**: CLOSED (normal), OPEN (blocking), HALF_OPEN (testing)
- **Configurable Thresholds**: 
  - Failure threshold: 3 failures to open
  - Success threshold: 2 successes to close
  - Error rate threshold: 40% to open
- **Exponential Backoff**: Increases retry delays on repeated failures
- **Automatic Cleanup**: Removes stale circuit states after 5 minutes
- **Event-Driven**: Emits events for state changes

### 2. Job Recovery Manager (`/src/utils/job-recovery.ts`)
- **Stale Job Detection**: Identifies jobs stuck in waiting state > 5 minutes
- **Dead Letter Queue**: Moves permanently failed jobs to separate queue
- **Automatic Recovery**: Re-queues stale jobs with delay
- **Periodic Monitoring**: Checks every minute for stuck jobs

### 3. API Endpoints (`/src/routes/circuit-breaker.ts`)
- `GET /api/circuit-breaker/status` - View all circuit states
- `GET /api/circuit-breaker/status/:key` - View specific circuit
- `POST /api/circuit-breaker/close/:key` - Force close circuit
- `POST /api/circuit-breaker/reset-all` - Reset all circuits
- `POST /api/jobs/recover-stuck` - Manually recover stuck jobs

### 4. Health Monitoring
- Circuit breaker stats added to `/health` endpoint
- Shows open/closed/half-open counts
- Integrated with existing health metrics

## Configuration

### Circuit Breaker Settings
```javascript
{
  failureThreshold: 3,         // Failures before opening
  successThreshold: 2,         // Successes to close
  timeout: 30000,             // 30s initial timeout
  resetTimeout: 300000,       // 5min to clear state
  halfOpenRequests: 2,        // Tests in half-open
  errorThresholdPercentage: 40, // Error rate to open
  exponentialBackoffFactor: 1.5,
  maxBackoffTime: 300000      // 5min max backoff
}
```

### Job Recovery Settings
```javascript
{
  maxRetries: 3,              // Max retry attempts
  staleJobThreshold: 300000,  // 5min stale threshold
  deadLetterQueueName: 'BIDDING_BOT-dead-letter',
  checkInterval: 60000        // 1min check interval
}
```

## Usage

### Monitor Circuit Status
```bash
curl http://localhost:3003/api/circuit-breaker/status
```

### Reset All Circuits (Emergency)
```bash
curl -X POST http://localhost:3003/api/circuit-breaker/reset-all
```

### Recover Stuck Jobs
```bash
curl -X POST http://localhost:3003/api/jobs/recover-stuck \
  -H "Content-Type: application/json" \
  -d '{"limit": 100}'
```

## Debugging Features

### Loop Counter
- Tracks how many times each task has looped
- Visible in logs: "Scheduling task X to run again (Loop #N)"
- Available via API: `GET /api/task-status/:taskId`

### Event Logging
- Circuit state changes logged with colors
- Failed job recovery attempts logged
- Stale job detection logged periodically

## Benefits

1. **Prevents Deadlock**: Circuit breakers prevent infinite failure loops
2. **Auto-Recovery**: System automatically recovers from transient failures
3. **Manual Control**: Operators can force reset circuits when needed
4. **Better Visibility**: Clear logging and monitoring of circuit states
5. **Job Recovery**: Stuck jobs are automatically detected and retried
6. **Dead Letter Queue**: Failed jobs preserved for analysis

## Migration Notes

- Old `CircuitBreaker` class replaced with `EnhancedCircuitBreaker`
- All references updated to use `enhancedCounterbidCircuitBreaker`
- Event listeners added for state change notifications
- Cleanup added to shutdown handlers