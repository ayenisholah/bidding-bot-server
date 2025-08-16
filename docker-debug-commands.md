# Docker Debug Commands for NFT Bidding Bot

## Debugging Stuck Task at 99% Issue

These commands help diagnose why tasks get stuck at 99% completion. Replace `689f9a8030d8e039c59e6e5d` with your actual task ID.

### 1. Check for numeric job IDs being skipped
```bash
docker logs --follow -t "nft-bidding-bot-server-1" 2>&1 | grep --line-buffered -E "689f9a8030d8e039c59e6e5d|Numeric job ID.*completed"
```
**Purpose**: Identifies if jobs with numeric IDs (instead of "bid-" prefix) are being skipped by the event handler.

### 2. Check for job creation errors or missing data
```bash
docker logs --follow -t "nft-bidding-bot-server-1" 2>&1 | grep --line-buffered -E "689f9a8030d8e039c59e6e5d" | grep --line-buffered -E "error|Error|undefined|null|Cannot read|JSON\.parse|createJobKey|generateJobKey"
```
**Purpose**: Finds errors during job key creation that might cause jobs to fall back to numeric IDs.

### 3. Check for jobs without proper bid- prefix
```bash
docker logs --follow -t "nft-bidding-bot-server-1" 2>&1 | grep --line-buffered -E "689f9a8030d8e039c59e6e5d|Job completed:" | grep --line-buffered -vE "bid-689f9a8030d8e039c59e6e5d|schedule"
```
**Purpose**: Shows completed jobs that don't have the expected "bid-" prefix format.

### 4. Track job completion events and their IDs
```bash
docker logs --follow -t "nft-bidding-bot-server-1" 2>&1 | grep --line-buffered -E "689f9a8030d8e039c59e6e5d" | grep --line-buffered -E "\[Queue Event\].*completed:|markJobAsProcessed|Could not extract"
```
**Purpose**: Monitors job completion events and whether they're being properly processed.

### 5. Check parent job tracking for discrepancies
```bash
docker logs --follow -t "nft-bidding-bot-server-1" 2>&1 | grep --line-buffered -E "689f9a8030d8e039c59e6e5d" | grep --line-buffered -E "completedJobs|totalJobs|actualJobs|Parent Job|updateTaskRunningPercentage.*loop3"
```
**Purpose**: Tracks parent job counters to identify mismatches between expected and completed jobs.

### 6. Combined diagnostic command (most comprehensive)
```bash
docker logs --follow -t "nft-bidding-bot-server-1" 2>&1 | grep --line-buffered -E "689f9a8030d8e039c59e6e5d" | grep --line-buffered -E "Numeric job ID|Job completed:|Could not extract|error.*Key|undefined.*trait|null.*data|JSON\.parse|actualJobs|completedJobs" | grep --line-buffered -vE "\[OpenSea Counterbid\] Task|Own bid detected.*MAGICEDEN|Own bid detected.*OPENSEA"
```
**Purpose**: Comprehensive check for all potential issues in one command.

### 7. Check for job creation issues specifically
```bash
docker logs --follow -t "nft-bidding-bot-server-1" 2>&1 | grep --line-buffered -E "689f9a8030d8e039c59e6e5d" | grep --line-buffered -E "processBulkJobs|Creating key for job|Generated key|addBulk|jobKey.*undefined"
```
**Purpose**: Focuses on the job creation process to identify where job IDs might be failing to generate.

### 8. Monitor counter lock behavior (NEW)
```bash
docker logs --follow -t "nft-bidding-bot-server-1" 2>&1 | grep --line-buffered -E "689f9a8030d8e039c59e6e5d" | grep --line-buffered -E "Counter incremented|Failed to acquire counter lock|CRITICAL.*lock"
```
**Purpose**: Monitors the new mutex lock implementation to ensure counter increments are atomic.

### 9. Monitor stuck task detection (NEW)
```bash
docker logs --follow -t "nft-bidding-bot-server-1" 2>&1 | grep --line-buffered -E "Stuck Task|stuck at 99|Forcing completion"
```
**Purpose**: Monitors the stuck task detector that forces completion when tasks are stuck at 99%.

## Root Cause Analysis

### 1. Race Condition in Job Completion Counter (99% Stuck Issue)

The 99% stuck issue occurs due to a **race condition** in the job completion counter:

1. **Race Condition in Counter Increment**: When multiple jobs complete simultaneously (within milliseconds), the `parentJob.completedJobs++` operation is not atomic
2. **Lost Increments**: Some counter increments are lost when multiple threads try to update the same counter at the same time
3. **Completion counter never reaches 100%**: The actual completed jobs count is less than the total, causing the percentage to get stuck at 99%
4. **Task stuck at 99%**: The task can never reach 100% because the counter is off by a few jobs

## Solution Implemented

### 1. Mutex Lock for Counter Updates
- Added a distributed lock around the counter increment operation
- Uses Redis-based locks to ensure only one job can increment the counter at a time
- Includes retry logic if lock acquisition fails

### 2. Enhanced Stuck Task Detection
- Monitors tasks that are stuck at 99% for more than 2 minutes
- Reconciles actual job counts across all marketplaces
- Forces completion if the task is actually ≥99.5% complete

### 3. Debug Logging
- Added detailed logging of counter increments
- Logs show before/after values for debugging race conditions
- Tracks lock acquisition failures

## Usage Tips

1. Always use `--line-buffered` with grep for real-time output
2. Replace the task ID with your actual task ID
3. Use `2>&1` to capture both stdout and stderr
4. Add `-n 1000` before the first pipe to limit historical logs if needed
5. Remove `--follow` to search only existing logs instead of tailing

### 2. Schedule Job Timeout (Stuck QUEUEING Issue)

Tasks can get stuck in QUEUEING status when schedule jobs timeout:

1. **Worker Timeout**: Schedule jobs have a 15-minute timeout at the worker level
2. **Job Creation Failure**: If `processBulkJobs` takes too long, the worker aborts the job
3. **Stuck State**: Task remains in QUEUEING with empty parent jobs (0 totalJobs)
4. **No Recovery**: Without intervention, the task stays stuck indefinitely

### 3. Partial Job Queueing (Stuck at Any Percentage)

Tasks can get stuck at any percentage when not all expected jobs are queued:

1. **Queue Capacity**: When queue is at capacity, only partial jobs get queued
2. **Stuck Progress**: Task can't reach 100% if actualJobs < expectedJobs
3. **Example**: 9950 jobs queued out of 10000 expected = max 99.5% completion
4. **Any Percentage**: Can happen at 45%, 98.5%, or any other percentage

## Solution Implemented

### 1. Mutex Lock for Counter Updates
- Added a distributed lock around the counter increment operation
- Uses Redis-based locks to ensure only one job can increment the counter at a time
- Includes retry logic if lock acquisition fails

### 2. Enhanced Stuck Task Detection
- Monitors tasks that are stuck at 99% for more than 2 minutes
- Reconciles actual job counts across all marketplaces
- Forces completion if the task is actually ≥99.5% complete

### 3. Schedule Job Timeout Recovery (NEW)
- Automatically detects when schedule jobs timeout
- Extracts taskId from failed jobId and restarts the entire task
- Uses distributed lock to prevent multiple restarts
- Clears stuck parent jobs and preprocessing data
- Logs all restarts for monitoring

### 4. Partial Queueing Completion (NEW)
- Detects when actualJobs < expectedJobs due to queue capacity
- Triggers completion when all actual jobs are done, even at <100%
- Shows warning logs for partial queueing situations
- Ensures tasks don't get stuck at any percentage

### 5. Debug Logging
- Added detailed logging of counter increments
- Logs show before/after values for debugging race conditions
- Tracks lock acquisition failures
- Monitors tasks stuck in QUEUEING status
- Shows partial queueing warnings with actual vs expected jobs

## Quick One-Liner to Find Stuck Tasks

```bash
# Find all tasks with high completion percentage but still running
docker logs -t "nft-bidding-bot-server-1" 2>&1 | grep "updateTaskRunningPercentage" | grep -E "9[5-9]\.[0-9]+%|99\.[0-9]+%" | tail -20

# Find tasks stuck in QUEUEING status
docker logs -t "nft-bidding-bot-server-1" 2>&1 | grep -E "QUEUEING|stuck in QUEUEING|Schedule job timeout" | tail -20

# Monitor task restarts due to timeouts
docker logs -t "nft-bidding-bot-server-1" 2>&1 | grep -E "Schedule job timeout detected|Restarting task.*after timeout" | tail -20

# Monitor partial completion (tasks completing at less than 100%)
docker logs -t "nft-bidding-bot-server-1" 2>&1 | grep -E "Partial queueing detected|Partial Completion" | tail -20
```