# Recovery Functions Documentation

> **Last Updated**: 2026-01-08
> **File**: `bidding-bot-server/src/index.ts` (unless otherwise noted)

This document provides comprehensive documentation of all recovery and watchdog functions in the bidding-bot-server that ensure task reliability and automatic error recovery.

---

## Table of Contents

1. [Overview](#overview)
2. [Recovery Function Summary](#recovery-function-summary)
3. [Detailed Function Documentation](#detailed-function-documentation)
   - [Timer Recovery Functions](#1-timer-recovery-functions)
   - [Queueing Recovery Functions](#2-queueing-recovery-functions)
   - [Job Recovery Functions](#3-job-recovery-functions)
   - [Task Health Monitoring](#4-task-health-monitoring)
   - [Fallback Recovery](#5-fallback-recovery)
4. [Interval Configuration](#interval-configuration)
5. [Recovery Flow Diagrams](#recovery-flow-diagrams)
6. [Log Prefixes](#log-prefixes)

---

## Overview

The bidding-bot-server implements a multi-layered recovery system to handle various failure scenarios:

| Layer | Purpose | Functions |
|-------|---------|-----------|
| **Timer Layer** | Ensures scheduled tasks have valid timers | `verifyScheduledTaskTimers()`, `recoverStuckTaskTimer()` |
| **Queueing Layer** | Recovers tasks stuck during job creation | `queueingTaskWatchdog()`, `recoverPartialQueueingTasks()` |
| **Job Layer** | Handles stuck/orphaned BullMQ jobs | `cleanupStuckJobs()`, `recoverOrphanedJobs()`, `recoverStuckJobs()` |
| **Task Layer** | Monitors overall task health | `checkStuckTasks()`, `healthCheckStuckTasks()`, `recoverStuckTask()` |
| **Schedule Layer** | Backup for in-memory timer failures | `taskScheduleWatchdog()` |

---

## Recovery Function Summary

| Function | Location | Interval | Timeout/Threshold | Problem Solved |
|----------|----------|----------|-------------------|----------------|
| `verifyScheduledTaskTimers()` | Line ~6618 | 120s | 5 min no update | SCHEDULED tasks with missing timers |
| `recoverStuckTaskTimer()` | Line ~6570 | On-demand | - | Direct timer recovery |
| `queueingTaskWatchdog()` | Line ~29830 | 60s | 5-30 min dynamic | Tasks stuck in QUEUEING state |
| `recoverPartialQueueingTasks()` | Line ~29507 | 60s | - | Partially queued tasks with 0 jobs |
| `checkStuckTasks()` | Line ~30013 | 60s | 30s-2min | 99% stuck, all-complete stuck, SCHEDULED stuck |
| `healthCheckStuckTasks()` | Line ~30365 | 30s | 1 min at 0% | Tasks at 0% with parent jobs but no actual jobs |
| `recoverStuckTask()` | Line ~30436 | On-demand | - | Creates missing jobs for stuck task |
| `cleanupStuckJobs()` | Line ~6745 | 60s | 60s stuck | Jobs stuck in delayed/waiting state |
| `taskScheduleWatchdog()` | Line ~29642 | 5s | 3s grace | MongoDB-backed timer recovery |
| `recoverOrphanedJobs()` | Line ~31092 | Disabled | - | Jobs without parent job tracking |
| `verifyJobCreation()` | Line ~30556 | 15s | 30s timeout | Job creation verification |
| `recoverStuckJobs()` | redis-job-fallback.ts:220 | On-demand | - | Fallback store job recovery |

---

## Detailed Function Documentation

### 1. Timer Recovery Functions

#### `recoverStuckTaskTimer(taskId, marketplace)`
**Location**: Line ~6570
**Trigger**: Called by `verifyScheduledTaskTimers()` or `checkStuckTasks()`

**Purpose**: Directly recovers a task that has lost its timer by calling `scheduleNextLoop()`.

**Guards/Checks**:
1. ✅ Skips if task is actively queueing (`isTaskInActiveQueueing()`)
2. ✅ Skips if timer already exists (`taskTimeouts.has()`)
3. ✅ Skips if task not active or not running
4. ✅ Skips if task is blacklisted

**Recovery Action**:
```
SCHEDULED (no timer) → scheduleNextLoop() → SCHEDULED (with timer)
```

**Log Prefix**: `TimerRecovery`, `TimerHealth`

---

#### `verifyScheduledTaskTimers()`
**Location**: Line ~6618
**Trigger**: `setInterval()` every 120 seconds (`TIMER_HEALTH_CHECK_INTERVAL`)

**Purpose**: Periodic health check that verifies ALL SCHEDULED tasks have valid timers and recovers those that don't.

**What It Recovers**:
1. **Missing Timer for SCHEDULED Task**: Task is SCHEDULED but `taskTimeouts` has no entry
2. **Stale Tasks (Defensive)**: Any running task without timer for >5 minutes (regardless of status)

**Recovery Flow**:
```
1. Scan all activeTasks with SCHEDULED status
2. Check if timer exists for each marketplace
3. If no timer:
   a. Try recoverStuckTaskTimer()
   b. If fails, try DB reload (Task.findById)
   c. If task not running in DB, cleanup orphaned status
4. DEFENSIVE: Check ALL running tasks, create timer if missing >5min
```

**Diagnostic Logging**:
- `TimerDiagnostic` - Logs timer map state
- `STUCK_TASKS_FOUND` - Lists tasks with waitingTime=0 and no timer

---

### 2. Queueing Recovery Functions

#### `queueingTaskWatchdog()`
**Location**: Line ~29830
**Trigger**: `setInterval()` every 60 seconds (`QUEUEING_WATCHDOG_INTERVAL`)

**Purpose**: Recovers tasks stuck in QUEUEING state where job queueing has stalled.

**Dynamic Timeout Calculation**:
```javascript
const timeoutMs = Math.min(
  BASE_TIMEOUT_MS + (totalJobs * PER_JOB_TIMEOUT_MS),  // 5min + 100ms per job
  MAX_TIMEOUT_MS  // Cap at 30 minutes
);
```

| Jobs | Timeout |
|------|---------|
| 0 | 5 min |
| 1,000 | 6 min 40 sec |
| 6,000 | 15 min |
| 15,000+ | 30 min (max) |

**What It Checks**:
1. ✅ Has recent progress in `queueingProgress` map?
2. ✅ Has jobs already queued (`jobsQueued > 0`)?
3. ✅ Has jobs in BullMQ queue (`hasTaskJobsInQueue()`)?
4. ✅ Is `queuedAt` old enough?

**Recovery Action**:
```
QUEUEING [truly stuck] → COMPLETED → scheduleNextLoop() → SCHEDULED
```

**Key Innovation**: Progress-based timeout instead of absolute timeout prevents false positives for large tasks.

---

#### `recoverPartialQueueingTasks()`
**Location**: Line ~29507
**Trigger**: `setInterval()` every 60 seconds (`STANDARD_INTERVAL`)

**Purpose**: Detects and recovers tasks where job queueing started but failed midway, leaving `expectedJobs > actualJobs` with `totalJobs = 0`.

**Detection Criteria**:
- Marketplace status is RUNNING/QUEUED/QUEUEING
- Parent job has `expectedJobs > 0`
- Parent job has `actualJobs < expectedJobs`
- Parent job has `allJobsQueued = false`
- Total jobs across marketplaces = 0

**Recovery Action**:
```
Calls triggerTaskRestart(taskId) to restart the task
```

**Dedup Protection**: Uses `tasksBeingRestarted` Set with 30-second cooldown.

---

### 3. Job Recovery Functions

#### `cleanupStuckJobs()`
**Location**: Line ~6745
**Trigger**: `setInterval()` every 60 seconds (`STUCK_JOB_CLEANUP_INTERVAL`)

**Purpose**: Cleans up BullMQ jobs stuck in `delayed` or `waiting` state for too long.

**Thresholds**:
- `MAX_STUCK_TIME_MS`: 60 seconds
- `MAX_ATTEMPTS`: 3 retries before task restart

**Recovery Actions**:
| Condition | Action |
|-----------|--------|
| Job stuck < 60s | Skip (not yet stuck) |
| Job stuck > 60s, attempts < 3 | Fail job to trigger retry |
| Job stuck > 60s, attempts >= 3 | Remove job + restart task |

**Special Handling**: Catches "Missing lock for job" errors and uses `job.remove()` directly.

---

#### `recoverOrphanedJobs()`
**Location**: Line ~31092
**Trigger**: **DISABLED** (was interval-based)

**Purpose**: Finds jobs in BullMQ queue that have no parent job tracking and creates tracking retroactively.

**Why Disabled**: Can cause "state bleed" where completed jobs are re-counted.

**What It Would Do**:
```
1. Scan jobs in completed/failed/active/waiting states
2. Check if parent job exists for each bid job
3. If no parent job: create one retroactively
4. Update counts based on job state
```

---

#### `recoverStuckJobs()` (Fallback Store)
**Location**: `src/utils/redis-job-fallback.ts` Line ~220
**Trigger**: Called by `checkStuckTasks()` when 99% stuck detected

**Purpose**: Recovers job completion counts from the fallback store when jobs are missing from parent job tracking.

**How It Works**:
```javascript
// For each marketplace with missing jobs:
const missing = expectedCounts[mp] - actualCounts[mp];
const fallbackCount = jobFallbackStore.getByTaskId(taskId).filter(j => j.marketplace === mp).length;
const recoveryCount = Math.min(fallbackCount, missing);
await updateCallback(taskId, mp, recoveryCount);
```

**Fallback Store**: In-memory store with 10-minute TTL that records job completions as backup.

---

### 4. Task Health Monitoring

#### `checkStuckTasks()`
**Location**: Line ~30013
**Trigger**: `setInterval()` every 60 seconds (`STUCK_TASK_CHECK_INTERVAL`)

**Purpose**: Comprehensive stuck task detector handling multiple scenarios.

**Scenarios Handled**:

##### A. QUEUEING Stuck (10 minutes)
```
Detection: status = QUEUEING for > 10 minutes
Action: Log diagnostic with parent job state
```

##### B. 99% Stuck (2 minutes)
```
Detection: runningPercentage >= 99 && < 100 for > 2 minutes
Recovery:
1. Try recoverStuckJobs() from fallback store
2. If actual >= 99%: force all parent jobs complete
3. Force runningPercentage = 100
4. Trigger checkTaskCompletionAndScheduleNext()
```

##### C. All Complete Stuck (30 seconds)
```
Detection: checkAllParentJobsComplete() = true for > 30 seconds
Recovery:
1. Force runningPercentage = 100 for all marketplaces
2. Trigger checkTaskCompletionAndScheduleNext()
```

##### D. SCHEDULED Stuck (120 seconds)
```
Detection: status = SCHEDULED, waitingTime <= 0, no timer, for > 120 seconds
Guard: Skip if any marketplace is actively queueing
Recovery: startTask(task, true, marketplace)
```

---

#### `healthCheckStuckTasks()`
**Location**: Line ~30365
**Trigger**: `setInterval()` every 30 seconds (`TASK_HEALTH_CHECK_INTERVAL`)

**Purpose**: Early detection of tasks stuck at 0% with job creation failures.

**Detection Criteria**:
- Marketplace status = RUNNING
- runningPercentage = 0 for > 1 minute
- Parent job has `expectedJobs > 0`
- Parent job has `actualJobs = 0`

**Recovery Action**: Calls `recoverStuckTask(taskId)`

---

#### `recoverStuckTask(taskId)`
**Location**: Line ~30436
**Trigger**: Called by `healthCheckStuckTasks()`

**Purpose**: Attempts to create missing jobs for a task stuck at 0%.

**Recovery Strategy**:
1. For each marketplace with `expectedJobs > 0` but `actualJobs = 0`:
2. Create a collection bid job as recovery
3. If jobs created: update task percentage
4. If no jobs created: schedule `triggerTaskRestart()` after 5 seconds

---

#### `verifyJobCreation()`
**Location**: Line ~30556
**Trigger**: `setInterval()` every 15 seconds (`JOB_VERIFICATION_INTERVAL`)

**Purpose**: Verifies that jobs were actually created after `processBulkJobs()` was called.

**Tracking**: Uses `jobCreationVerificationMap` to track expected vs actual jobs.

**Detection**: If `actualJobs = 0` after 10 seconds when `expectedJobs > 0`:
- Log error
- Trigger recovery if marketplace still in QUEUEING state

---

### 5. Fallback Recovery

#### `taskScheduleWatchdog()`
**Location**: Line ~29642
**Trigger**: `setInterval()` every 5 seconds (`WATCHDOG_INTERVAL`)

**Purpose**: MongoDB-backed watchdog that recovers tasks whose in-memory timers failed to fire (GC pressure, memory issues).

**How It Works**:
1. Query `TaskSchedule` collection for overdue schedules
2. Grace period: 3 seconds (allow in-memory timer to fire first)
3. Exponential backoff: 5s → 10s → 20s → ... → 5 min max
4. Execute task directly if timer failed

**Key Difference**: Uses MongoDB persistence instead of in-memory state.

---

## Interval Configuration

All intervals are initialized in `startServer()` after MongoDB connection:

```javascript
// Line ~3822
setInterval(recoverPartialQueueingTasks, STANDARD_INTERVAL);           // 60s

// Line ~3937
setInterval(checkStuckTasks, STUCK_TASK_CHECK_INTERVAL);               // 60s

// Line ~3941
setInterval(verifyScheduledTaskTimers, TIMER_HEALTH_CHECK_INTERVAL);   // 120s

// Line ~3945
setInterval(cleanupStuckJobs, STUCK_JOB_CLEANUP_INTERVAL);             // 60s

// Line ~3949
setInterval(taskScheduleWatchdog, WATCHDOG_INTERVAL);                   // 5s

// Line ~3957
setInterval(queueingTaskWatchdog, QUEUEING_WATCHDOG_INTERVAL);         // 60s

// Line ~30546
setInterval(healthCheckStuckTasks, TASK_HEALTH_CHECK_INTERVAL);        // 30s

// Line ~30601
setInterval(verifyJobCreation, JOB_VERIFICATION_INTERVAL);             // 15s
```

---

## Recovery Flow Diagrams

### Timer Recovery Flow
```
verifyScheduledTaskTimers() [every 120s]
         │
         ▼
┌─────────────────────────┐
│ Scan SCHEDULED tasks    │
│ Check timer exists      │
└─────────────────────────┘
         │
         ▼ (no timer)
┌─────────────────────────┐
│ recoverStuckTaskTimer() │
└─────────────────────────┘
         │
    ┌────┴────┐
    ▼         ▼
 Success    Failure
    │         │
    │         ▼
    │    ┌─────────────┐
    │    │ DB Reload   │
    │    │ Task.findById│
    │    └─────────────┘
    │         │
    ▼         ▼
scheduleNextLoop() → SCHEDULED (with timer)
```

### Queueing Recovery Flow
```
queueingTaskWatchdog() [every 60s]
         │
         ▼
┌─────────────────────────────┐
│ Query TaskRuntimeState      │
│ status = 'QUEUEING'         │
└─────────────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│ Check for each task:        │
│ - Has recent progress?      │
│ - Has jobs queued?          │
│ - Has jobs in BullMQ?       │
└─────────────────────────────┘
         │
         ▼ (all false = truly stuck)
┌─────────────────────────────┐
│ Clean up queueingProgress   │
│ setTaskStatus → COMPLETED   │
│ scheduleNextLoop()          │
└─────────────────────────────┘
         │
         ▼
    SCHEDULED (new cycle)
```

### 99% Stuck Recovery Flow
```
checkStuckTasks() [every 60s]
         │
         ▼
┌─────────────────────────┐
│ Detect 99% stuck > 2min │
└─────────────────────────┘
         │
         ▼
┌─────────────────────────┐
│ recoverStuckJobs()      │
│ (fallback store)        │
└─────────────────────────┘
         │
         ▼
┌─────────────────────────┐
│ Recalculate percentage  │
│ If >= 99%:              │
│ - Force parent complete │
│ - Force percentage 100  │
└─────────────────────────┘
         │
         ▼
checkTaskCompletionAndScheduleNext()
         │
         ▼
    COMPLETED → SCHEDULED
```

---

## Log Prefixes

| Prefix | Category | Description |
|--------|----------|-------------|
| `TimerHealth` | taskflow | Timer verification and recovery |
| `TimerRecovery` | taskflow | Direct timer recovery attempts |
| `TimerDiagnostic` | taskflow | Timer map state diagnostics |
| `QueueingWatchdog` | taskflow | Queueing watchdog operations |
| `StuckTaskCheck` | taskflow | Stuck task detection |
| `RecoveryMech` | taskflow | 99% stuck and fallback recovery |
| `StuckJobCleanup` | taskflow | BullMQ job cleanup |
| `Watchdog` | taskflow | MongoDB schedule watchdog |
| `TaskRecovery` | taskflow | Task restart recovery |
| `JobVerification` | taskflow | Job creation verification |
| `PartialQueueingRecovery` | taskflow | Partial queueing recovery |
| `OrphanedJobRecovery` | taskflow | Orphaned job recovery |

---

## Critical Invariants

1. **Guard Against Active Queueing**: All recovery functions check `isTaskInActiveQueueing()` before recovery to avoid interrupting job creation.

2. **Timer-Status Atomicity**: Recovery functions ensure timers exist before setting SCHEDULED status.

3. **Dedup Protection**: `tasksBeingRestarted` Set prevents concurrent recovery attempts.

4. **Grace Periods**: All watchdogs include grace periods to avoid false positives during normal transitions.

5. **Persistence**: Critical state persisted to MongoDB (`TaskRuntimeState`, `TaskSchedule`) for crash recovery.
