#!/bin/bash

# Redis Container Restart Monitor
# This script monitors Redis container and logs restart reasons to a file

# Configuration
CONTAINER_NAME="redis"
LOG_FILE="/var/log/redis-restart-reasons.log"
MONITOR_LOG="/var/log/redis-restart-monitor.log"
CHECK_INTERVAL=5  # Check every 5 seconds

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Create log files if they don't exist
touch "$LOG_FILE"
touch "$MONITOR_LOG"

# Function to log messages
log_message() {
    local level=$1
    local message=$2
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message" | tee -a "$MONITOR_LOG"
}

# Function to get container exit code
get_exit_code() {
    docker inspect "$CONTAINER_NAME" --format='{{.State.ExitCode}}' 2>/dev/null
}

# Function to get OOM kill status
check_oom_killed() {
    docker inspect "$CONTAINER_NAME" --format='{{.State.OOMKilled}}' 2>/dev/null
}

# Function to get container state
get_container_state() {
    docker inspect "$CONTAINER_NAME" --format='{{.State.Status}}' 2>/dev/null
}

# Function to get last 100 lines of container logs
get_container_logs() {
    docker logs "$CONTAINER_NAME" --tail 100 2>&1
}

# Function to check system memory
check_system_memory() {
    free -h
}

# Function to check Docker events for the container
get_docker_events() {
    docker events --filter container="$CONTAINER_NAME" --since "10m" --until "0s" 2>/dev/null
}

# Function to analyze and log restart reason
analyze_restart() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local report_file="/tmp/redis-restart-$(date +%s).txt"
    
    echo "===== REDIS RESTART ANALYSIS REPORT =====" > "$report_file"
    echo "Timestamp: $timestamp" >> "$report_file"
    echo "Container: $CONTAINER_NAME" >> "$report_file"
    echo "" >> "$report_file"
    
    # Get container inspection data
    echo "=== CONTAINER INSPECTION ===" >> "$report_file"
    docker inspect "$CONTAINER_NAME" >> "$report_file" 2>&1
    echo "" >> "$report_file"
    
    # Check exit code
    local exit_code=$(get_exit_code)
    echo "=== EXIT CODE: $exit_code ===" >> "$report_file"
    
    # Interpret exit codes
    case $exit_code in
        0)
            echo "Exit Code 0: Normal shutdown" >> "$report_file"
            ;;
        1)
            echo "Exit Code 1: General application error" >> "$report_file"
            ;;
        125)
            echo "Exit Code 125: Docker run command failed" >> "$report_file"
            ;;
        126)
            echo "Exit Code 126: Container command not executable" >> "$report_file"
            ;;
        127)
            echo "Exit Code 127: Container command not found" >> "$report_file"
            ;;
        137)
            echo "Exit Code 137: Container killed with SIGKILL (possibly OOM)" >> "$report_file"
            ;;
        139)
            echo "Exit Code 139: Container killed with SIGSEGV (segmentation fault)" >> "$report_file"
            ;;
        143)
            echo "Exit Code 143: Container killed with SIGTERM" >> "$report_file"
            ;;
        *)
            echo "Exit Code $exit_code: Unknown error" >> "$report_file"
            ;;
    esac
    echo "" >> "$report_file"
    
    # Check OOM status
    local oom_killed=$(check_oom_killed)
    echo "=== OOM KILLED: $oom_killed ===" >> "$report_file"
    if [ "$oom_killed" = "true" ]; then
        echo "WARNING: Container was killed due to Out Of Memory!" >> "$report_file"
    fi
    echo "" >> "$report_file"
    
    # System memory status
    echo "=== SYSTEM MEMORY STATUS ===" >> "$report_file"
    check_system_memory >> "$report_file"
    echo "" >> "$report_file"
    
    # Docker events
    echo "=== RECENT DOCKER EVENTS ===" >> "$report_file"
    get_docker_events >> "$report_file"
    echo "" >> "$report_file"
    
    # Container logs (last 100 lines)
    echo "=== LAST 100 LINES OF CONTAINER LOGS ===" >> "$report_file"
    get_container_logs >> "$report_file"
    echo "" >> "$report_file"
    
    # Check for specific Redis error patterns
    echo "=== REDIS ERROR ANALYSIS ===" >> "$report_file"
    local logs=$(get_container_logs)
    
    if echo "$logs" | grep -q "MISCONF Redis is configured to save RDB snapshots"; then
        echo "ERROR: Redis cannot save RDB snapshots (disk full or permissions issue)" >> "$report_file"
    fi
    
    if echo "$logs" | grep -q "Fatal error loading the DB"; then
        echo "ERROR: Redis fatal error loading database" >> "$report_file"
    fi
    
    if echo "$logs" | grep -q "FATAL CONFIG FILE ERROR"; then
        echo "ERROR: Redis configuration file error" >> "$report_file"
    fi
    
    if echo "$logs" | grep -q "Can't handle RDB format"; then
        echo "ERROR: Redis RDB format incompatibility" >> "$report_file"
    fi
    
    if echo "$logs" | grep -q "OOM command not allowed"; then
        echo "ERROR: Redis Out of Memory - command rejected" >> "$report_file"
    fi
    
    if echo "$logs" | grep -q "used_memory:"; then
        echo "Memory usage from Redis INFO:" >> "$report_file"
        echo "$logs" | grep -A5 -B5 "used_memory:" >> "$report_file"
    fi
    
    # Copy report to main log file
    cat "$report_file" >> "$LOG_FILE"
    echo "=========================================" >> "$LOG_FILE"
    echo "" >> "$LOG_FILE"
    
    # Also save individual report
    cp "$report_file" "/var/log/redis-restart-$(date +%Y%m%d-%H%M%S).log"
    
    log_message "ERROR" "Redis container restarted! Report saved to $report_file and $LOG_FILE"
}

# Function to check Redis health via redis-cli
check_redis_health() {
    if command -v redis-cli &> /dev/null; then
        redis-cli -h localhost -p 6379 ping 2>/dev/null || echo "FAILED"
    else
        docker exec "$CONTAINER_NAME" redis-cli ping 2>/dev/null || echo "FAILED"
    fi
}

# Main monitoring loop
log_message "INFO" "Starting Redis container restart monitor"
log_message "INFO" "Monitoring container: $CONTAINER_NAME"
log_message "INFO" "Logging to: $LOG_FILE"

# Track previous state
PREVIOUS_STATE=""
PREVIOUS_START_TIME=""

while true; do
    # Get current container state
    CURRENT_STATE=$(get_container_state)
    
    if [ -z "$CURRENT_STATE" ]; then
        log_message "WARNING" "Container $CONTAINER_NAME not found"
        sleep "$CHECK_INTERVAL"
        continue
    fi
    
    # Get container start time
    CURRENT_START_TIME=$(docker inspect "$CONTAINER_NAME" --format='{{.State.StartedAt}}' 2>/dev/null)
    
    # Check if container restarted
    if [ "$PREVIOUS_START_TIME" != "" ] && [ "$PREVIOUS_START_TIME" != "$CURRENT_START_TIME" ]; then
        log_message "ALERT" "Redis container has restarted!"
        analyze_restart
    fi
    
    # Update tracking variables
    PREVIOUS_STATE="$CURRENT_STATE"
    PREVIOUS_START_TIME="$CURRENT_START_TIME"
    
    # Check Redis health
    if [ "$CURRENT_STATE" = "running" ]; then
        REDIS_HEALTH=$(check_redis_health)
        if [ "$REDIS_HEALTH" = "FAILED" ]; then
            log_message "WARNING" "Redis health check failed but container is running"
        fi
    fi
    
    # Sleep before next check
    sleep "$CHECK_INTERVAL"
done