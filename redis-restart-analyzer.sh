#!/bin/bash

# Redis Restart Analyzer - One-time analysis script
# Run this script to analyze why Redis restarted

# Configuration
CONTAINER_NAME="redis"
OUTPUT_FILE="/root/redis-restart-analysis-$(date +%Y%m%d-%H%M%S).log"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}Redis Restart Analyzer${NC}"
echo "========================="
echo "Analyzing container: $CONTAINER_NAME"
echo "Output file: $OUTPUT_FILE"
echo ""

# Start analysis
{
    echo "===== REDIS RESTART ANALYSIS REPORT ====="
    echo "Generated: $(date)"
    echo "Host: $(hostname)"
    echo "Container: $CONTAINER_NAME"
    echo ""
    
    # 1. Container Status
    echo "=== 1. CURRENT CONTAINER STATUS ==="
    docker ps -a | grep -E "CONTAINER|$CONTAINER_NAME"
    echo ""
    
    # 2. Container Inspection
    echo "=== 2. CONTAINER STATE DETAILS ==="
    docker inspect "$CONTAINER_NAME" --format='
Status: {{.State.Status}}
Running: {{.State.Running}}
Paused: {{.State.Paused}}
Restarting: {{.State.Restarting}}
OOMKilled: {{.State.OOMKilled}}
Dead: {{.State.Dead}}
Pid: {{.State.Pid}}
ExitCode: {{.State.ExitCode}}
Error: {{.State.Error}}
StartedAt: {{.State.StartedAt}}
FinishedAt: {{.State.FinishedAt}}
RestartCount: {{.RestartCount}}
'
    echo ""
    
    # 3. Exit Code Analysis
    echo "=== 3. EXIT CODE ANALYSIS ==="
    EXIT_CODE=$(docker inspect "$CONTAINER_NAME" --format='{{.State.ExitCode}}')
    echo "Exit Code: $EXIT_CODE"
    
    case $EXIT_CODE in
        0)
            echo "✓ Normal shutdown (exit code 0)"
            ;;
        1)
            echo "✗ General application error (exit code 1)"
            echo "  - Check Redis logs for application errors"
            ;;
        137)
            echo "✗ SIGKILL received (exit code 137)"
            echo "  - Possibly killed by OOM killer"
            echo "  - Check if OOMKilled flag is true"
            echo "  - Review memory limits and usage"
            ;;
        139)
            echo "✗ Segmentation fault (exit code 139)"
            echo "  - Redis process crashed"
            echo "  - Check for corrupted data or bugs"
            ;;
        143)
            echo "✗ SIGTERM received (exit code 143)"
            echo "  - Container was gracefully stopped"
            ;;
        *)
            echo "✗ Unexpected exit code: $EXIT_CODE"
            ;;
    esac
    echo ""
    
    # 4. Memory Analysis
    echo "=== 4. MEMORY ANALYSIS ==="
    echo "System Memory:"
    free -h
    echo ""
    echo "Container Memory Limits:"
    docker inspect "$CONTAINER_NAME" --format='Memory Limit: {{.HostConfig.Memory}}'
    docker inspect "$CONTAINER_NAME" --format='Memory Swap Limit: {{.HostConfig.MemorySwap}}'
    echo ""
    
    # 5. Docker Events (last 1 hour)
    echo "=== 5. DOCKER EVENTS (Last 1 hour) ==="
    docker events --filter container="$CONTAINER_NAME" --since "1h" --until "0s" --format 'Time: {{.Time}} Action: {{.Action}} {{if .Actor.Attributes.exitCode}}ExitCode: {{.Actor.Attributes.exitCode}}{{end}}'
    echo ""
    
    # 6. System logs
    echo "=== 6. SYSTEM LOGS (OOM Killer) ==="
    if [ -f /var/log/syslog ]; then
        echo "Checking /var/log/syslog for OOM kills..."
        grep -i "killed process.*redis" /var/log/syslog | tail -20
    fi
    
    if [ -f /var/log/kern.log ]; then
        echo "Checking /var/log/kern.log for OOM kills..."
        grep -i "killed process.*redis" /var/log/kern.log | tail -20
    fi
    
    dmesg | grep -i -E "(redis|oom|killed)" | tail -20
    echo ""
    
    # 7. Container Logs (Last restart)
    echo "=== 7. CONTAINER LOGS (Last 200 lines) ==="
    docker logs "$CONTAINER_NAME" --tail 200 2>&1
    echo ""
    
    # 8. Redis Specific Checks
    echo "=== 8. REDIS SPECIFIC ERROR PATTERNS ==="
    LOGS=$(docker logs "$CONTAINER_NAME" --tail 500 2>&1)
    
    echo -n "Checking for disk space issues... "
    if echo "$LOGS" | grep -q "MISCONF Redis is configured to save RDB snapshots"; then
        echo -e "${RED}FOUND${NC}"
        echo "$LOGS" | grep -B2 -A2 "MISCONF Redis"
    else
        echo -e "${GREEN}OK${NC}"
    fi
    
    echo -n "Checking for database corruption... "
    if echo "$LOGS" | grep -q -E "(Fatal error loading|Bad file format|Wrong signature)"; then
        echo -e "${RED}FOUND${NC}"
        echo "$LOGS" | grep -B2 -A2 -E "(Fatal error loading|Bad file format|Wrong signature)"
    else
        echo -e "${GREEN}OK${NC}"
    fi
    
    echo -n "Checking for memory errors... "
    if echo "$LOGS" | grep -q -E "(OOM command not allowed|used_memory_human:[0-9.]+G)"; then
        echo -e "${RED}FOUND${NC}"
        echo "$LOGS" | grep -B2 -A2 -E "(OOM command not allowed|used_memory)"
    else
        echo -e "${GREEN}OK${NC}"
    fi
    
    echo -n "Checking for configuration errors... "
    if echo "$LOGS" | grep -q "FATAL CONFIG FILE ERROR"; then
        echo -e "${RED}FOUND${NC}"
        echo "$LOGS" | grep -B2 -A2 "FATAL CONFIG"
    else
        echo -e "${GREEN}OK${NC}"
    fi
    echo ""
    
    # 9. Disk Space
    echo "=== 9. DISK SPACE ==="
    df -h
    echo ""
    echo "Docker volumes:"
    docker system df
    echo ""
    
    # 10. Recent Container Restarts
    echo "=== 10. CONTAINER RESTART HISTORY ==="
    docker ps -a --filter "name=$CONTAINER_NAME" --format "table {{.Names}}\t{{.Status}}\t{{.CreatedAt}}"
    
    echo ""
    echo "===== END OF ANALYSIS ====="
    
} | tee "$OUTPUT_FILE"

echo ""
echo -e "${GREEN}Analysis complete!${NC}"
echo "Report saved to: $OUTPUT_FILE"
echo ""
echo -e "${YELLOW}Quick Summary:${NC}"

# Quick summary
EXIT_CODE=$(docker inspect "$CONTAINER_NAME" --format='{{.State.ExitCode}}')
OOM_KILLED=$(docker inspect "$CONTAINER_NAME" --format='{{.State.OOMKilled}}')

if [ "$OOM_KILLED" = "true" ]; then
    echo -e "${RED}⚠ Container was OOM Killed!${NC}"
    echo "  Recommendations:"
    echo "  - Increase container memory limits"
    echo "  - Optimize Redis memory usage"
    echo "  - Set maxmemory-policy in Redis config"
elif [ "$EXIT_CODE" = "137" ]; then
    echo -e "${RED}⚠ Container received SIGKILL (possibly OOM)${NC}"
    echo "  Check system logs for OOM killer activity"
elif [ "$EXIT_CODE" = "0" ]; then
    echo -e "${GREEN}✓ Container shutdown was normal${NC}"
else
    echo -e "${YELLOW}⚠ Container exited with code $EXIT_CODE${NC}"
    echo "  Check the container logs for details"
fi

# Check for disk space issues
DISK_USAGE=$(df -h / | awk 'NR==2 {print $5}' | sed 's/%//')
if [ "$DISK_USAGE" -gt 90 ]; then
    echo -e "${RED}⚠ Disk usage is critical: ${DISK_USAGE}%${NC}"
fi