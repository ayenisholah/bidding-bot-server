#!/bin/bash

# Server Restart Investigation Script
# Analyzes why the NFT Bidding Bot server restarted twice

set -e

echo "===== NFT Bidding Bot Server Restart Investigation ====="
echo "Generated at: $(date)"
echo "Hostname: $(hostname)"
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

SERVER_CONTAINER="nft-bidding-bot-server-1"

# Function to print colored output
print_status() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

print_section() {
    echo ""
    echo "===== $1 ====="
}

# Get current container info
get_container_info() {
    print_section "CURRENT CONTAINER STATUS"
    
    if docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.CreatedAt}}" | grep -q "$SERVER_CONTAINER"; then
        echo "Current container status:"
        docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.CreatedAt}}" | grep "$SERVER_CONTAINER"
        
        echo ""
        echo "Container details:"
        docker inspect $SERVER_CONTAINER --format '
Container ID: {{.Id}}
Created: {{.Created}}
Started: {{.State.StartedAt}}
Restart Count: {{.RestartCount}}
Last Exit Code: {{.State.ExitCode}}
Status: {{.State.Status}}
Restart Policy: {{.HostConfig.RestartPolicy.Name}}
Max Restart Count: {{.HostConfig.RestartPolicy.MaximumRetryCount}}
        '
    else
        print_status $RED "Container $SERVER_CONTAINER not found"
        exit 1
    fi
}

# Analyze Docker events
analyze_docker_events() {
    print_section "DOCKER EVENTS ANALYSIS"
    
    echo "Docker events for the last 2 hours (focusing on server container):"
    # Get events from the last 2 hours
    since_time=$(date -d '2 hours ago' '+%Y-%m-%dT%H:%M:%S')
    
    echo "Events since $since_time:"
    docker events --since "$since_time" --filter container="$SERVER_CONTAINER" --format 'table {{.TimeNano}}\t{{.Action}}\t{{.Actor.Attributes.name}}' 2>/dev/null || {
        echo "Trying alternative method for Docker events..."
        docker events --since "$since_time" --format 'table {{.TimeNano}}\t{{.Action}}\t{{.Actor.Attributes.name}}' 2>/dev/null | grep -i server || echo "No server-related events found"
    }
    
    echo ""
    echo "All container events (last 100 events):"
    docker events --since "$since_time" --format 'table {{.Time}}\t{{.Action}}\t{{.Actor.Attributes.name}}\t{{.Actor.Attributes.exitCode}}' 2>/dev/null | head -20 || echo "Could not retrieve Docker events"
    
    # Try to get more specific restart events
    echo ""
    echo "Looking for restart/die/kill events:"
    docker events --since "$since_time" --format '{{.Time}} {{.Action}} {{.Actor.Attributes.name}} {{.Actor.Attributes.exitCode}}' 2>/dev/null | grep -E "(die|kill|restart|stop|start)" | head -20 || echo "No restart events found in recent history"
}

# Check system logs for container issues
check_system_logs() {
    print_section "SYSTEM LOG ANALYSIS"
    
    echo "Checking systemd/journalctl logs for Docker and container issues:"
    echo ""
    
    # Check for Docker daemon issues
    echo "--- Docker daemon logs (last 50 lines) ---"
    journalctl -u docker.service --since "2 hours ago" -n 50 --no-pager 2>/dev/null || echo "Could not access Docker service logs"
    
    echo ""
    echo "--- System logs mentioning our container (last 30) ---"
    journalctl --since "2 hours ago" | grep -i -E "(nft-bidding-bot-server|container|docker)" | tail -30 || echo "No container-related system logs found"
    
    echo ""
    echo "--- Checking for OOM kills in system logs ---"
    journalctl --since "2 hours ago" | grep -i -E "(killed|oom|memory)" | tail -20 || echo "No OOM kill events found"
    
    echo ""
    echo "--- Checking dmesg for container/memory issues ---"
    dmesg | grep -i -E "(killed process|oom|docker|container)" | tail -20 || echo "No relevant dmesg entries found"
    
    # Check if there were any disk space issues
    echo ""
    echo "--- Checking for disk space issues ---"
    journalctl --since "2 hours ago" | grep -i -E "(space|disk|no space)" | tail -10 || echo "No disk space issues found"
}

# Analyze container restart patterns
analyze_restart_patterns() {
    print_section "RESTART PATTERN ANALYSIS"
    
    echo "Container creation and restart timeline:"
    
    # Get container creation time
    created_time=$(docker inspect $SERVER_CONTAINER --format '{{.Created}}' 2>/dev/null)
    started_time=$(docker inspect $SERVER_CONTAINER --format '{{.State.StartedAt}}' 2>/dev/null)
    restart_count=$(docker inspect $SERVER_CONTAINER --format '{{.RestartCount}}' 2>/dev/null)
    
    echo "Container created: $created_time"
    echo "Current instance started: $started_time" 
    echo "Total restart count: $restart_count"
    
    # Calculate time differences
    if command -v python3 >/dev/null 2>&1; then
        echo ""
        echo "Time analysis:"
        python3 -c "
from datetime import datetime
import sys

try:
    created = datetime.fromisoformat('$created_time'.replace('Z', '+00:00'))
    started = datetime.fromisoformat('$started_time'.replace('Z', '+00:00'))
    
    time_diff = started - created
    print(f'Time between creation and current start: {time_diff}')
    print(f'This suggests the container failed and restarted {restart_count} times over {time_diff}')
    
    if time_diff.total_seconds() > 3600:  # More than 1 hour
        print('⚠️  Long delay between creation and start suggests multiple restart attempts')
    else:
        print('✓ Container started relatively quickly after creation')
except Exception as e:
    print(f'Could not parse timestamps: {e}')
"
    fi
    
    echo ""
    echo "Restart policy details:"
    docker inspect $SERVER_CONTAINER --format '
Restart Policy: {{.HostConfig.RestartPolicy.Name}}
Maximum Retry Count: {{.HostConfig.RestartPolicy.MaximumRetryCount}}
Current Restart Count: {{.RestartCount}}
'
}

# Deep dive into container logs
analyze_container_logs_deep() {
    print_section "DEEP CONTAINER LOG ANALYSIS"
    
    echo "Full container log analysis (looking for crash patterns):"
    
    # Get all logs
    echo "--- Container startup sequence ---"
    docker logs $SERVER_CONTAINER 2>&1 | head -50
    
    echo ""
    echo "--- All error/warning messages ---"
    docker logs $SERVER_CONTAINER 2>&1 | grep -i -E "(error|warn|fail|exception|crash|fatal|panic|kill|exit|died)" || echo "No error messages found"
    
    echo ""
    echo "--- Database connection issues ---"
    docker logs $SERVER_CONTAINER 2>&1 | grep -i -E "(mongo|redis|connection|timeout|refused)" || echo "No database connection issues found"
    
    echo ""
    echo "--- Memory/Resource issues ---"
    docker logs $SERVER_CONTAINER 2>&1 | grep -i -E "(memory|heap|oom|resource|limit)" || echo "No memory issues found"
    
    echo ""
    echo "--- Port/Network issues ---"
    docker logs $SERVER_CONTAINER 2>&1 | grep -i -E "(port|listen|bind|network|eaddrinuse)" || echo "No port/network issues found"
    
    echo ""
    echo "--- Uncaught exceptions or crashes ---"
    docker logs $SERVER_CONTAINER 2>&1 | grep -i -E "(uncaught|unhandled|segfault|core dump)" || echo "No uncaught exceptions found"
    
    echo ""
    echo "--- Process exit messages ---"
    docker logs $SERVER_CONTAINER 2>&1 | grep -i -E "(exit|quit|stop|shut|term)" || echo "No process exit messages found"
    
    # Check the last 100 lines before current startup for clues
    echo ""
    echo "--- Last messages before current startup (may show crash reason) ---"
    docker logs $SERVER_CONTAINER 2>&1 | tail -100 | head -50
}

# Check resource constraints that might cause restarts
check_resource_constraints() {
    print_section "RESOURCE CONSTRAINT ANALYSIS"
    
    echo "Memory limits and usage:"
    memory_limit=$(docker inspect $SERVER_CONTAINER --format '{{.HostConfig.Memory}}')
    if [[ "$memory_limit" != "0" ]]; then
        echo "Container memory limit: $((memory_limit / 1024 / 1024))MB"
    else
        echo "No memory limit set"
    fi
    
    # Check current memory usage
    echo ""
    echo "Current resource usage:"
    docker stats --no-stream $SERVER_CONTAINER
    
    echo ""
    echo "System memory status:"
    free -h
    
    echo ""
    echo "Checking for memory pressure events:"
    # Look for memory cgroup events
    if [ -d "/sys/fs/cgroup/memory/docker" ]; then
        echo "Docker cgroup memory stats available"
        find /sys/fs/cgroup -name "*$SERVER_CONTAINER*" -type d 2>/dev/null | head -3
    else
        echo "Docker cgroup memory stats not accessible"
    fi
    
    echo ""
    echo "Historical memory usage (if available):"
    # Check if we can get historical stats
    if command -v sar >/dev/null 2>&1; then
        echo "System Activity Reporter (sar) data:"
        sar -r 1 1 2>/dev/null || echo "No sar data available"
    fi
}

# Check container environment and configuration
check_container_environment() {
    print_section "CONTAINER ENVIRONMENT ANALYSIS"
    
    echo "Container environment variables (filtered for sensitive data):"
    docker exec $SERVER_CONTAINER env | grep -v -E "(PASSWORD|SECRET|KEY|TOKEN)" | sort
    
    echo ""
    echo "Container resource limits:"
    docker inspect $SERVER_CONTAINER --format '
Memory Limit: {{.HostConfig.Memory}}
Memory Swap Limit: {{.HostConfig.MemorySwap}}  
CPU Shares: {{.HostConfig.CpuShares}}
CPU Period: {{.HostConfig.CpuPeriod}}
CPU Quota: {{.HostConfig.CpuQuota}}
PID Limit: {{.HostConfig.PidsLimit}}
'

    echo ""
    echo "Current process list inside container:"
    docker exec $SERVER_CONTAINER ps aux
    
    echo ""
    echo "Container network configuration:"
    docker inspect $SERVER_CONTAINER --format '{{range $net, $conf := .NetworkSettings.Networks}}Network: {{$net}}, IP: {{$conf.IPAddress}}{{end}}'
}

# Main execution
main() {
    get_container_info
    analyze_docker_events  
    check_system_logs
    analyze_restart_patterns
    analyze_container_logs_deep
    check_resource_constraints
    check_container_environment
    
    print_section "INVESTIGATION SUMMARY"
    
    echo "Key Findings:"
    
    # Analyze restart count
    restart_count=$(docker inspect $SERVER_CONTAINER --format '{{.RestartCount}}' 2>/dev/null || echo "0")
    if [[ "$restart_count" -gt "0" ]]; then
        print_status $YELLOW "• Server has restarted $restart_count times"
        
        # Try to determine likely cause
        if docker logs $SERVER_CONTAINER 2>&1 | grep -q -i "mongo"; then
            print_status $BLUE "  - Possible MongoDB connection issues detected in logs"
        fi
        
        if docker logs $SERVER_CONTAINER 2>&1 | grep -q -i "redis"; then
            print_status $BLUE "  - Possible Redis connection issues detected in logs"
        fi
        
        if docker logs $SERVER_CONTAINER 2>&1 | grep -q -i -E "(error|fail|exception)"; then
            print_status $BLUE "  - Application errors detected in logs"
        fi
        
        # Check if it's a resource issue
        memory_limit=$(docker inspect $SERVER_CONTAINER --format '{{.HostConfig.Memory}}' 2>/dev/null || echo "0")
        if [[ "$memory_limit" != "0" ]] && [[ "$memory_limit" -lt "1073741824" ]]; then  # Less than 1GB
            print_status $YELLOW "  - Container has relatively low memory limit: $((memory_limit / 1024 / 1024))MB"
        fi
    fi
    
    echo ""
    echo "Recommendations:"
    echo "1. Check the full container logs: docker logs $SERVER_CONTAINER | less"
    echo "2. Monitor container in real-time: docker logs $SERVER_CONTAINER -f"
    echo "3. Check resource usage: docker stats $SERVER_CONTAINER"
    echo "4. Review Docker events: docker events --filter container=$SERVER_CONTAINER"
    
    if [[ "$restart_count" -gt "0" ]]; then
        echo "5. Consider increasing memory limit if resource-constrained"
        echo "6. Check database connection stability"
        echo "7. Review application error handling"
    fi
    
    print_section "END OF INVESTIGATION"
    echo "Report completed at: $(date)"
}

# Run the investigation
main