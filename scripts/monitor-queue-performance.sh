#!/bin/bash

# Queue Performance Monitoring Script
# For 2 vCPU / 4GB system optimization

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "🔍 Queue Performance Monitor"
echo "============================"
echo ""

# Function to get queue stats from API
get_queue_stats() {
    curl -s http://localhost:3003/health | jq -r '.queueCounts' 2>/dev/null || echo "{}"
}

# Function to check Redis memory
check_redis_memory() {
    echo "📊 Redis Memory Usage:"
    for i in {1..3}; do
        if docker exec redis_node_$i redis-cli info memory 2>/dev/null | grep -E "used_memory_human|maxmemory_human" > /dev/null; then
            echo -n "  Node $i: "
            docker exec redis_node_$i redis-cli info memory 2>/dev/null | grep -E "used_memory_human|maxmemory_human" | awk -F: '{print $2}' | tr '\n' ' ' | awk '{print "Used: "$1" / Max: "$2}'
        fi
    done
}

# Function to check system resources
check_system_resources() {
    echo ""
    echo "💻 System Resources:"
    
    # CPU usage
    CPU_USAGE=$(top -l 1 | grep "CPU usage" | awk '{print $3}' | sed 's/%//')
    if (( $(echo "$CPU_USAGE > 80" | bc -l) )); then
        echo -e "  CPU: ${RED}${CPU_USAGE}%${NC} ⚠️  HIGH"
    elif (( $(echo "$CPU_USAGE > 60" | bc -l) )); then
        echo -e "  CPU: ${YELLOW}${CPU_USAGE}%${NC}"
    else
        echo -e "  CPU: ${GREEN}${CPU_USAGE}%${NC}"
    fi
    
    # Memory usage
    MEM_INFO=$(vm_stat | perl -ne '/page size of (\d+)/ and $size=$1; /Pages\s+([^:]+)[^\d]+(\d+)/ and printf("%-16s % 16.2f MB\n", "$1:", $2 * $size / 1048576);')
    TOTAL_MEM=4096  # 4GB in MB
    USED_MEM=$(echo "$MEM_INFO" | grep -E "(wired|active|compressed)" | awk '{sum+=$2} END {print sum}')
    MEM_PERCENT=$(echo "scale=2; $USED_MEM / $TOTAL_MEM * 100" | bc)
    
    if (( $(echo "$MEM_PERCENT > 85" | bc -l) )); then
        echo -e "  Memory: ${RED}${MEM_PERCENT}%${NC} (${USED_MEM}MB / ${TOTAL_MEM}MB) ⚠️  HIGH"
    elif (( $(echo "$MEM_PERCENT > 70" | bc -l) )); then
        echo -e "  Memory: ${YELLOW}${MEM_PERCENT}%${NC} (${USED_MEM}MB / ${TOTAL_MEM}MB)"
    else
        echo -e "  Memory: ${GREEN}${MEM_PERCENT}%${NC} (${USED_MEM}MB / ${TOTAL_MEM}MB)"
    fi
}

# Function to check Node.js process
check_nodejs_process() {
    echo ""
    echo "🟢 Node.js Process:"
    
    NODE_PID=$(pgrep -f "node.*src/index.ts" | head -1)
    if [ ! -z "$NODE_PID" ]; then
        NODE_INFO=$(ps aux | grep $NODE_PID | grep -v grep)
        NODE_CPU=$(echo "$NODE_INFO" | awk '{print $3}')
        NODE_MEM=$(echo "$NODE_INFO" | awk '{print $4}')
        NODE_RSS=$(echo "$NODE_INFO" | awk '{print $6/1024}')
        
        echo "  PID: $NODE_PID"
        
        if (( $(echo "$NODE_CPU > 100" | bc -l) )); then
            echo -e "  CPU: ${RED}${NODE_CPU}%${NC} ⚠️  HIGH"
        else
            echo -e "  CPU: ${GREEN}${NODE_CPU}%${NC}"
        fi
        
        echo "  Memory: ${NODE_MEM}% (RSS: ${NODE_RSS}MB)"
    else
        echo -e "  ${RED}Process not found${NC}"
    fi
}

# Function to check job failures
check_job_failures() {
    echo ""
    echo "❌ Recent Job Failures (last 5 minutes):"
    
    FAILURES=$(docker logs server --since 5m 2>&1 | grep -i "fail\|error" | wc -l)
    LOCK_FAILURES=$(docker logs server --since 5m 2>&1 | grep -i "could not renew lock" | wc -l)
    RATE_LIMITS=$(docker logs server --since 5m 2>&1 | grep -i "rate limit\|429" | wc -l)
    
    echo "  Total failures: $FAILURES"
    echo "  Lock renewal failures: $LOCK_FAILURES"
    echo "  Rate limit errors: $RATE_LIMITS"
    
    if [ $LOCK_FAILURES -gt 5 ]; then
        echo -e "  ${RED}⚠️  High lock renewal failures - increase lockDuration${NC}"
    fi
    
    if [ $RATE_LIMITS -gt 10 ]; then
        echo -e "  ${RED}⚠️  High rate limit errors - reduce concurrency${NC}"
    fi
}

# Function to show recommendations
show_recommendations() {
    echo ""
    echo "📋 Optimization Recommendations:"
    
    # Check if using optimized config
    if ! grep -q "compose.dev.3node.yaml\|compose.dev.optimized.yaml" ~/.bash_history 2>/dev/null; then
        echo -e "  ${YELLOW}• Switch to 3-node Redis cluster (compose.dev.3node.yaml)${NC}"
    fi
    
    # Check broadcast interval
    if ! grep -q "BROADCAST_INTERVAL=5000" .env 2>/dev/null; then
        echo -e "  ${YELLOW}• Set BROADCAST_INTERVAL=5000 in .env${NC}"
    fi
    
    # Check worker concurrency
    CURRENT_RATE_LIMIT=$(grep "RATE_LIMIT=" .env 2>/dev/null | cut -d= -f2)
    if [ "$CURRENT_RATE_LIMIT" -gt "20" ]; then
        echo -e "  ${YELLOW}• Reduce RATE_LIMIT to 20 for 2 vCPU system${NC}"
    fi
    
    echo ""
}

# Main monitoring loop
while true; do
    clear
    echo "🔍 Queue Performance Monitor - $(date)"
    echo "============================"
    
    # Get queue stats
    QUEUE_STATS=$(get_queue_stats)
    if [ ! -z "$QUEUE_STATS" ] && [ "$QUEUE_STATS" != "{}" ]; then
        echo ""
        echo "📈 Queue Statistics:"
        echo "$QUEUE_STATS" | jq -r 'to_entries | .[] | "  \(.key): \(.value)"' 2>/dev/null
    fi
    
    check_redis_memory
    check_system_resources
    check_nodejs_process
    check_job_failures
    show_recommendations
    
    echo ""
    echo "Press Ctrl+C to exit. Refreshing in 10 seconds..."
    sleep 10
done