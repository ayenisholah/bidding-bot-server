#!/bin/bash

# Enhanced Docker Diagnostics Script for OOM Issues
# This script captures detailed memory and container information

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_FILE="docker_oom_diagnostics_${TIMESTAMP}.log"

echo "Starting enhanced diagnostics collection..."
echo "Output will be saved to: $OUTPUT_FILE"

{
    echo "===== Docker OOM Diagnostics Report ====="
    echo "Generated at: $(date)"
    echo ""

    echo "===== System Memory Info ====="
    free -h
    echo ""
    cat /proc/meminfo | head -20
    echo ""

    echo "===== Memory Pressure ====="
    vmstat 1 5
    echo ""

    echo "===== Top Memory Consumers ====="
    ps aux --sort=-%mem | head -20
    echo ""

    echo "===== Docker Memory Usage ====="
    docker system df
    echo ""
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Size}}" --size
    echo ""

    echo "===== Container Resource Stats ====="
    docker stats --no-stream --format "table {{.Container}}\t{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}"
    echo ""

    echo "===== Container Resource Limits ====="
    for container in $(docker ps -q); do
        name=$(docker inspect $container --format '{{.Name}}' | sed 's/^\///')
        echo "Container: $name"
        docker inspect $container --format '
Memory Limit: {{.HostConfig.Memory}}
Memory Reservation: {{.HostConfig.MemoryReservation}}
Memory Swap: {{.HostConfig.MemorySwap}}
CPU Quota: {{.HostConfig.CpuQuota}}
CPU Period: {{.HostConfig.CpuPeriod}}
Restart Policy: {{.HostConfig.RestartPolicy.Name}}
OOM Kill Disable: {{.HostConfig.OomKillDisable}}'
        echo ""
    done

    echo "===== Server Container Details ====="
    SERVER_ID=$(docker ps -q -f name=server)
    if [ ! -z "$SERVER_ID" ]; then
        echo "Restart Count and State:"
        docker inspect $SERVER_ID --format '
Restart Count: {{.RestartCount}}
Started At: {{.State.StartedAt}}
Status: {{.State.Status}}
OOMKilled: {{.State.OOMKilled}}
Exit Code: {{.State.ExitCode}}'
        echo ""
        
        echo "Process Memory Map (top 10):"
        docker exec $SERVER_ID sh -c "cat /proc/1/status | grep -E 'VmSize|VmRSS|VmData|VmStk|VmExe|VmLib'" 2>/dev/null || echo "Unable to get process memory"
        echo ""
        
        echo "Node.js Memory Usage:"
        docker exec $SERVER_ID sh -cnode -e "console.log(process.memoryUsage())" 2>/dev/null || echo "Unable to get Node.js memory"
        echo ""
    fi

    echo "===== Recent OOM Events (last 100 lines) ====="
    dmesg | grep -i "out of memory\|oom-kill\|killed process" | tail -100
    echo ""

    echo "===== Container Logs Around Restart Time ====="
    echo "Server container (last 200 lines before restart):"
    docker logs nft-bidding-bot-server-1 --tail 200 2>&1 | grep -B 50 -A 10 "SIGTERM\|Error\|crash\|memory"
    echo ""

    echo "===== Docker Events (last 2 hours) ====="
    docker events --since "2 hours ago" --filter type=container --filter event=die --filter event=oom --filter event=kill
    echo ""

    echo "===== Systemd/Journal Logs for Docker ====="
    journalctl -u docker --since "2 hours ago" | grep -i "memory\|oom\|killed" | tail -50
    echo ""

    echo "===== Memory Configuration Recommendations ====="
    TOTAL_MEM=$(free -b | awk 'NR==2{print $2}')
    TOTAL_MEM_GB=$(echo "scale=2; $TOTAL_MEM / 1024 / 1024 / 1024" | bc)
    echo "Total System Memory: $TOTAL_MEM_GB GB"
    echo ""
    echo "Recommended container memory limits:"
    echo "- Server: $(echo "scale=0; $TOTAL_MEM * 0.4 / 1024 / 1024" | bc)M (40% of total)"
    echo "- MongoDB: $(echo "scale=0; $TOTAL_MEM * 0.25 / 1024 / 1024" | bc)M (25% of total)"
    echo "- Redis (total): $(echo "scale=0; $TOTAL_MEM * 0.2 / 1024 / 1024" | bc)M (20% of total)"
    echo "- Client: $(echo "scale=0; $TOTAL_MEM * 0.1 / 1024 / 1024" | bc)M (10% of total)"
    echo "- System overhead: 5%"
    echo ""

    echo "===== Process Tree ====="
    ps auxf | grep -E "node|redis|mongo" | grep -v grep
    echo ""

    echo "===== Swap Configuration ====="
    swapon -s
    echo ""
    echo "Swappiness value:"
    cat /proc/sys/vm/swappiness
    echo ""

    echo "===== Cgroup Memory Limits ====="
    for container in $(docker ps -q); do
        name=$(docker inspect $container --format '{{.Name}}' | sed 's/^\///')
        cgroup_path="/sys/fs/cgroup/memory/docker/${container}*/memory.stat"
        if [ -f $cgroup_path ]; then
            echo "Container: $name"
            cat $cgroup_path | grep -E "cache|rss|swap|limit" | head -10
            echo ""
        fi
    done

} > "$OUTPUT_FILE" 2>&1

echo "Diagnostics collection complete!"
echo "Report saved to: $OUTPUT_FILE"
echo ""
echo "Quick summary of findings:"
echo "=========================="
grep -i "out of memory" "$OUTPUT_FILE" | wc -l | xargs echo "OOM Kill events found:"
docker ps -a --filter "status=exited" --filter "name=server" --format "table {{.Names}}\t{{.Status}}" | tail -5

# Check if swap is enabled
if [ $(swapon -s | wc -l) -eq 0 ]; then
    echo ""
    echo "WARNING: No swap space configured!"
    echo "Consider adding swap to prevent OOM kills:"
    echo "  sudo fallocate -l 2G /swapfile"
    echo "  sudo chmod 600 /swapfile"
    echo "  sudo mkswap /swapfile"
    echo "  sudo swapon /swapfile"
fi