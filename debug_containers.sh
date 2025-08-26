#!/bin/bash

# Debug script for unhealthy Docker containers
# This script collects comprehensive diagnostics and outputs to a file

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_FILE="container_debug_${TIMESTAMP}.log"

echo "Collecting container diagnostics..."
echo "Output will be saved to: $OUTPUT_FILE"

{
    echo "===== Container Health Debug Report ====="
    echo "Generated at: $(date)"
    echo "Hostname: $(hostname)"
    echo ""

    echo "===== Docker Info ====="
    docker version --format 'Server Version: {{.Server.Version}}'
    docker info --format 'Total Memory: {{.MemTotal}}'
    echo ""

    echo "===== All Containers Status ====="
    docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.State}}\t{{.Health}}"
    echo ""

    echo "===== System Resources ====="
    echo "Memory Usage:"
    free -h
    echo ""
    echo "Disk Usage:"
    df -h | grep -E "^/dev|Filesystem"
    echo ""

    # Function to check container health
    check_container_health() {
        local container_name=$1
        local container_type=$2
        
        echo "===== $container_name ($container_type) ====="
        
        # Check if container exists
        if ! docker ps -a --format '{{.Names}}' | grep -q "^${container_name}$"; then
            echo "Container $container_name does not exist"
            echo "-----------------------------------"
            echo ""
            return
        fi
        
        # Get container state
        echo "Container State:"
        docker inspect $container_name --format='State: {{.State.Status}}, Running: {{.State.Running}}, Exit Code: {{.State.ExitCode}}'
        echo ""
        
        # Get health status
        echo "Health Check Status:"
        docker inspect $container_name --format='{{json .State.Health}}' 2>/dev/null | jq '.' 2>/dev/null || echo "No health check configured"
        echo ""
        
        # Get health check logs
        echo "Health Check Logs:"
        docker inspect $container_name --format='{{json .State.Health.Log}}' 2>/dev/null | jq '.[-5:]' 2>/dev/null || echo "No health check logs"
        echo ""
        
        # Get container configuration
        echo "Health Check Configuration:"
        docker inspect $container_name --format='{{json .Config.Healthcheck}}' 2>/dev/null | jq '.' 2>/dev/null || echo "No health check configured"
        echo ""
        
        # Get resource limits
        echo "Resource Limits:"
        docker inspect $container_name --format='Memory Limit: {{.HostConfig.Memory}} bytes ({{.HostConfig.Memory | printf "%.0f" | printf "%d" | printf "%.2f" | printf "%.0f"}}MB)
Memory Swap Limit: {{.HostConfig.MemorySwap}} bytes
Memory Reservation: {{.HostConfig.MemoryReservation}} bytes'
        echo ""
        
        # Get actual resource usage
        echo "Current Resource Usage:"
        docker stats --no-stream --format "CPU: {{.CPUPerc}}, Memory: {{.MemUsage}} ({{.MemPerc}}), PIDs: {{.PIDs}}" $container_name 2>/dev/null || echo "Unable to get stats"
        echo ""
        
        # Get recent logs
        echo "Recent Error Logs (last 30 lines with errors/warnings):"
        docker logs $container_name --tail 100 2>&1 | grep -iE "error|warning|failed|fatal|panic|exception|critical" | tail -30 || echo "No error logs found"
        echo ""
        
        echo "Recent General Logs (last 20 lines):"
        docker logs $container_name --tail 20 2>&1
        echo ""
        
        # Check specific issues based on container type
        if [[ $container_type == "redis" ]]; then
            echo "=== Redis Specific Checks ==="
            
            echo "Redis Ping Test:"
            docker exec $container_name redis-cli ping 2>&1 || echo "Redis not responding to ping"
            echo ""
            
            echo "Redis Cluster Status:"
            docker exec $container_name redis-cli cluster info 2>&1 || echo "Failed to get cluster info"
            echo ""
            
            echo "Redis Cluster Nodes:"
            docker exec $container_name redis-cli cluster nodes 2>&1 | head -10 || echo "Failed to get cluster nodes"
            echo ""
            
            echo "Redis Memory Info:"
            docker exec $container_name redis-cli info memory 2>&1 | grep -E "used_memory_human|maxmemory_human|mem_fragmentation_ratio" || echo "Failed to get memory info"
            echo ""
            
            echo "Redis Error Log:"
            docker exec $container_name redis-cli --scan --pattern "*" 2>&1 | head -5 || echo "No scan possible"
            echo ""
            
        elif [[ $container_type == "client" ]]; then
            echo "=== Client Specific Checks ==="
            
            echo "Port 3001 Listening Check:"
            docker exec $container_name netstat -tlnp 2>&1 | grep :3001 || docker exec $container_name ss -tlnp 2>&1 | grep :3001 || echo "Unable to check port 3001"
            echo ""
            
            echo "Process Check:"
            docker exec $container_name ps aux 2>&1 | grep -E "node|next" | head -5 || echo "No node/next processes found"
            echo ""
            
            echo "Network Connectivity Test:"
            docker exec $container_name wget --spider -q -T 5 http://localhost:3001 2>&1 && echo "Web server responding on port 3001" || echo "Web server NOT responding on port 3001"
            echo ""
            
            echo "MongoDB Connectivity Test:"
            docker exec $container_name nc -zv mongodb 27017 -w 5 2>&1 || echo "Cannot connect to MongoDB"
            echo ""
            
        elif [[ $container_type == "server" ]]; then
            echo "=== Server Specific Checks ==="
            
            echo "Port 3003 Listening Check:"
            docker exec $container_name netstat -tlnp 2>&1 | grep :3003 || docker exec $container_name ss -tlnp 2>&1 | grep :3003 || echo "Unable to check port 3003"
            echo ""
            
            echo "Process Check:"
            docker exec $container_name ps aux 2>&1 | grep node | head -5 || echo "No node processes found"
            echo ""
            
            echo "Memory Usage Details:"
            docker exec $container_name cat /proc/1/status 2>&1 | grep -E "VmSize|VmRSS|VmData" || echo "Unable to get process memory"
            echo ""
            
        elif [[ $container_type == "mongodb" ]]; then
            echo "=== MongoDB Specific Checks ==="
            
            echo "MongoDB Status:"
            docker exec $container_name mongosh --eval "db.adminCommand('ping')" 2>&1 || echo "MongoDB not responding"
            echo ""
            
            echo "MongoDB Memory Stats:"
            docker exec $container_name mongosh --eval "db.serverStatus().mem" 2>&1 || echo "Unable to get MongoDB memory stats"
            echo ""
        fi
        
        echo "-----------------------------------"
        echo ""
    }

    # Check all containers
    echo "===== Individual Container Diagnostics ====="
    echo ""

    # Redis nodes
    for i in {1..6}; do
        check_container_health "redis_node_$i" "redis"
    done

    # MongoDB
    check_container_health "mongodb_container" "mongodb"

    # Server
    check_container_health "nft-bidding-bot-server-1" "server"

    # Client
    check_container_health "nft-bidding-bot-client-1" "client"

    # Redis cluster overall check
    echo "===== Redis Cluster Overall Analysis ====="
    echo "Attempting cluster check from redis_node_1:"
    docker exec redis_node_1 redis-cli --cluster check redis-node-1:6379 2>&1 || echo "Cluster check failed"
    echo ""

    echo "===== Docker Network Analysis ====="
    echo "Network List:"
    docker network ls
    echo ""
    
    echo "Network Inspect (my_network):"
    docker network inspect nft-bidding-bot_my_network 2>&1 | jq '.[] | {Name: .Name, Driver: .Driver, Containers: .Containers | keys}' 2>/dev/null || docker network inspect my_network 2>&1 | jq '.[] | {Name: .Name, Driver: .Driver, Containers: .Containers | keys}' 2>/dev/null || echo "Network not found"
    echo ""

    echo "===== Summary Analysis ====="
    echo "Total Containers: $(docker ps -a | tail -n +2 | wc -l)"
    echo "Running Containers: $(docker ps | tail -n +2 | wc -l)"
    echo "Unhealthy Containers: $(docker ps --filter "health=unhealthy" | tail -n +2 | wc -l)"
    echo ""

    echo "Unhealthy containers list:"
    docker ps --filter "health=unhealthy" --format "table {{.Names}}\t{{.Status}}\t{{.Health}}"
    echo ""

    echo "===== Common Issues Found ====="
    # Check for OOM kills
    if dmesg | grep -q "Out of memory"; then
        echo "⚠️  WARNING: OOM kills detected in system logs"
    fi
    
    # Check for port conflicts
    for port in 3001 3003 27017 8001 8002 8003 8004 8005 8006; do
        if netstat -tlnp 2>/dev/null | grep -q ":$port " || ss -tlnp 2>/dev/null | grep -q ":$port "; then
            echo "✓ Port $port is in use"
        else
            echo "✗ Port $port is NOT in use (container might be down)"
        fi
    done
    echo ""

    echo "===== Troubleshooting Recommendations ====="
    echo "1. For Redis cluster 'unhealthy' status:"
    echo "   - This often happens when the cluster is not fully initialized"
    echo "   - Try: docker-compose down && docker-compose up -d"
    echo "   - Or manually create cluster: docker exec -it redis_node_1 redis-cli --cluster create redis-node-1:6379 redis-node-2:6379 redis-node-3:6379 redis-node-4:6379 redis-node-5:6379 redis-node-6:6379 --cluster-replicas 1 --cluster-yes"
    echo ""
    echo "2. For Client 'unhealthy' status:"
    echo "   - Check if Next.js server started properly"
    echo "   - Verify MongoDB connection"
    echo "   - Check for JavaScript errors in logs"
    echo ""
    echo "3. For memory issues:"
    echo "   - Current total allocated: ~3GB"
    echo "   - Ensure host has enough free memory"
    echo "   - Consider reducing memory limits in compose.yaml"
    echo ""
    echo "4. Quick fixes to try:"
    echo "   docker-compose restart [container_name]  # Restart specific container"
    echo "   docker-compose logs -f [container_name]  # Watch real-time logs"
    echo "   docker system prune -a                    # Clean up disk space"
    echo ""
    
    echo "===== End of Report ====="
    echo "Report generated at: $(date)"

} > "$OUTPUT_FILE" 2>&1

echo "Diagnostics complete!"
echo "Report saved to: $OUTPUT_FILE"
echo ""
echo "To share this report:"
echo "cat $OUTPUT_FILE"