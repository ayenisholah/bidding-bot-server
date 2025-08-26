#!/bin/bash

# Container Health Diagnostic Script
# Analyzes why NFT Bidding Bot containers are unhealthy

set -e

echo "===== NFT Bidding Bot Container Health Diagnostic ====="
echo "Generated at: $(date)"
echo "Hostname: $(hostname)"
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Container names
CLIENT_CONTAINER="nft-bidding-bot-client-1"
SERVER_CONTAINER="nft-bidding-bot-server-1"
MONGO_CONTAINER="mongodb_container"

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

# Check if containers exist and are running
check_container_status() {
    print_section "CONTAINER STATUS CHECK"
    
    for container in $CLIENT_CONTAINER $SERVER_CONTAINER $MONGO_CONTAINER; do
        if docker ps -a --format "table {{.Names}}\t{{.Status}}" | grep -q "$container"; then
            status=$(docker ps -a --format "table {{.Names}}\t{{.Status}}" | grep "$container" | awk '{print $2,$3,$4,$5}')
            if [[ $status == *"healthy"* ]]; then
                print_status $GREEN "✓ $container: $status"
            elif [[ $status == *"unhealthy"* ]]; then
                print_status $RED "✗ $container: $status"
            elif [[ $status == *"Up"* ]]; then
                print_status $YELLOW "⚠ $container: $status (no health check)"
            else
                print_status $RED "✗ $container: $status"
            fi
        else
            print_status $RED "✗ $container: NOT FOUND"
        fi
    done
}

# Analyze health checks
analyze_health_checks() {
    print_section "HEALTH CHECK ANALYSIS"
    
    for container in $CLIENT_CONTAINER $SERVER_CONTAINER; do
        echo ""
        echo "--- $container Health Check ---"
        
        # Check if container exists
        if ! docker ps -a --format "{{.Names}}" | grep -q "^$container$"; then
            print_status $RED "Container $container not found"
            continue
        fi
        
        # Get health check configuration
        health_config=$(docker inspect $container --format '{{.Config.Healthcheck}}' 2>/dev/null)
        if [[ "$health_config" == "<no value>" ]] || [[ "$health_config" == "" ]]; then
            print_status $YELLOW "No health check configured for $container"
            continue
        fi
        
        echo "Health Check Config: $health_config"
        
        # Get health check logs
        health_logs=$(docker inspect $container --format '{{range .State.Health.Log}}{{.Output}}{{end}}' 2>/dev/null)
        if [[ -n "$health_logs" ]]; then
            echo "Recent Health Check Output:"
            echo "$health_logs" | tail -10
        else
            echo "No health check logs available"
        fi
        
        # Try to run health check manually based on container type
        echo "Manual Health Check Test:"
        if [[ "$container" == "$CLIENT_CONTAINER" ]]; then
            # Test Next.js server
            if curl -sf http://localhost:3001 >/dev/null 2>&1; then
                print_status $GREEN "✓ Client responding on port 3001"
            else
                print_status $RED "✗ Client not responding on port 3001"
                # Check if process is running
                docker exec $container ps aux | grep -i next || echo "Next.js process not found"
            fi
        elif [[ "$container" == "$SERVER_CONTAINER" ]]; then
            # Test Express server
            if curl -sf http://localhost:3003/health >/dev/null 2>&1; then
                print_status $GREEN "✓ Server responding on port 3003/health"
            else
                print_status $RED "✗ Server not responding on port 3003/health"
                # Try basic port check
                if curl -sf http://localhost:3003 >/dev/null 2>&1; then
                    print_status $YELLOW "⚠ Server responding on port 3003 but /health endpoint failing"
                fi
            fi
        fi
    done
}

# Analyze container logs for errors
analyze_container_logs() {
    print_section "CONTAINER LOG ANALYSIS"
    
    for container in $CLIENT_CONTAINER $SERVER_CONTAINER; do
        echo ""
        echo "--- $container Recent Logs (Last 50 lines) ---"
        
        if ! docker ps -a --format "{{.Names}}" | grep -q "^$container$"; then
            print_status $RED "Container $container not found"
            continue
        fi
        
        # Get recent logs
        echo "Recent general logs:"
        docker logs --tail 20 $container 2>&1 | tail -20
        
        echo ""
        echo "Error/Warning logs (last 30 lines):"
        docker logs --tail 100 $container 2>&1 | grep -i -E "(error|warn|fail|exception|crash|kill|exit)" | tail -30 || echo "No error/warning logs found"
        
        # Check for OOM kills
        echo ""
        echo "Checking for OOM kills:"
        docker logs $container 2>&1 | grep -i "killed" || echo "No kill signals found"
        
        # Check container restart count
        restart_count=$(docker inspect $container --format '{{.RestartCount}}' 2>/dev/null)
        echo "Container restart count: $restart_count"
        
        # Check when container was started
        started_at=$(docker inspect $container --format '{{.State.StartedAt}}' 2>/dev/null)
        echo "Container started at: $started_at"
    done
}

# Test service connectivity
test_service_connectivity() {
    print_section "SERVICE CONNECTIVITY TEST"
    
    echo "Testing external connectivity:"
    
    # Test client port 3001
    if curl -sf -m 5 http://localhost:3001 >/dev/null 2>&1; then
        print_status $GREEN "✓ Client port 3001 accessible externally"
    else
        print_status $RED "✗ Client port 3001 not accessible externally"
    fi
    
    # Test server port 3003
    if curl -sf -m 5 http://localhost:3003 >/dev/null 2>&1; then
        print_status $GREEN "✓ Server port 3003 accessible externally"
    else
        print_status $RED "✗ Server port 3003 not accessible externally"
    fi
    
    # Test server health endpoint
    if curl -sf -m 5 http://localhost:3003/health >/dev/null 2>&1; then
        print_status $GREEN "✓ Server /health endpoint responding"
    else
        print_status $RED "✗ Server /health endpoint not responding"
    fi
    
    echo ""
    echo "Testing internal container connectivity:"
    
    # Test from inside containers
    for container in $CLIENT_CONTAINER $SERVER_CONTAINER; do
        if docker ps --format "{{.Names}}" | grep -q "^$container$"; then
            echo "--- Testing from $container ---"
            
            # Test MongoDB connection
            if docker exec $container nc -z mongodb_container 27017 2>/dev/null; then
                print_status $GREEN "✓ MongoDB accessible from $container"
            else
                print_status $RED "✗ MongoDB not accessible from $container"
            fi
            
            # Test Redis connection
            if docker exec $container nc -z redis_node_1 6379 2>/dev/null; then
                print_status $GREEN "✓ Redis accessible from $container"
            else
                print_status $RED "✗ Redis not accessible from $container"
            fi
            
            if [[ "$container" == "$CLIENT_CONTAINER" ]]; then
                # Test server connection from client
                if docker exec $container nc -z $SERVER_CONTAINER 3003 2>/dev/null; then
                    print_status $GREEN "✓ Server accessible from client"
                else
                    print_status $RED "✗ Server not accessible from client"
                fi
            fi
        fi
    done
}

# Check resource usage
check_resource_usage() {
    print_section "RESOURCE USAGE ANALYSIS"
    
    for container in $CLIENT_CONTAINER $SERVER_CONTAINER; do
        if docker ps --format "{{.Names}}" | grep -q "^$container$"; then
            echo "--- $container Resource Usage ---"
            
            # Get container stats
            docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.PIDs}}" $container
            
            # Check memory limits
            memory_limit=$(docker inspect $container --format '{{.HostConfig.Memory}}' 2>/dev/null)
            if [[ "$memory_limit" != "0" ]]; then
                echo "Memory limit: $((memory_limit / 1024 / 1024))MB"
            else
                echo "No memory limit set"
            fi
            
            # Check if container is being OOM killed
            dmesg | grep -i "killed process" | grep -i docker | tail -5 || echo "No recent OOM kills in dmesg"
            
            echo ""
        fi
    done
    
    echo "System memory usage:"
    free -h
    
    echo ""
    echo "System disk usage:"
    df -h | head -10
}

# Check Docker network
check_docker_network() {
    print_section "DOCKER NETWORK ANALYSIS"
    
    echo "Docker networks:"
    docker network ls
    
    echo ""
    echo "Container network details:"
    for container in $CLIENT_CONTAINER $SERVER_CONTAINER $MONGO_CONTAINER; do
        if docker ps --format "{{.Names}}" | grep -q "^$container$"; then
            echo "--- $container Network ---"
            docker inspect $container --format '{{range $net, $conf := .NetworkSettings.Networks}}Network: {{$net}}, IP: {{$conf.IPAddress}}, Gateway: {{$conf.Gateway}}{{end}}'
        fi
    done
    
    echo ""
    echo "Port bindings:"
    docker port $CLIENT_CONTAINER 2>/dev/null || echo "No port bindings for $CLIENT_CONTAINER"
    docker port $SERVER_CONTAINER 2>/dev/null || echo "No port bindings for $SERVER_CONTAINER"
}

# Check process status inside containers
check_container_processes() {
    print_section "CONTAINER PROCESS ANALYSIS"
    
    for container in $CLIENT_CONTAINER $SERVER_CONTAINER; do
        if docker ps --format "{{.Names}}" | grep -q "^$container$"; then
            echo "--- $container Processes ---"
            docker exec $container ps aux || echo "Could not list processes in $container"
            echo ""
        fi
    done
}

# Main execution
main() {
    check_container_status
    analyze_health_checks
    analyze_container_logs
    test_service_connectivity
    check_resource_usage
    check_docker_network
    check_container_processes
    
    print_section "SUMMARY & RECOMMENDATIONS"
    
    echo "Key Issues Found:"
    
    # Check client health
    if ! curl -sf -m 5 http://localhost:3001 >/dev/null 2>&1; then
        print_status $RED "• Client (port 3001) is not responding - check Next.js startup and MongoDB connection"
    fi
    
    # Check server health
    if ! curl -sf -m 5 http://localhost:3003/health >/dev/null 2>&1; then
        print_status $RED "• Server health endpoint not responding - check Express server startup and database connections"
    fi
    
    # Check restart count
    server_restarts=$(docker inspect $SERVER_CONTAINER --format '{{.RestartCount}}' 2>/dev/null || echo "0")
    if [[ "$server_restarts" -gt "0" ]]; then
        print_status $YELLOW "• Server has restarted $server_restarts times - check logs for crash reasons"
    fi
    
    echo ""
    echo "Quick fixes to try:"
    echo "1. docker logs $CLIENT_CONTAINER -f    # Watch client logs"
    echo "2. docker logs $SERVER_CONTAINER -f    # Watch server logs"
    echo "3. docker restart $CLIENT_CONTAINER    # Restart client"
    echo "4. docker restart $SERVER_CONTAINER    # Restart server"
    echo "5. docker exec $CLIENT_CONTAINER curl http://localhost:3001  # Test from inside client"
    echo "6. docker exec $SERVER_CONTAINER curl http://localhost:3003/health  # Test from inside server"
    
    print_section "END OF DIAGNOSTIC"
    echo "Report completed at: $(date)"
}

# Run the diagnostic
main