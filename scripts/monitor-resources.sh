#!/bin/bash

# Monitor script for resource usage
echo "=== System Resources ==="
echo "CPU Cores: $(nproc)"
echo "Memory: $(free -h | grep Mem | awk '{print $2}')"
echo ""

echo "=== Docker Container Stats ==="
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"
echo ""

echo "=== Node.js Process ==="
NODE_PID=$(pgrep -f "node.*src/index.ts" | head -1)
if [ ! -z "$NODE_PID" ]; then
    echo "PID: $NODE_PID"
    ps aux | grep $NODE_PID | grep -v grep | awk '{print "CPU: "$3"% Memory: "$4"% RSS: "$6/1024"MB"}'
else
    echo "Node.js process not found"
fi
echo ""

echo "=== Redis Memory Usage ==="
for i in {1..6}; do
    echo -n "Node $i: "
    docker exec redis_node_$i redis-cli info memory 2>/dev/null | grep -E "used_memory_human|maxmemory_human" | awk -F: '{print $2}' | tr '\n' ' ' | awk '{print "Used: "$1" Max: "$2}'
done