#!/bin/bash

echo "Fixing system configuration for Docker containers..."

# Fix Redis memory overcommit warning
echo "Setting vm.overcommit_memory = 1 for Redis"
sudo sysctl vm.overcommit_memory=1

# Make it persistent
echo "Making vm.overcommit_memory persistent"
if ! grep -q "vm.overcommit_memory = 1" /etc/sysctl.conf; then
    echo "vm.overcommit_memory = 1" | sudo tee -a /etc/sysctl.conf
fi

# Set transparent huge pages to never (Redis recommendation)
echo "Disabling transparent huge pages for Redis"
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || echo "Could not disable transparent huge pages (normal on some systems)"

# Increase max map count for better performance
echo "Setting vm.max_map_count for better performance"
sudo sysctl vm.max_map_count=262144

# Make it persistent
if ! grep -q "vm.max_map_count = 262144" /etc/sysctl.conf; then
    echo "vm.max_map_count = 262144" | sudo tee -a /etc/sysctl.conf
fi

echo "System configuration completed!"
echo "Changes applied:"
echo "- vm.overcommit_memory = 1 (prevents Redis memory issues)"
echo "- vm.max_map_count = 262144 (improves performance)"
echo ""
echo "These settings are now persistent and will survive reboots."