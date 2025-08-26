#!/bin/bash

# Swap Setup Script for VPS
# This script creates and enables 2GB of swap space

echo "Setting up 2GB swap space..."

# Check if swap already exists
if [ $(swapon -s | wc -l) -gt 0 ]; then
    echo "Swap already configured:"
    swapon -s
    exit 0
fi

# Create swap file
echo "Creating 2GB swap file..."
sudo fallocate -l 2G /swapfile

# Set permissions
echo "Setting permissions..."
sudo chmod 600 /swapfile

# Make swap
echo "Creating swap area..."
sudo mkswap /swapfile

# Enable swap
echo "Enabling swap..."
sudo swapon /swapfile

# Make permanent
echo "Making swap permanent..."
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

# Set swappiness (optional - lower value = less swap usage)
echo "Setting swappiness to 10..."
echo 'vm.swappiness=10' | sudo tee -a /etc/sysctl.conf
sudo sysctl vm.swappiness=10

echo "Swap setup complete!"
echo ""
echo "Current swap status:"
free -h