#!/bin/bash

# Installation script for Redis restart monitoring

echo "Installing Redis Container Restart Monitor..."

# 1. Copy the monitor script
sudo cp redis-restart-monitor.sh /usr/local/bin/
sudo chmod +x /usr/local/bin/redis-restart-monitor.sh

# 2. Copy the analyzer script  
sudo cp redis-restart-analyzer.sh /usr/local/bin/
sudo chmod +x /usr/local/bin/redis-restart-analyzer.sh

# 3. Create log directory
sudo mkdir -p /var/log
sudo touch /var/log/redis-restart-reasons.log
sudo touch /var/log/redis-restart-monitor.log

# 4. Install systemd service (optional)
if command -v systemctl &> /dev/null; then
    echo "Installing systemd service..."
    sudo cp redis-monitor.service /etc/systemd/system/
    sudo systemctl daemon-reload
    echo ""
    echo "To start monitoring as a service:"
    echo "  sudo systemctl start redis-monitor"
    echo "  sudo systemctl enable redis-monitor  # For auto-start on boot"
    echo ""
fi

echo "Installation complete!"
echo ""
echo "Usage:"
echo "  One-time analysis:  redis-restart-analyzer.sh"
echo "  Continuous monitor: redis-restart-monitor.sh"
echo ""
echo "Logs will be written to:"
echo "  /var/log/redis-restart-reasons.log"
echo "  /var/log/redis-restart-monitor.log"