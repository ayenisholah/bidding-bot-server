#!/bin/bash

# Script to restore console logs from backup

echo "Restoring console logs from backup..."
echo ""

# Define files to restore
FILES=(
  "src/index.ts"
  "src/utils/task-state-manager.ts"
  "src/utils/enhanced-task-state-manager.ts"
  "src/init.ts"
  "src/utils/websocket-manager.ts"
  "src/marketplace/opensea/bid.ts"
  "src/marketplace/magiceden/bid.ts"
  "src/marketplace/blur/bid.ts"
)

# Restore each file
for file in "${FILES[@]}"; do
  if [ -f "$file.bak" ]; then
    echo "Restoring: $file"
    cp "$file.bak" "$file"
    rm "$file.bak"
  else
    echo "No backup found for: $file"
  fi
done

echo ""
echo "All files have been restored!"