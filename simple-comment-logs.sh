#!/bin/bash

# Simple script to comment out console logs while preserving taskStatus related ones

echo "Starting to comment out console logs..."
echo ""

# Define files to process
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

# Create backup first
echo "Creating backup..."
for file in "${FILES[@]}"; do
  if [ -f "$file" ]; then
    cp "$file" "$file.bak"
  fi
done

# Function to comment console logs in a file
comment_logs() {
  local file=$1
  echo "Processing: $file"
  
  # Use sed to comment out console.log/warn that don't match preserve patterns
  # This is a multi-step process to handle multi-line statements
  
  # First, mark lines we want to preserve
  sed -i '' -E '
    /console\.(log|warn).*\[TaskStatus-[A-Z]+\]/b
    /console\.(log|warn).*\[TaskStateManager-[A-Z]+\]/b
    /console\.(log|warn).*\[StateManager\]/b
    /console\.(log|warn).*\[EnhancedStateManager\]/b
    /console\.(log|warn).*setTaskStatus/b
    /console\.(log|warn).*taskStatus\[/b
    /console\.(log|warn).*Task.*transitioned/b
    /console\.(log|warn).*[Ss]tate transition/b
    /console\.(log|warn).*Invalid state transition/b
    /console\.(log|warn).*Task Status/b
    /console\.(log|warn).*TRANSITION.*Task/b
    /console\.(log|warn).*taskStatusHistory/b
    /console\.error/b
    s/^( *)console\.(log|warn)\(/\1\/\/ console.\2(/
  ' "$file"
  
  echo "  - Done"
  echo ""
}

# Process each file
for file in "${FILES[@]}"; do
  if [ -f "$file" ]; then
    comment_logs "$file"
  else
    echo "File not found: $file"
    echo ""
  fi
done

echo "Summary:"
echo "All non-taskStatus console.log and console.warn statements have been commented out."
echo "All console.error statements have been preserved."
echo ""
echo "To restore, run: ./restore-logs.sh"