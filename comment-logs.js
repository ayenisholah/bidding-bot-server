#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

// Patterns to preserve (taskStatus related)
const preservePatterns = [
  /console\.(log|error|warn).*\[TaskStatus-[A-Z]+\]/,
  /console\.(log|error|warn).*\[TaskStateManager-[A-Z]+\]/,
  /console\.(log|error|warn).*\[StateManager\]/,
  /console\.(log|error|warn).*\[EnhancedStateManager\]/,
  /console\.(log|error|warn).*setTaskStatus/,
  /console\.(log|error|warn).*taskStatus\[/,
  /console\.(log|error|warn).*Task.*transitioned/,
  /console\.(log|error|warn).*state transition/i,
  /console\.(log|error|warn).*State transition/,
  /console\.(log|error|warn).*Invalid state transition/,
  /console\.(log|error|warn).*Task Status/,
  /console\.(log|error|warn).*TRANSITION.*Task/,
  /console\.(log|error|warn).*taskStatusHistory/,
  /console\.error.*[Ee]rror/, // Keep all error logs
];

function shouldPreserveLine(line) {
  // Always preserve console.error lines
  if (line.includes('console.error')) {
    return true;
  }
  
  // Check if line matches any preserve pattern
  return preservePatterns.some(pattern => pattern.test(line));
}

function processFile(filePath) {
  console.log(`Processing: ${filePath}`);
  
  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.split('\n');
  let modified = false;
  let commentedCount = 0;
  let preservedCount = 0;
  
  const processedLines = lines.map((line, index) => {
    // Check if line contains console.log/warn/error
    if (line.match(/^\s*console\.(log|warn|error)\(/)) {
      if (shouldPreserveLine(line)) {
        preservedCount++;
        return line;
      } else {
        modified = true;
        commentedCount++;
        return '// ' + line;
      }
    }
    return line;
  });
  
  if (modified) {
    fs.writeFileSync(filePath, processedLines.join('\n'), 'utf8');
    console.log(`  - Commented: ${commentedCount} lines`);
    console.log(`  - Preserved: ${preservedCount} lines`);
  } else {
    console.log('  - No changes needed');
  }
  
  return { commented: commentedCount, preserved: preservedCount };
}

// Files to process
const filesToProcess = [
  '/Users/titan/projects/nfttools/bidding-bot/server/src/index.ts',
  '/Users/titan/projects/nfttools/bidding-bot/server/src/utils/task-state-manager.ts',
  '/Users/titan/projects/nfttools/bidding-bot/server/src/utils/enhanced-task-state-manager.ts',
  '/Users/titan/projects/nfttools/bidding-bot/server/src/init.ts',
  '/Users/titan/projects/nfttools/bidding-bot/server/src/utils/websocket-manager.ts',
  '/Users/titan/projects/nfttools/bidding-bot/server/src/marketplace/opensea/bid.ts',
  '/Users/titan/projects/nfttools/bidding-bot/server/src/marketplace/magiceden/bid.ts',
  '/Users/titan/projects/nfttools/bidding-bot/server/src/marketplace/blur/bid.ts',
];

console.log('Starting to comment out console logs...\n');

let totalCommented = 0;
let totalPreserved = 0;

filesToProcess.forEach(filePath => {
  if (fs.existsSync(filePath)) {
    const result = processFile(filePath);
    totalCommented += result.commented;
    totalPreserved += result.preserved;
  } else {
    console.log(`File not found: ${filePath}`);
  }
  console.log('');
});

console.log(`\nSummary:`);
console.log(`Total lines commented: ${totalCommented}`);
console.log(`Total lines preserved: ${totalPreserved}`);
console.log('\nDone! To restore, run: node uncomment-logs.js');