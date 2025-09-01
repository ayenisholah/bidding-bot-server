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
  let inMultilineConsole = false;
  let multilinePreserve = false;
  let parenCount = 0;
  
  const processedLines = lines.map((line, index) => {
    const trimmedLine = line.trim();
    
    // Check if we're starting a console statement
    if (!inMultilineConsole && line.match(/^\s*console\.(log|warn|error)\(/)) {
      inMultilineConsole = true;
      parenCount = 1; // We've seen the opening paren
      multilinePreserve = shouldPreserveLine(line);
      
      // Count parentheses in the rest of the line
      const afterConsole = line.substring(line.indexOf('(') + 1);
      for (const char of afterConsole) {
        if (char === '(') parenCount++;
        else if (char === ')') parenCount--;
      }
      
      // Check if statement completes on this line
      if (parenCount === 0) {
        inMultilineConsole = false;
        if (multilinePreserve) {
          preservedCount++;
          return line;
        } else {
          modified = true;
          commentedCount++;
          return '// ' + line;
        }
      } else {
        // Multi-line console statement started
        if (multilinePreserve) {
          return line;
        } else {
          modified = true;
          return '// ' + line;
        }
      }
    }
    
    // If we're in a multi-line console statement
    if (inMultilineConsole) {
      // Count parentheses
      for (const char of line) {
        if (char === '(') parenCount++;
        else if (char === ')') parenCount--;
      }
      
      // Check if statement ends on this line
      if (parenCount === 0) {
        inMultilineConsole = false;
        if (multilinePreserve) {
          preservedCount++;
          return line;
        } else {
          commentedCount++;
          return '// ' + line;
        }
      } else {
        // Continue multi-line
        if (multilinePreserve) {
          return line;
        } else {
          return '// ' + line;
        }
      }
    }
    
    return line;
  });
  
  if (modified) {
    fs.writeFileSync(filePath, processedLines.join('\n'), 'utf8');
    console.log(`  - Commented: ${commentedCount} statements`);
    console.log(`  - Preserved: ${preservedCount} statements`);
  } else {
    console.log('  - No changes needed');
  }
  
  return { commented: commentedCount, preserved: preservedCount };
}

// First restore all files
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

// First uncomment everything
console.log('First restoring all files...\n');
const uncommentScript = require('./uncomment-logs.js');

console.log('\nNow applying smart commenting...\n');

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
console.log(`Total statements commented: ${totalCommented}`);
console.log(`Total statements preserved: ${totalPreserved}`);
console.log('\nDone! To restore, run: node uncomment-logs.js');