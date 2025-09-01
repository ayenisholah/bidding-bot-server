#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

function processFile(filePath) {
  console.log(`Processing: ${filePath}`);
  
  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.split('\n');
  let modified = false;
  let uncommentedCount = 0;
  
  const processedLines = lines.map((line, index) => {
    // Check if line is a commented console.log/warn/error
    if (line.match(/^\s*\/\/\s*console\.(log|warn|error)\(/)) {
      modified = true;
      uncommentedCount++;
      // Remove the comment
      return line.replace(/^(\s*)\/\/\s*/, '$1');
    }
    return line;
  });
  
  if (modified) {
    fs.writeFileSync(filePath, processedLines.join('\n'), 'utf8');
    console.log(`  - Uncommented: ${uncommentedCount} lines`);
  } else {
    console.log('  - No changes needed');
  }
  
  return uncommentedCount;
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

console.log('Starting to uncomment console logs...\n');

let totalUncommented = 0;

filesToProcess.forEach(filePath => {
  if (fs.existsSync(filePath)) {
    totalUncommented += processFile(filePath);
  } else {
    console.log(`File not found: ${filePath}`);
  }
  console.log('');
});

console.log(`\nTotal lines uncommented: ${totalUncommented}`);
console.log('All console logs have been restored!');