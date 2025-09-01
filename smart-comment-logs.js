#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const ts = require('typescript');

// Patterns to preserve (taskStatus related)
const preservePatterns = [
  /\[TaskStatus-[A-Z]+\]/,
  /\[TaskStateManager-[A-Z]+\]/,
  /\[StateManager\]/,
  /\[EnhancedStateManager\]/,
  /setTaskStatus/,
  /taskStatus\[/,
  /Task.*transitioned/,
  /state transition/i,
  /State transition/,
  /Invalid state transition/,
  /Task Status/,
  /TRANSITION.*Task/,
  /taskStatusHistory/,
];

function shouldPreserveLog(text) {
  // Always preserve console.error
  if (text.includes('console.error')) {
    return true;
  }
  
  // Check against preserve patterns
  return preservePatterns.some(pattern => pattern.test(text));
}

function processTypeScriptFile(filePath, fileContent) {
  console.log(`Processing: ${filePath}`);
  
  // Parse the TypeScript file
  const sourceFile = ts.createSourceFile(
    filePath,
    fileContent,
    ts.ScriptTarget.Latest,
    true
  );

  let commentedCount = 0;
  let preservedCount = 0;
  const linesToComment = new Set();
  const linesToPreserve = new Set();

  // Visit each node in the AST
  function visit(node) {
    // Check if it's a console.log/warn/error call
    if (ts.isCallExpression(node) && 
        ts.isPropertyAccessExpression(node.expression) &&
        ts.isIdentifier(node.expression.expression) &&
        node.expression.expression.text === 'console' &&
        ['log', 'warn', 'error'].includes(node.expression.name.text)) {
      
      const start = sourceFile.getLineAndCharacterOfPosition(node.getStart());
      const end = sourceFile.getLineAndCharacterOfPosition(node.getEnd());
      
      // Get the full text of the console statement
      const fullText = fileContent.substring(node.getStart(), node.getEnd());
      
      if (shouldPreserveLog(fullText)) {
        // Mark all lines of this console statement as preserve
        for (let line = start.line; line <= end.line; line++) {
          linesToPreserve.add(line);
        }
        preservedCount++;
      } else {
        // Mark all lines of this console statement to comment
        for (let line = start.line; line <= end.line; line++) {
          linesToComment.add(line);
        }
        commentedCount++;
      }
    }
    
    ts.forEachChild(node, visit);
  }

  visit(sourceFile);

  // Process the file line by line
  const lines = fileContent.split('\n');
  const processedLines = lines.map((line, index) => {
    const lineNumber = index; // 0-based
    
    // Don't comment lines that should be preserved
    if (linesToPreserve.has(lineNumber)) {
      return line;
    }
    
    // Comment lines that contain console statements
    if (linesToComment.has(lineNumber)) {
      // Only add comment if line doesn't already start with //
      if (!line.trim().startsWith('//')) {
        return '//' + line;
      }
    }
    
    return line;
  });

  fs.writeFileSync(filePath, processedLines.join('\n'));
  
  console.log(`  - Commented: ${commentedCount} console statements`);
  console.log(`  - Preserved: ${preservedCount} console statements`);
  
  return { commented: commentedCount, preserved: preservedCount };
}

function processJavaScriptFile(filePath, fileContent) {
  // For JS files, use the simple line-based approach
  console.log(`Processing: ${filePath}`);
  
  const lines = fileContent.split('\n');
  let inMultilineConsole = false;
  let multilinePreserve = false;
  let parenCount = 0;
  let commentedCount = 0;
  let preservedCount = 0;
  
  const processedLines = lines.map((line) => {
    const trimmedLine = line.trim();
    
    // Check if we're starting a console statement
    if (!inMultilineConsole && line.match(/^\s*console\.(log|warn|error)\(/)) {
      inMultilineConsole = true;
      parenCount = 1;
      multilinePreserve = shouldPreserveLog(line);
      
      // Count parentheses
      const afterConsole = line.substring(line.indexOf('(') + 1);
      for (const char of afterConsole) {
        if (char === '(') parenCount++;
        else if (char === ')') parenCount--;
      }
      
      if (parenCount === 0) {
        inMultilineConsole = false;
        if (multilinePreserve) {
          preservedCount++;
          return line;
        } else {
          commentedCount++;
          return '//' + line;
        }
      } else {
        if (multilinePreserve) {
          return line;
        } else {
          return '//' + line;
        }
      }
    }
    
    // If we're in a multi-line console statement
    if (inMultilineConsole) {
      for (const char of line) {
        if (char === '(') parenCount++;
        else if (char === ')') parenCount--;
      }
      
      if (parenCount === 0) {
        inMultilineConsole = false;
        if (multilinePreserve) {
          preservedCount++;
          return line;
        } else {
          commentedCount++;
          return '//' + line;
        }
      } else {
        if (multilinePreserve) {
          return line;
        } else {
          return '//' + line;
        }
      }
    }
    
    return line;
  });
  
  fs.writeFileSync(filePath, processedLines.join('\n'));
  
  console.log(`  - Commented: ${commentedCount} console statements`);
  console.log(`  - Preserved: ${preservedCount} console statements`);
  
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

console.log('Starting smart console log commenting...\n');

let totalCommented = 0;
let totalPreserved = 0;

filesToProcess.forEach(filePath => {
  if (fs.existsSync(filePath)) {
    const fileContent = fs.readFileSync(filePath, 'utf8');
    const ext = path.extname(filePath);
    
    let result;
    if (ext === '.ts') {
      result = processTypeScriptFile(filePath, fileContent);
    } else {
      result = processJavaScriptFile(filePath, fileContent);
    }
    
    totalCommented += result.commented;
    totalPreserved += result.preserved;
  } else {
    console.log(`File not found: ${filePath}`);
  }
  console.log('');
});

console.log(`\nSummary:`);
console.log(`Total console statements commented: ${totalCommented}`);
console.log(`Total console statements preserved: ${totalPreserved}`);
console.log('\nDone! To restore, run: node uncomment-logs.js');