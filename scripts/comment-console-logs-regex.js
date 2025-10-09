#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { glob } = require('glob');

// Color codes for output
const RESET = '\x1b[0m';
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const BLUE = '\x1b[34m';
const MAGENTA = '\x1b[35m';
const RED = '\x1b[31m';

/**
 * Comment out console.log statements using a more sophisticated regex approach
 */
function commentConsoleLogs(content, filePath) {
  const lines = content.split('\n');
  let inConsoleLog = false;
  let consoleLogStart = -1;
  let parenCount = 0;
  let hasTaskStatus = false;
  let changesMade = 0;
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    
    // Check if this line starts a console.log
    const consoleLogMatch = line.match(/^(\s*)console\.log\s*\(/);
    if (consoleLogMatch && !inConsoleLog) {
      inConsoleLog = true;
      consoleLogStart = i;
      parenCount = 1; // We already have the opening paren
      hasTaskStatus = line.includes('taskStatus');
      
      // Count parentheses in the rest of the line
      const afterMatch = line.substring(consoleLogMatch[0].length);
      for (const char of afterMatch) {
        if (char === '(') parenCount++;
        else if (char === ')') parenCount--;
      }
      
      // Check if console.log ends on this line
      if (parenCount === 0) {
        if (!hasTaskStatus) {
          lines[i] = consoleLogMatch[1] + '// ' + line.trim();
          changesMade++;
        }
        inConsoleLog = false;
      }
    } else if (inConsoleLog) {
      // We're inside a multi-line console.log
      hasTaskStatus = hasTaskStatus || line.includes('taskStatus');
      
      // Count parentheses
      for (const char of line) {
        if (char === '(') parenCount++;
        else if (char === ')') parenCount--;
      }
      
      // Check if console.log ends on this line
      if (parenCount === 0) {
        // Comment out all lines of this console.log if it doesn't have taskStatus
        if (!hasTaskStatus) {
          for (let j = consoleLogStart; j <= i; j++) {
            const lineToComment = lines[j];
            const leadingWhitespace = lineToComment.match(/^(\s*)/)[1];
            lines[j] = leadingWhitespace + '// ' + lineToComment.trim();
          }
          changesMade++;
        }
        inConsoleLog = false;
      }
    }
  }
  
  if (changesMade > 0) {
    console.log(`${GREEN}  Commented out ${changesMade} console.log statements${RESET}`);
  }
  
  return lines.join('\n');
}

/**
 * Process a single file
 */
function processFile(filePath) {
  try {
    const relativePath = path.relative(process.cwd(), filePath);
    console.log(`${BLUE}Processing: ${relativePath}${RESET}`);
    
    const content = fs.readFileSync(filePath, 'utf8');
    const processed = commentConsoleLogs(content, filePath);
    
    if (content !== processed) {
      fs.writeFileSync(filePath, processed, 'utf8');
      console.log(`${GREEN}  ✓ File updated${RESET}`);
    } else {
      console.log(`${MAGENTA}  No changes needed${RESET}`);
    }
  } catch (error) {
    console.error(`${RED}Error processing ${filePath}:${RESET}`, error.message);
  }
}

/**
 * Main execution
 */
async function main() {
  const srcPath = path.join(__dirname, '..', 'src');
  
  console.log(`${BLUE}Finding TypeScript files in ${srcPath}...${RESET}`);
  
  // Find all TypeScript files
  const files = await glob('**/*.ts', {
    cwd: srcPath,
    absolute: true,
    ignore: ['**/node_modules/**', '**/dist/**', '**/*.d.ts']
  });
  
  console.log(`${BLUE}Found ${files.length} TypeScript files to process${RESET}\n`);
  
  // Count total console.logs for summary
  let totalFiles = 0;
  let filesModified = 0;
  
  // Process each file
  for (const file of files) {
    const before = fs.readFileSync(file, 'utf8');
    processFile(file);
    const after = fs.readFileSync(file, 'utf8');
    
    totalFiles++;
    if (before !== after) {
      filesModified++;
    }
  }
  
  console.log(`\n${GREEN}Processing complete!${RESET}`);
  console.log(`${BLUE}Files processed: ${totalFiles}${RESET}`);
  console.log(`${BLUE}Files modified: ${filesModified}${RESET}`);
  
  // Build the project to verify
  console.log(`\n${BLUE}Running build to verify changes...${RESET}`);
  const { execSync } = require('child_process');
  try {
    execSync('npm run build', { stdio: 'inherit', cwd: path.join(__dirname, '..') });
    console.log(`${GREEN}✓ Build succeeded!${RESET}`);
  } catch (error) {
    console.error(`${RED}✗ Build failed - you may need to review the changes${RESET}`);
    process.exit(1);
  }
}

// Run the script
main().catch(error => {
  console.error(`${RED}Script failed:${RESET}`, error);
  process.exit(1);
});