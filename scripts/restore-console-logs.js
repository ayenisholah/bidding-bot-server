#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { glob } = require('glob');

// Function to restore console.log statements
function restoreConsoleLogs(content) {
  // Pattern to match commented console.log statements
  // This handles both single line and multi-line comments
  
  let result = content;
  
  // First, handle single-line commented console.logs
  result = result.replace(/^(\s*)\/\/\s*console\.log\(/gm, '$1console.log(');
  
  // Handle multi-line commented console.logs
  // Look for patterns where console.log starts commented and continues on next lines
  const lines = result.split('\n');
  const processedLines = [];
  let inCommentedConsoleLog = false;
  let consoleLogIndent = '';
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const trimmedLine = line.trim();
    
    // Check if this line starts a commented console.log
    if (trimmedLine.startsWith('// console.log(')) {
      inCommentedConsoleLog = true;
      // Extract the indentation
      const match = line.match(/^(\s*)\/\/\s*/);
      consoleLogIndent = match ? match[1] : '';
      // Remove the comment
      processedLines.push(line.replace(/^(\s*)\/\/\s*/, '$1'));
    } else if (inCommentedConsoleLog) {
      // Check if this line is a continuation of the console.log
      const expectedIndent = consoleLogIndent + '  '; // Assuming 2-space indent
      const lineIndentMatch = line.match(/^(\s*)\/\/\s*/);
      
      if (lineIndentMatch) {
        // This line is commented, might be part of the console.log
        const uncommentedLine = line.replace(/^(\s*)\/\/\s*/, '$1');
        processedLines.push(uncommentedLine);
        
        // Check if this line ends the console.log
        if (uncommentedLine.includes(');')) {
          inCommentedConsoleLog = false;
        }
      } else {
        // This line is not commented, so we're done with the console.log
        inCommentedConsoleLog = false;
        processedLines.push(line);
      }
    } else {
      processedLines.push(line);
    }
  }
  
  return processedLines.join('\n');
}

// Function to process a file
function processFile(filePath) {
  try {
    const content = fs.readFileSync(filePath, 'utf8');
    const processed = restoreConsoleLogs(content);
    
    if (content !== processed) {
      fs.writeFileSync(filePath, processed, 'utf8');
      console.log(`Restored: ${filePath}`);
    }
  } catch (error) {
    console.error(`Error processing ${filePath}:`, error.message);
  }
}

// Main execution
async function main() {
  const srcPath = path.join(__dirname, '..', 'src');
  
  // Find all TypeScript files
  const files = await glob('**/*.ts', {
    cwd: srcPath,
    absolute: true,
    ignore: ['**/node_modules/**', '**/dist/**']
  });
  
  console.log(`Found ${files.length} TypeScript files to process...`);
  
  // Process each file
  for (const file of files) {
    processFile(file);
  }
  
  console.log('Done!');
}

// Run the script
main().catch(console.error);