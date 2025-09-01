#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { glob } = require('glob');

// Function to comment out console.log statements
function commentConsoleLogs(content) {
  // Stack to track if we're inside a console.log
  let result = '';
  let i = 0;
  
  while (i < content.length) {
    // Look for console.log
    if (content.substring(i).startsWith('console.log(')) {
      const startIndex = i;
      
      // Check if this line contains 'taskStatus'
      let endIndex = i + 12; // Length of 'console.log('
      let parenCount = 1;
      let inString = false;
      let stringChar = '';
      let hasTaskStatus = false;
      let logContent = '';
      
      // Find the matching closing parenthesis
      while (endIndex < content.length && parenCount > 0) {
        const char = content[endIndex];
        const prevChar = endIndex > 0 ? content[endIndex - 1] : '';
        
        logContent += char;
        
        // Track if we're in a string
        if (!inString && (char === '"' || char === "'" || char === '`')) {
          inString = true;
          stringChar = char;
        } else if (inString && char === stringChar && prevChar !== '\\') {
          inString = false;
        }
        
        // Count parentheses only when not in a string
        if (!inString) {
          if (char === '(') parenCount++;
          else if (char === ')') parenCount--;
        }
        
        endIndex++;
      }
      
      // Check if the full console.log contains 'taskStatus'
      const fullLog = content.substring(startIndex, endIndex);
      hasTaskStatus = fullLog.includes('taskStatus');
      
      if (hasTaskStatus) {
        // Keep taskStatus logs
        result += fullLog;
      } else {
        // Comment out the console.log
        // Handle multi-line by adding // at the beginning of each line
        const lines = fullLog.split('\n');
        const commentedLines = lines.map((line, index) => {
          if (index === 0) {
            return '// ' + line;
          } else {
            // For continuation lines, check if they're already indented
            return line.replace(/^(\s*)/, '$1// ');
          }
        });
        result += commentedLines.join('\n');
      }
      
      i = endIndex;
    } else {
      result += content[i];
      i++;
    }
  }
  
  return result;
}

// Function to process a file
function processFile(filePath) {
  try {
    const content = fs.readFileSync(filePath, 'utf8');
    const processed = commentConsoleLogs(content);
    
    if (content !== processed) {
      fs.writeFileSync(filePath, processed, 'utf8');
      console.log(`Processed: ${filePath}`);
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