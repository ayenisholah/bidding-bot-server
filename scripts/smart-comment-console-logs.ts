#!/usr/bin/env node

import * as fs from 'fs';
import * as path from 'path';
import { glob } from 'glob';
import * as ts from 'typescript';

// Color codes for output
const RESET = '\x1b[0m';
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const BLUE = '\x1b[34m';
const MAGENTA = '\x1b[35m';

interface ConsoleLogInfo {
  start: number;
  end: number;
  text: string;
  hasTaskStatus: boolean;
}

/**
 * Find all console.log calls in the AST
 */
function findConsoleLogs(sourceFile: ts.SourceFile): ConsoleLogInfo[] {
  const consoleLogs: ConsoleLogInfo[] = [];
  
  function visit(node: ts.Node) {
    // Check if this is a console.log call
    if (ts.isCallExpression(node)) {
      const expression = node.expression;
      
      // Check if it's console.log
      if (ts.isPropertyAccessExpression(expression) &&
          expression.expression.getText() === 'console' &&
          expression.name.getText() === 'log') {
        
        // Get the full text of the console.log call
        const start = node.getStart();
        const end = node.getEnd();
        const text = sourceFile.text.substring(start, end);
        
        // Check if it contains 'taskStatus'
        const hasTaskStatus = text.includes('taskStatus');
        
        consoleLogs.push({
          start,
          end,
          text,
          hasTaskStatus
        });
      }
    }
    
    ts.forEachChild(node, visit);
  }
  
  visit(sourceFile);
  return consoleLogs;
}

/**
 * Comment out console.log statements
 */
function commentOutConsoleLogs(content: string, filePath: string): string {
  // Create a TypeScript source file
  const sourceFile = ts.createSourceFile(
    filePath,
    content,
    ts.ScriptTarget.Latest,
    true
  );
  
  // Find all console.log calls
  const consoleLogs = findConsoleLogs(sourceFile);
  
  // If no console.logs found, return original content
  if (consoleLogs.length === 0) {
    return content;
  }
  
  // Sort by position (descending) to process from end to start
  consoleLogs.sort((a, b) => b.start - a.start);
  
  let modifiedContent = content;
  let changesMade = 0;
  
  for (const log of consoleLogs) {
    // Skip if it contains taskStatus
    if (log.hasTaskStatus) {
      console.log(`${YELLOW}  Preserving taskStatus log at position ${log.start}${RESET}`);
      continue;
    }
    
    // Find the full statement (including semicolon and newline)
    let statementStart = log.start;
    let statementEnd = log.end;
    
    // Look backward to find the start of the line
    while (statementStart > 0 && content[statementStart - 1] !== '\n') {
      statementStart--;
    }
    
    // Look forward to include semicolon and optional newline
    while (statementEnd < content.length && 
           (content[statementEnd] === ';' || 
            content[statementEnd] === ' ' || 
            content[statementEnd] === '\t')) {
      statementEnd++;
    }
    
    // Get the statement text
    const statementText = content.substring(statementStart, statementEnd);
    
    // Comment out the entire statement
    const lines = statementText.split('\n');
    const commentedLines = lines.map((line, index) => {
      // Don't add comment to empty lines
      if (line.trim() === '') return line;
      // Add comment prefix
      return '// ' + line;
    });
    
    const commentedText = commentedLines.join('\n');
    
    // Replace in the content
    modifiedContent = 
      modifiedContent.substring(0, statementStart) + 
      commentedText + 
      modifiedContent.substring(statementEnd);
    
    changesMade++;
  }
  
  if (changesMade > 0) {
    console.log(`${GREEN}  Commented out ${changesMade} console.log statements${RESET}`);
  }
  
  return modifiedContent;
}

/**
 * Process a single file
 */
function processFile(filePath: string): void {
  try {
    console.log(`${BLUE}Processing: ${path.relative(process.cwd(), filePath)}${RESET}`);
    
    const content = fs.readFileSync(filePath, 'utf8');
    const processed = commentOutConsoleLogs(content, filePath);
    
    if (content !== processed) {
      fs.writeFileSync(filePath, processed, 'utf8');
      console.log(`${GREEN}  ✓ File updated${RESET}`);
    } else {
      console.log(`${MAGENTA}  No changes needed${RESET}`);
    }
  } catch (error) {
    console.error(`Error processing ${filePath}:`, error);
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
  
  // Process each file
  for (const file of files) {
    processFile(file);
  }
  
  console.log(`\n${GREEN}Processing complete!${RESET}`);
  
  // Build the project to verify
  console.log(`\n${BLUE}Running build to verify changes...${RESET}`);
  const { execSync } = require('child_process');
  try {
    execSync('npm run build', { stdio: 'inherit' });
    console.log(`${GREEN}Build succeeded!${RESET}`);
  } catch (error) {
    console.error(`${YELLOW}Build failed - you may need to review the changes${RESET}`);
  }
}

// Run the script
main().catch(console.error);