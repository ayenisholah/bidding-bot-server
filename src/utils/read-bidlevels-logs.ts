#!/usr/bin/env node

import fs from 'fs';
import path from 'path';
import readline from 'readline';

const logsDir = path.join(process.cwd(), 'logs');

// Function to read and parse log files
async function readBidLevelsLogs(dateStr?: string) {
  try {
    // If no date provided, use today
    const targetDate = dateStr || new Date().toISOString().split('T')[0];
    const logFile = path.join(logsDir, `bidLevels-${targetDate}.log`);
    
    if (!fs.existsSync(logFile)) {
      // console.log(`No log file found for date: ${targetDate}`);
      // console.log(`Available log files:`);
      
      const files = fs.readdirSync(logsDir).filter(f => f.startsWith('bidLevels-'));
      files.forEach(f => console.log(`  - ${f}`));
      return;
    }
    
    // console.log(`Reading bidLevels log for ${targetDate}...`);
    // console.log('─'.repeat(80));
    
    const fileStream = fs.createReadStream(logFile);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });
    
    let count = 0;
    for await (const line of rl) {
      count++;
      try {
        const entry = JSON.parse(line);
        // console.log(`\n[${count}] ${entry.timestamp}`);
        // console.log('Contract:', entry.message['1']?.contractAddress);
        // console.log('Price:', entry.message['1']?.updates?.[0]?.price);
        // console.log('Bidder Count:', entry.message['1']?.updates?.[0]?.bidderCount);
        // console.log('Single Bidder:', entry.message['1']?.updates?.[0]?.singleBidder);
        // console.log('Event Type:', entry.message.eventType);
        // console.log('Full Message:', JSON.stringify(entry.message, null, 2));
      } catch (err) {
        console.error(`Error parsing line ${count}:`, err);
      }
    }
    
    // console.log('\n' + '─'.repeat(80));
    // console.log(`Total entries: ${count}`);
    
  } catch (error) {
    console.error('Error reading logs:', error);
  }
}

// Function to watch logs in real-time
function watchBidLevelsLogs() {
  const dateStr = new Date().toISOString().split('T')[0];
  const logFile = path.join(logsDir, `bidLevels-${dateStr}.log`);
  
  // console.log(`Watching bidLevels log file: ${logFile}`);
  // console.log('Press Ctrl+C to stop\n');
  
  // Create file if it doesn't exist
  if (!fs.existsSync(logFile)) {
    fs.writeFileSync(logFile, '');
  }
  
  let size = fs.statSync(logFile).size;
  
  fs.watchFile(logFile, { interval: 1000 }, (curr, prev) => {
    if (curr.size > size) {
      const stream = fs.createReadStream(logFile, {
        start: size,
        end: curr.size - 1,
        encoding: 'utf8'
      });
      
      stream.on('data', (chunk) => {
        const lines = chunk.toString().split('\n').filter(l => l.trim());
        lines.forEach(line => {
          try {
            const entry = JSON.parse(line);
            // console.log(`[${new Date().toLocaleTimeString()}] New bidLevels message:`);
            // console.log('  Contract:', entry.message['1']?.contractAddress);
            // console.log('  Price:', entry.message['1']?.updates?.[0]?.price);
            // console.log('  Event:', entry.message.eventType);
            // console.log('─'.repeat(60));
          } catch (err) {
            // Ignore parse errors
          }
        });
      });
      
      size = curr.size;
    }
  });
}

// Parse command line arguments
const args = process.argv.slice(2);
const command = args[0];
const date = args[1];

if (command === 'watch') {
  watchBidLevelsLogs();
} else if (command === 'read') {
  readBidLevelsLogs(date);
} else {
  // console.log('Usage:');
  // console.log('  ts-node read-bidlevels-logs.ts read [date]     # Read logs for specific date (YYYY-MM-DD)');
  // console.log('  ts-node read-bidlevels-logs.ts watch           # Watch logs in real-time');
  // console.log('\nExamples:');
  // console.log('  ts-node read-bidlevels-logs.ts read            # Read today\'s logs');
  // console.log('  ts-node read-bidlevels-logs.ts read 2024-01-15 # Read logs for Jan 15, 2024');
  // console.log('  ts-node read-bidlevels-logs.ts watch           # Watch live logs');
}