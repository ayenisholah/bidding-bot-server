#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

// Read both log files
const processLogFile = path.join(__dirname, 'logs', 'opensea-process-trait-bid-timing.log');
const bidLogFile = path.join(__dirname, 'logs', 'opensea-bid-timing.log');

// Parse process log entries
const processLogContent = fs.readFileSync(processLogFile, 'utf8');
const processEntries = processLogContent
  .trim()
  .split('\n')
  .filter(line => line.trim())
  .map(line => {
    try {
      return JSON.parse(line);
    } catch (error) {
      return null;
    }
  })
  .filter(entry => entry !== null);

// Parse bid log entries (filter for TRAIT bids only)
const bidLogContent = fs.readFileSync(bidLogFile, 'utf8');
const bidEntries = bidLogContent
  .trim()
  .split('\n')
  .filter(line => line.trim())
  .map(line => {
    try {
      const entry = JSON.parse(line);
      return entry.bidType === 'TRAIT' ? entry : null;
    } catch (error) {
      return null;
    }
  })
  .filter(entry => entry !== null);

console.log(`📊 OpenSea Trait Bid Complete Performance Analysis`);
console.log(`═`.repeat(80));
console.log(`Process Entries: ${processEntries.length}`);
console.log(`Bid Entries (TRAIT only): ${bidEntries.length}`);
console.log('');

// Analyze process entries
const successfulProcesses = processEntries.filter(e => e.success);
const failedProcesses = processEntries.filter(e => !e.success);

console.log(`🎯 Process Stage Analysis:`);
console.log(`  Successful processes: ${successfulProcesses.length} (${((successfulProcesses.length / processEntries.length) * 100).toFixed(1)}%)`);
console.log(`  Failed processes: ${failedProcesses.length} (${((failedProcesses.length / processEntries.length) * 100).toFixed(1)}%)`);
console.log('');

// Calculate timing for successful processes
if (successfulProcesses.length > 0) {
  const durations = successfulProcesses.map(e => e.totalDuration).sort((a, b) => a - b);
  const preBidTimes = successfulProcesses.map(e => e.bidOnOpenseaCallTime).filter(t => t !== undefined).sort((a, b) => a - b);
  
  const avgDuration = durations.reduce((sum, d) => sum + d, 0) / durations.length;
  const avgPreBid = preBidTimes.length > 0 ? preBidTimes.reduce((sum, d) => sum + d, 0) / preBidTimes.length : 0;
  
  console.log(`⏱️  Successful Process Timing:`);
  console.log(`  Average total duration: ${avgDuration.toFixed(0)}ms`);
  console.log(`  Average pre-bid time: ${avgPreBid.toFixed(0)}ms`);
  console.log(`  P95 total duration: ${durations[Math.floor(durations.length * 0.95)]}ms`);
  console.log('');
}

// Analyze bid entries
const successfulBids = bidEntries.filter(e => e.success);
const failedBids = bidEntries.filter(e => !e.success);

console.log(`🎯 Bid Execution Analysis:`);
console.log(`  Successful bids: ${successfulBids.length} (${((successfulBids.length / bidEntries.length) * 100).toFixed(1)}%)`);
console.log(`  Failed bids: ${failedBids.length} (${((failedBids.length / bidEntries.length) * 100).toFixed(1)}%)`);
console.log('');

if (successfulBids.length > 0) {
  const bidDurations = successfulBids.map(e => e.totalDuration).sort((a, b) => a - b);
  const avgBidDuration = bidDurations.reduce((sum, d) => sum + d, 0) / bidDurations.length;
  
  console.log(`⏱️  Bid Execution Timing:`);
  console.log(`  Average bid duration: ${avgBidDuration.toFixed(0)}ms`);
  console.log(`  P95 bid duration: ${bidDurations[Math.floor(bidDurations.length * 0.95)]}ms`);
}
