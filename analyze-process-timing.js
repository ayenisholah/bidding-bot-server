#!/usr/bin/env node

/**
 * OpenSea Process Token Bid Timing Analyzer
 * 
 * This script analyzes the timing logs for processOpenseaTokenBid function
 * to understand the performance from queue processing to bidOnOpensea call.
 * 
 * Usage: node analyze-process-timing.js [logfile]
 */

const fs = require('fs');
const path = require('path');

const logFile = process.argv[2] || path.join(__dirname, 'logs', 'opensea-process-token-bid-timing.log');

if (!fs.existsSync(logFile)) {
  console.error(`❌ Log file not found: ${logFile}`);
  console.error('Make sure to run some token bids first to generate timing data.');
  process.exit(1);
}

console.log(`📊 Analyzing processOpenseaTokenBid timing from: ${logFile}\n`);

// Read and parse log entries
const logContent = fs.readFileSync(logFile, 'utf8');
const logEntries = logContent
  .trim()
  .split('\n')
  .filter(line => line.trim())
  .map(line => {
    try {
      return JSON.parse(line);
    } catch (error) {
      console.warn('⚠️  Failed to parse log line:', line);
      return null;
    }
  })
  .filter(entry => entry !== null);

if (logEntries.length === 0) {
  console.log('No valid log entries found.');
  process.exit(0);
}

console.log(`📈 Found ${logEntries.length} process attempts\n`);

// Analyze overall performance
const totalStats = {
  total: logEntries.length,
  successful: 0,
  failed: 0,
  withOutbid: 0,
  withoutOutbid: 0,
  durations: [],
  bidCallTimes: []
};

const stepStats = {};

logEntries.forEach(entry => {
  const { totalDuration, success, outbidEnabled, bidOnOpenseaCallTime, steps } = entry;
  
  if (success) {
    totalStats.successful++;
    totalStats.durations.push(totalDuration);
    if (bidOnOpenseaCallTime !== undefined) {
      totalStats.bidCallTimes.push(bidOnOpenseaCallTime);
    }
  } else {
    totalStats.failed++;
  }
  
  if (outbidEnabled) {
    totalStats.withOutbid++;
  } else {
    totalStats.withoutOutbid++;
  }
  
  // Analyze step timings
  Object.entries(steps).forEach(([stepName, stepData]) => {
    if (!stepStats[stepName]) {
      stepStats[stepName] = {
        count: 0,
        totalDuration: 0,
        successful: 0,
        failed: 0,
        durations: []
      };
    }
    
    stepStats[stepName].count++;
    stepStats[stepName].totalDuration += stepData.duration;
    stepStats[stepName].durations.push(stepData.duration);
    
    if (stepData.success) {
      stepStats[stepName].successful++;
    } else {
      stepStats[stepName].failed++;
    }
  });
});

// Calculate statistics
function calculateStats(durations) {
  if (durations.length === 0) return { avg: 0, min: 0, max: 0, p95: 0 };
  
  const sorted = durations.sort((a, b) => a - b);
  const avg = durations.reduce((sum, d) => sum + d, 0) / durations.length;
  const min = sorted[0];
  const max = sorted[sorted.length - 1];
  const p95Index = Math.floor(sorted.length * 0.95);
  const p95 = sorted[p95Index] || max;
  
  return { avg, min, max, p95 };
}

const overallTiming = calculateStats(totalStats.durations);
const bidCallTiming = calculateStats(totalStats.bidCallTimes);

// Display overall summary
console.log('🎯 Overall Performance Summary:');
console.log('═'.repeat(80));
console.log(`Total Processes: ${totalStats.total}`);
console.log(`Success Rate: ${((totalStats.successful / totalStats.total) * 100).toFixed(1)}% (${totalStats.successful} successful, ${totalStats.failed} failed)`);
console.log(`Outbid Mode: ${totalStats.withOutbid} enabled, ${totalStats.withoutOutbid} disabled`);
console.log('');

if (totalStats.durations.length > 0) {
  console.log('⏱️  Total Process Duration:');
  console.log(`  Average: ${overallTiming.avg.toFixed(0)}ms`);
  console.log(`  Range: ${overallTiming.min}ms - ${overallTiming.max}ms`);
  console.log(`  P95: ${overallTiming.p95.toFixed(0)}ms`);
  console.log('');
}

if (totalStats.bidCallTimes.length > 0) {
  console.log('🚀 Time to bidOnOpensea Call:');
  console.log(`  Average: ${bidCallTiming.avg.toFixed(0)}ms`);
  console.log(`  Range: ${bidCallTiming.min}ms - ${bidCallTiming.max}ms`);
  console.log(`  P95: ${bidCallTiming.p95.toFixed(0)}ms`);
  console.log('');
}

// Display step performance
console.log('⚡ Step-by-Step Performance Analysis:');
console.log('═'.repeat(80));

const sortedSteps = Object.entries(stepStats)
  .sort(([,a], [,b]) => (b.totalDuration / b.count) - (a.totalDuration / a.count));

sortedSteps.forEach(([stepName, stats]) => {
  const avgDuration = stats.totalDuration / stats.count;
  const successRate = ((stats.successful / stats.count) * 100).toFixed(1);
  const timing = calculateStats(stats.durations);
  
  console.log(`${stepName}:`);
  console.log(`  Count: ${stats.count} | Success Rate: ${successRate}% | Avg: ${avgDuration.toFixed(0)}ms`);
  console.log(`  Range: ${timing.min}ms - ${timing.max}ms | P95: ${timing.p95.toFixed(0)}ms`);
  console.log('');
});

// Performance breakdown analysis
console.log('📊 Performance Breakdown:');
console.log('═'.repeat(80));

if (totalStats.bidCallTimes.length > 0 && totalStats.durations.length > 0) {
  const avgProcessTime = overallTiming.avg;
  const avgBidCallTime = bidCallTiming.avg;
  const preProcessingTime = avgBidCallTime;
  const processingPercentage = ((preProcessingTime / avgProcessTime) * 100).toFixed(1);
  
  console.log(`Pre-bidOnOpensea Processing: ${preProcessingTime.toFixed(0)}ms (${processingPercentage}% of total)`);
  console.log(`bidOnOpensea Execution: ${(avgProcessTime - preProcessingTime).toFixed(0)}ms (${(100 - processingPercentage).toFixed(1)}% of total)`);
  console.log('');
}

// Identify bottlenecks
console.log('🚨 Performance Issues & Bottlenecks:');
console.log('═'.repeat(80));

const slowSteps = Object.entries(stepStats)
  .filter(([, stats]) => (stats.totalDuration / stats.count) > 100) // Steps averaging > 100ms
  .sort(([,a], [,b]) => (b.totalDuration / b.count) - (a.totalDuration / a.count));

if (slowSteps.length > 0) {
  console.log('🐌 Slowest steps (>100ms average):');
  slowSteps.forEach(([stepName, stats]) => {
    const avgDuration = (stats.totalDuration / stats.count).toFixed(0);
    console.log(`  • ${stepName}: ${avgDuration}ms average`);
  });
  console.log('');
}

const failingSteps = Object.entries(stepStats)
  .filter(([, stats]) => stats.failed > 0)
  .sort(([,a], [,b]) => (b.failed / b.count) - (a.failed / a.count));

if (failingSteps.length > 0) {
  console.log('❌ Steps with failures:');
  failingSteps.forEach(([stepName, stats]) => {
    const failRate = ((stats.failed / stats.count) * 100).toFixed(1);
    console.log(`  • ${stepName}: ${stats.failed}/${stats.count} failures (${failRate}%)`);
  });
  console.log('');
}

// Outbid vs regular performance comparison
const outbidEntries = logEntries.filter(e => e.outbidEnabled && e.success);
const regularEntries = logEntries.filter(e => !e.outbidEnabled && e.success);

if (outbidEntries.length > 0 && regularEntries.length > 0) {
  console.log('⚔️  Outbid vs Regular Mode Comparison:');
  console.log('═'.repeat(80));
  
  const outbidTiming = calculateStats(outbidEntries.map(e => e.totalDuration));
  const regularTiming = calculateStats(regularEntries.map(e => e.totalDuration));
  
  console.log(`Regular Mode (${regularEntries.length} samples):`);
  console.log(`  Average: ${regularTiming.avg.toFixed(0)}ms | P95: ${regularTiming.p95.toFixed(0)}ms`);
  
  console.log(`Outbid Mode (${outbidEntries.length} samples):`);
  console.log(`  Average: ${outbidTiming.avg.toFixed(0)}ms | P95: ${outbidTiming.p95.toFixed(0)}ms`);
  
  const difference = outbidTiming.avg - regularTiming.avg;
  const percentDiff = ((difference / regularTiming.avg) * 100).toFixed(1);
  console.log(`Outbid Overhead: +${difference.toFixed(0)}ms (+${percentDiff}%)`);
  console.log('');
}

// Recent performance trends
const recentEntries = logEntries.slice(-10);
if (recentEntries.length > 0) {
  const recentSuccessRate = ((recentEntries.filter(e => e.success).length / recentEntries.length) * 100).toFixed(1);
  const recentAvgDuration = recentEntries.filter(e => e.success).length > 0 
    ? (recentEntries.filter(e => e.success).reduce((sum, e) => sum + e.totalDuration, 0) / recentEntries.filter(e => e.success).length).toFixed(0)
    : 'N/A';
  
  console.log('📈 Recent Performance (last 10 processes):');
  console.log(`  Success Rate: ${recentSuccessRate}% | Average Duration: ${recentAvgDuration}ms`);
  console.log('');
}

console.log('✅ Analysis complete!');
console.log('');
console.log('📋 Key Insights:');
console.log('• Pre-processing time = Time until bidOnOpensea call');
console.log('• Total time includes both pre-processing + bidOnOpensea execution');
console.log('• Outbid mode typically adds overhead due to fetching current offers');
console.log('• Watch for slow steps that could benefit from optimization');