#!/usr/bin/env node
/* eslint-disable */

/**
 * Test script to verify OpenSea trait bid fix
 * Run this to check if the loop counter issue is resolved
 */

import { config } from 'dotenv';
import mongoose from 'mongoose';
import Task from './src/models/task.model';

config();

// Color codes
const RED = '\x1b[31m';
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const BLUE = '\x1b[34m';
const RESET = '\x1b[0m';

async function testOpenSeaFix() {
  console.log(BLUE + '🔍 Testing OpenSea Trait Bid Fix\n' + RESET);
  
  try {
    // Connect to MongoDB
    const mongoUri = process.env.MONGODB_URI || 'mongodb://localhost:27017/BIDDING_BOT';
    await mongoose.connect(mongoUri);
    console.log(GREEN + '✅ Connected to MongoDB' + RESET);
    
    // Find a task with OpenSea trait bidding enabled
    const tasks = await Task.find({
      running: true,
      selectedMarketplaces: 'OpenSea',
      'selectedTraits.opensea': { $exists: true, $ne: {} }
    }).limit(5);
    
    if (tasks.length === 0) {
      console.log(YELLOW + '⚠️  No active tasks found with OpenSea trait bidding enabled' + RESET);
      console.log('Please ensure you have:');
      console.log('  1. A task with running: true');
      console.log('  2. OpenSea in selectedMarketplaces');
      console.log('  3. Traits selected for OpenSea');
      return;
    }
    
    console.log(GREEN + `\n✅ Found ${tasks.length} tasks with OpenSea trait bidding:\n` + RESET);
    
    for (const task of tasks) {
      console.log(`Task ID: ${task._id}`);
      console.log(`Collection: ${task.contract.slug}`);
      console.log(`Loop Interval: ${task.loopInterval} seconds`);
      
      const openseaTraits = task.selectedTraits?.opensea || {};
      const traitCount = Object.values(openseaTraits).flat().length;
      console.log(`OpenSea Traits: ${traitCount} selected`);
      console.log('---');
    }
    
    console.log(BLUE + '\n📋 To test the fix:' + RESET);
    console.log('1. Start the server: npm run dev');
    console.log('2. Watch the logs for the task processing');
    console.log('3. Look for these key indicators:');
    console.log('   - Loop counter incrementing correctly (loop0, loop1, loop2, etc.)');
    console.log('   - Jobs being added successfully to the queue');
    console.log('   - Parent job tracking showing progress (X/Y completed)');
    console.log('   - Task completing at 100% (not stuck at 57.4% or 64%)');
    console.log('\n4. Monitor with: npm run test:opensea-trait-loops');
    
  } catch (error) {
    console.error(RED + 'Error:' + RESET, error);
  } finally {
    await mongoose.disconnect();
    console.log(YELLOW + '\n✅ Disconnected from MongoDB' + RESET);
  }
}

// Run the test
testOpenSeaFix().then(() => {
  process.exit(0);
}).catch(error => {
  console.error(RED + 'Script error:' + RESET, error);
  process.exit(1);
});