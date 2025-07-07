#!/usr/bin/env ts-node

import { config } from "dotenv";
config();

import mongoose from "mongoose";
import WebSocketManager from "../utils/websocket-manager";
import Listing from "../models/listing.model";
import { logger, ErrorCategory } from "../utils/logger";

const MARKETPLACE_WS_URL = process.env.MARKETPLACE_WS_URL || "ws://localhost:8080";

async function checkListingSubscriptions() {
  try {
    // Connect to MongoDB
    await mongoose.connect(process.env.MONGODB_URI as string);
    console.log("✅ Connected to MongoDB");

    // Get all active listings with instant price matching
    const instantPriceMatchingListings = await Listing.find({
      running: true,
      instantPriceMatching: true
    }).select('tokenId contract selectedMarketplaces wallet');
    
    console.log(`\n📊 Found ${instantPriceMatchingListings.length} active listings with instant price matching:\n`);
    
    instantPriceMatchingListings.forEach(listing => {
      console.log(`  - Token #${listing.tokenId} (${listing.contract})`);
      console.log(`    Marketplaces: ${listing.selectedMarketplaces.join(', ')}`);
      console.log(`    Wallet: ${listing.wallet}\n`);
    });

    // Initialize WebSocket manager
    const clientId = process.env.API_KEY || process.env.CLIENT_ID || 'diagnostic-client';
    const wsManager = new WebSocketManager({
      url: MARKETPLACE_WS_URL,
      clientId: clientId,
      maxRetries: 1, // Just try once for diagnostics
      reconnectBaseDelay: 1000,
      reconnectMaxDelay: 5000,
      heartbeatInterval: 30000,
      connectionTimeout: 10000
    });

    console.log("🔌 Connecting to WebSocket server...");
    
    // Set up event handlers
    wsManager.on('connected', async (connectionId) => {
      console.log(`✅ Connected to WebSocket (ID: ${connectionId})\n`);
      
      // Wait a moment for connection to stabilize
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      // Check subscription stats
      const stats = wsManager.getListingSubscriptionStats();
      console.log("📊 Listing Subscription Stats:");
      console.log(`  - Active subscriptions: ${stats.active}`);
      console.log(`  - Persistent subscriptions: ${stats.persistent}`);
      console.log(`  - Unique collections: ${stats.slugs.length}`);
      if (stats.slugs.length > 0) {
        console.log(`  - Collections: ${stats.slugs.join(', ')}\n`);
      }
      
      // Verify subscriptions
      await wsManager.verifyListingSubscriptions();
      
      // Monitor for messages
      console.log("👂 Listening for listing price updates for 30 seconds...\n");
      
      let messageCount = 0;
      const messageTypes: { [key: string]: number } = {};
      
      wsManager.on('message', (message) => {
        if (message.subscriptionType === 'listings' || message.subscriptionType === 'both') {
          messageCount++;
          const key = `${message.marketplace}:${message.eventType}`;
          messageTypes[key] = (messageTypes[key] || 0) + 1;
          
          console.log(`📨 Received listing update #${messageCount}:`);
          console.log(`   Marketplace: ${message.marketplace}`);
          console.log(`   Event: ${message.eventType}`);
          console.log(`   Contract: ${message.contractAddress}`);
          console.log(`   Token: #${message.tokenId}`);
          console.log(`   Price: ${message.price} ETH\n`);
        }
      });
      
      // Wait 30 seconds then disconnect
      setTimeout(() => {
        console.log("\n📊 Summary after 30 seconds:");
        console.log(`  - Total listing messages received: ${messageCount}`);
        console.log(`  - Message types:`);
        Object.entries(messageTypes).forEach(([type, count]) => {
          console.log(`    - ${type}: ${count}`);
        });
        
        wsManager.disconnect();
        process.exit(0);
      }, 30000);
    });
    
    wsManager.on('disconnected', () => {
      console.log("❌ WebSocket disconnected");
    });
    
    wsManager.on('error', (error) => {
      console.error("❌ WebSocket error:", error);
    });
    
    // Connect
    await wsManager.connect();
    
  } catch (error) {
    console.error("❌ Error:", error);
    process.exit(1);
  }
}

// Run the check
checkListingSubscriptions().catch(console.error);