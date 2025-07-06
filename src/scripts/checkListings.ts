import mongoose from 'mongoose';
import { config } from 'dotenv';
import Listing from '../models/listing.model';

config();

async function checkListings() {
  try {
    // Connect to MongoDB
    await mongoose.connect(process.env.MONGODB_URI as string);
    console.log('Connected to MongoDB');

    // Find all listings
    const allListings = await Listing.find({});
    console.log(`\nTotal listings: ${allListings.length}`);

    // Find active listings
    const activeListings = await Listing.find({ running: true });
    console.log(`Active listings: ${activeListings.length}`);

    // Show details of active listings
    if (activeListings.length > 0) {
      console.log('\nActive listing details:');
      activeListings.forEach((listing, index) => {
        console.log(`\n${index + 1}. Listing ${listing._id}:`);
        console.log(`   - Token ID: ${listing.tokenId}`);
        console.log(`   - Contract: ${listing.contract?.contractAddress}`);
        console.log(`   - Running: ${listing.running}`);
        console.log(`   - Selected Marketplaces: ${listing.selectedMarketplaces.join(', ')}`);
        console.log(`   - Loop Interval: ${listing.loopInterval.value} ${listing.loopInterval.unit}`);
        console.log(`   - Instant Price Matching: ${listing.instantPriceMatching}`);
      });
    }

    // Find listings with instant price override
    const instantPriceListings = await Listing.find({ instantPriceOverride: { $exists: true } });
    console.log(`\nListings with instant price override: ${instantPriceListings.length}`);

    await mongoose.disconnect();
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

checkListings();