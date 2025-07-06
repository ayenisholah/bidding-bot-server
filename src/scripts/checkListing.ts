import { config } from 'dotenv';
import mongoose from 'mongoose';
import Listing from '../models/listing.model';

config();

async function checkListing() {
  try {
    // Connect to MongoDB
    await mongoose.connect(process.env.MONGODB_URI as string);
    console.log('Connected to MongoDB');

    // Find the listing
    const listingId = '6868eb3d35d9cb6109867bad';
    const listing = await Listing.findById(listingId);

    if (!listing) {
      console.log(`Listing ${listingId} not found!`);
    } else {
      console.log('\n=== LISTING DETAILS ===');
      console.log('ID:', listing._id);
      console.log('Token ID:', listing.tokenId);
      console.log('Running:', listing.running);
      console.log('Selected Marketplaces:', listing.selectedMarketplaces);
      console.log('Instant Price Matching:', listing.instantPriceMatching);
      console.log('Loop Interval:', listing.loopInterval);
      console.log('Final Listing Price:', listing.finalListingPrice);
      console.log('Listing Expiration:', listing.listingExpiration);
      console.log('Instant Price Override:', listing.instantPriceOverride);
      console.log('Instant Price Matching Active Until:', listing.instantPriceMatchingActiveUntil);
      console.log('\nFull Listing Object:');
      console.log(JSON.stringify(listing.toObject(), null, 2));
    }

    // Disconnect
    await mongoose.disconnect();
    console.log('\nDisconnected from MongoDB');
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

checkListing();