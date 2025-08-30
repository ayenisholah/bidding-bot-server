import 'dotenv/config';
import mongoose from 'mongoose';
import { fetchTokenMetadata, getTraitFloorPrice, tokenHasTrait } from '../functions/tokenMetadata';
import { getOpenseaTraitFloor } from '../utils/opensea-trait-floor';

// Test configuration - You can modify these for your test
const TEST_CONTRACT_ADDRESS = '0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D'; // BAYC
const TEST_TOKEN_ID = '1';
const TEST_COLLECTION_SLUG = 'boredapeyachtclub';
const TEST_TRAIT_TYPE = 'Background';
const TEST_TRAIT_VALUE = 'Gray';

async function runTest() {
  try {
    console.log('=== Starting Trait Filtering Test ===\n');

    // Connect to MongoDB
    await mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/BIDDING_BOT');
    console.log('✅ Connected to MongoDB\n');

    // Test 1: Fetch token metadata
    console.log('Test 1: Fetching token metadata...');
    const metadata = await fetchTokenMetadata(TEST_CONTRACT_ADDRESS, TEST_TOKEN_ID);
    if (metadata) {
      console.log(`✅ Found ${metadata.traits.length} traits for token ${TEST_TOKEN_ID}`);
      console.log('Traits:', metadata.traits.map(t => `${t.trait_type}: ${t.value}`).join(', '));
    } else {
      console.log('❌ No metadata found');
    }
    console.log();

    // Test 2: Check if token has specific trait
    console.log('Test 2: Checking if token has specific trait...');
    const hasTrait = await tokenHasTrait(TEST_CONTRACT_ADDRESS, TEST_TOKEN_ID, TEST_TRAIT_TYPE, TEST_TRAIT_VALUE);
    console.log(`Token has trait ${TEST_TRAIT_TYPE}:${TEST_TRAIT_VALUE}? ${hasTrait ? '✅ Yes' : '❌ No'}`);
    console.log();

    // Test 3: Get trait floor price
    console.log('Test 3: Fetching trait floor price...');
    const traitFloor = await getTraitFloorPrice(TEST_COLLECTION_SLUG, TEST_TRAIT_TYPE, TEST_TRAIT_VALUE);
    if (traitFloor !== null) {
      console.log(`✅ Trait floor price: ${traitFloor} ETH`);
    } else {
      console.log('❌ No trait floor price found');
    }
    console.log();

    // Test 4: Test caching (should be faster second time)
    console.log('Test 4: Testing caching...');
    const start = Date.now();
    await getOpenseaTraitFloor(TEST_COLLECTION_SLUG, TEST_TRAIT_TYPE, TEST_TRAIT_VALUE);
    const firstTime = Date.now() - start;
    
    const start2 = Date.now();
    await getOpenseaTraitFloor(TEST_COLLECTION_SLUG, TEST_TRAIT_TYPE, TEST_TRAIT_VALUE);
    const secondTime = Date.now() - start2;
    
    console.log(`First fetch: ${firstTime}ms`);
    console.log(`Second fetch (cached): ${secondTime}ms`);
    console.log(`Speed improvement: ${Math.round((firstTime - secondTime) / firstTime * 100)}%`);
    console.log();

    console.log('=== Test completed successfully! ===');
    
  } catch (error) {
    console.error('Test failed:', error);
  } finally {
    await mongoose.disconnect();
    process.exit(0);
  }
}

// Run the test
runTest();