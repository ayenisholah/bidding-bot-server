import { listingPriceCache } from '../utils/listing-price-cache';

async function testListingCache() {
  // console.log('Testing listing price cache...\n');

  const contractAddress = '0x1234567890abcdef';
  const tokenId = '100';
  const wallet1 = '0xaaaa';
  const wallet2 = '0xbbbb';

  // Test 1: Initial price setting
  // console.log('Test 1: Setting initial price');
  const result1 = await listingPriceCache.updatePrice(contractAddress, tokenId, 1.5, wallet1);
  // console.log(`Result: ${result1}`);
  
  const price1 = await listingPriceCache.getLowestPrice(contractAddress, tokenId);
  // console.log(`Current lowest price: ${price1} ETH\n`);

  // Test 2: Try to update with higher price (should fail)
  // console.log('Test 2: Trying to update with higher price');
  const result2 = await listingPriceCache.updatePrice(contractAddress, tokenId, 2.0, wallet2);
  // console.log(`Result: ${result2}`);
  
  const price2 = await listingPriceCache.getLowestPrice(contractAddress, tokenId);
  // console.log(`Current lowest price: ${price2} ETH\n`);

  // Test 3: Update with lower price (should succeed)
  // console.log('Test 3: Updating with lower price');
  const result3 = await listingPriceCache.updatePrice(contractAddress, tokenId, 1.2, wallet2);
  // console.log(`Result: ${result3}`);
  
  const price3 = await listingPriceCache.getLowestPrice(contractAddress, tokenId);
  // console.log(`Current lowest price: ${price3} ETH\n`);

  // Test 4: Test 9 decimal precision
  // console.log('Test 4: Testing 9 decimal precision');
  const result4 = await listingPriceCache.updatePrice(contractAddress, tokenId, 1.123456789123, wallet1);
  // console.log(`Result: ${result4}`);
  
  const price4 = await listingPriceCache.getLowestPrice(contractAddress, tokenId);
  // console.log(`Current lowest price: ${price4} ETH (should be 1.123456789)\n`);

  // Test 5: Get stats
  // console.log('Test 5: Cache statistics');
  const stats = listingPriceCache.getStats();
  // console.log('Stats:', stats);

  // Clean up
  await listingPriceCache.clearPrice(contractAddress, tokenId);
  // console.log('\nTest completed and cache cleared.');
  
  process.exit(0);
}

testListingCache().catch(console.error);