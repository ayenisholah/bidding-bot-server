import { fetchOpenSeaCollectionItems, fetchOpenseaTraits, fetchOpenseaTraitFloors } from './src/functions/collection';

const COLLECTION_SLUG = 'azuki';

async function testCaching() {
  console.log('Testing OpenSea GraphQL endpoint caching...\n');

  // Test 1: fetchOpenSeaCollectionItems
  console.log('=== Test 1: fetchOpenSeaCollectionItems ===');
  console.time('First call (cache miss)');
  const items1 = await fetchOpenSeaCollectionItems({
    collectionSlug: COLLECTION_SLUG,
    limit: 10,
    sort: { by: 'PRICE', direction: 'ASC' }
  });
  console.timeEnd('First call (cache miss)');
  console.log(`Found ${items1.length} items\n`);

  console.time('Second call (cache hit)');
  const items2 = await fetchOpenSeaCollectionItems({
    collectionSlug: COLLECTION_SLUG,
    limit: 10,
    sort: { by: 'PRICE', direction: 'ASC' }
  });
  console.timeEnd('Second call (cache hit)');
  console.log(`Found ${items2.length} items (should be same as first)\n`);

  // Test 2: fetchOpenseaTraits
  console.log('=== Test 2: fetchOpenseaTraits ===');
  console.time('First call (cache miss)');
  const traits1 = await fetchOpenseaTraits(COLLECTION_SLUG);
  console.timeEnd('First call (cache miss)');
  console.log(`Found ${traits1.length} trait types\n`);

  console.time('Second call (cache hit)');
  const traits2 = await fetchOpenseaTraits(COLLECTION_SLUG);
  console.timeEnd('Second call (cache hit)');
  console.log(`Found ${traits2.length} trait types (should be same as first)\n`);

  // Test 3: fetchOpenseaTraitFloors
  console.log('=== Test 3: fetchOpenseaTraitFloors ===');
  const testTraits = [
    { traitType: 'Background', values: ['Red', 'Blue'] },
    { traitType: 'Type', values: ['Human', 'Spirit'] }
  ];
  
  console.time('First call (cache miss)');
  const floors1 = await fetchOpenseaTraitFloors(COLLECTION_SLUG, testTraits);
  console.timeEnd('First call (cache miss)');
  console.log(`Found ${floors1.length} trait floor prices\n`);

  console.time('Second call (cache hit)');
  const floors2 = await fetchOpenseaTraitFloors(COLLECTION_SLUG, testTraits);
  console.timeEnd('Second call (cache hit)');
  console.log(`Found ${floors2.length} trait floor prices (should be same as first)\n`);

  // Test concurrent calls (locking mechanism)
  console.log('=== Test 4: Concurrent calls (testing locking) ===');
  console.time('Concurrent calls');
  const [concurrent1, concurrent2, concurrent3] = await Promise.all([
    fetchOpenseaTraits(`${COLLECTION_SLUG}-concurrent`),
    fetchOpenseaTraits(`${COLLECTION_SLUG}-concurrent`),
    fetchOpenseaTraits(`${COLLECTION_SLUG}-concurrent`)
  ]);
  console.timeEnd('Concurrent calls');
  console.log('All concurrent calls completed successfully\n');

  console.log('✅ All tests completed!');
}

// Run tests
testCaching().catch(console.error);