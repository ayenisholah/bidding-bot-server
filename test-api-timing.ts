/**
 * Test script to measure individual API call timings for collection stats
 *
 * Usage: npx ts-node test-api-timing.ts [collection-slug] [contract-address]
 *
 * Example: npx ts-node test-api-timing.ts azuki 0xed5af388653567af2f388e6224dc7c4b3241c544
 */

import axios from 'axios';
import { config } from 'dotenv';

config();

const BASE_URL = "https://nfttools.pro/kong";
const API_KEY = process.env.API_KEY;

// Test collection (Azuki by default)
const slug = process.argv[2] || 'azuki';
const contractAddress = process.argv[3] || '0xed5af388653567af2f388e6224dc7c4b3241c544';

interface TestResult {
  name: string;
  time: number;
  status: 'success' | 'error';
  responseSize?: number;
  error?: string;
  sample?: any;
}

const results: TestResult[] = [];

async function timedFetch(name: string, fn: () => Promise<any>): Promise<TestResult> {
  const start = Date.now();
  try {
    const result = await fn();
    const time = Date.now() - start;
    const responseSize = JSON.stringify(result).length;

    console.log(`\n${'='.repeat(60)}`);
    console.log(`${name}`);
    console.log(`${'='.repeat(60)}`);
    console.log(`Time: ${time}ms`);
    console.log(`Response Size: ${(responseSize / 1024).toFixed(2)} KB`);

    // Show sample of response
    if (typeof result === 'object' && result !== null) {
      const keys = Object.keys(result);
      console.log(`Response Keys: [${keys.slice(0, 10).join(', ')}${keys.length > 10 ? '...' : ''}]`);

      // For arrays, show count
      if (Array.isArray(result)) {
        console.log(`Array Length: ${result.length}`);
        if (result.length > 0) {
          console.log(`First Item Sample:`, JSON.stringify(result[0], null, 2).substring(0, 500));
        }
      } else if (result.attributes && Array.isArray(result.attributes)) {
        console.log(`Attributes Count: ${result.attributes.length}`);
        if (result.attributes.length > 0) {
          console.log(`First Attribute:`, JSON.stringify(result.attributes[0], null, 2).substring(0, 500));
        }
      } else if (result.traits) {
        const traitKeys = Object.keys(result.traits);
        console.log(`Trait Categories: ${traitKeys.length}`);
        console.log(`Categories: [${traitKeys.slice(0, 5).join(', ')}${traitKeys.length > 5 ? '...' : ''}]`);
      }
    } else {
      console.log(`Response: ${result}`);
    }

    return { name, time, status: 'success', responseSize, sample: result };
  } catch (error: any) {
    const time = Date.now() - start;
    const errorMsg = error.response?.data?.message || error.message;
    console.log(`\n${'='.repeat(60)}`);
    console.log(`${name} - ERROR`);
    console.log(`${'='.repeat(60)}`);
    console.log(`Time: ${time}ms`);
    console.log(`Error: ${errorMsg}`);
    if (error.response?.status) {
      console.log(`HTTP Status: ${error.response.status}`);
    }
    return { name, time, status: 'error', error: errorMsg };
  }
}

// 1. fetchBlurCollectionStats - Gets floor price from Blur
async function fetchBlurCollectionStats(): Promise<number> {
  const url = `${BASE_URL}/blur/v1/collections/${slug}/tokens`;
  const { data } = await axios.get(url, {
    headers: { "x-api-key": API_KEY },
    params: { filters: JSON.stringify({}) }
  });

  // Extract floor price
  if (data?.tokens?.length > 0) {
    const lowestPrice = data.tokens.reduce((min: any, token: any) => {
      const price = parseFloat(token.price?.amount || Infinity);
      return price < min ? price : min;
    }, Infinity);
    return lowestPrice === Infinity ? 0 : lowestPrice;
  }
  return data?.floorPrice || 0;
}

// 2. fetchMagicEdenCollectionStats - Gets floor price from MagicEden
async function fetchMagicEdenCollectionStats(): Promise<string> {
  const url = `${BASE_URL}/magiceden/v3/rtp/ethereum/collections/${contractAddress}/stats/v1`;
  const { data } = await axios.get(url, {
    headers: {
      accept: "application/json",
      "x-api-key": API_KEY
    }
  });
  return data?.stats?.floorAsk?.price?.amount?.decimal || '0';
}

// 3. getOpenseaFloor - Gets floor price from OpenSea
async function getOpenseaFloor(): Promise<number> {
  const url = `${BASE_URL}/opensea/api/v2/collections/${slug}/stats`;
  const { data } = await axios.get(url, {
    headers: { "x-api-key": API_KEY }
  });
  return data?.total?.floor_price || 0;
}

// 4. fetchBlurTraits - Gets trait data from Blur
async function fetchBlurTraits(): Promise<any> {
  const url = `${BASE_URL}/blur/v1/traits/${contractAddress}`;
  const { data } = await axios.get(url, {
    headers: { "x-api-key": API_KEY }
  });
  return data;
}

// 5. fetchMagicEdenAttributes - Gets trait data from MagicEden (THE BOTTLENECK)
async function fetchMagicEdenAttributes(): Promise<any> {
  const url = `${BASE_URL}/magiceden/v3/rtp/ethereum/collections/${contractAddress}/attributes/all/v4`;
  const { data } = await axios.get(url, {
    headers: {
      accept: "application/json",
      "x-api-key": API_KEY
    }
  });
  return data;
}

// 6. fetchOpenseaTraits - Gets trait data from OpenSea
async function fetchOpenseaTraits(): Promise<any> {
  const url = `${BASE_URL}/opensea/api/v2/traits/${slug}`;
  const { data } = await axios.get(url, {
    headers: { "x-api-key": API_KEY }
  });
  return data;
}

async function runTests() {
  console.log('\n');
  console.log('='.repeat(60));
  console.log('API TIMING TEST');
  console.log('='.repeat(60));
  console.log(`Collection Slug: ${slug}`);
  console.log(`Contract Address: ${contractAddress}`);
  console.log(`API Key: ${API_KEY ? API_KEY.substring(0, 8) + '...' : 'NOT SET'}`);
  console.log('='.repeat(60));

  if (!API_KEY) {
    console.error('\nERROR: API_KEY not set in environment');
    process.exit(1);
  }

  // Run tests sequentially to avoid rate limiting
  // NOTE: MagicEden APIs removed due to 504 timeouts
  const tests = [
    { name: '1. getOpenseaFloor (OS Floor)', fn: getOpenseaFloor },
    { name: '2. fetchBlurCollectionStats (Blur Floor)', fn: fetchBlurCollectionStats },
    { name: '3. fetchOpenseaTraits (OS Traits)', fn: fetchOpenseaTraits },
    { name: '4. fetchBlurTraits (Blur Traits)', fn: fetchBlurTraits },
  ];

  for (const test of tests) {
    const result = await timedFetch(test.name, test.fn);
    results.push(result);
  }

  // Summary
  console.log('\n');
  console.log('='.repeat(60));
  console.log('SUMMARY');
  console.log('='.repeat(60));

  const sortedResults = [...results].sort((a, b) => b.time - a.time);

  console.log('\nBy Time (slowest first):');
  console.log('-'.repeat(60));

  for (const r of sortedResults) {
    const status = r.status === 'success' ? '✓' : '✗';
    const size = r.responseSize ? `${(r.responseSize / 1024).toFixed(1)}KB` : '-';
    console.log(`${status} ${r.time.toString().padStart(6)}ms | ${size.padStart(10)} | ${r.name}`);
  }

  const totalTime = Math.max(...results.map(r => r.time));
  const avgTime = results.reduce((sum, r) => sum + r.time, 0) / results.length;

  console.log('-'.repeat(60));
  console.log(`Parallel Total Time: ${totalTime}ms (slowest call)`);
  console.log(`Sequential Total Time: ${results.reduce((sum, r) => sum + r.time, 0)}ms`);
  console.log(`Average Time: ${avgTime.toFixed(0)}ms`);

  const bottleneck = sortedResults[0];
  console.log(`\nBottleneck: ${bottleneck.name} (${bottleneck.time}ms)`);

  if (bottleneck.time > 10000) {
    console.log('\n⚠️  WARNING: Bottleneck is over 10 seconds!');
    console.log('Consider:');
    console.log('  - Increasing cache TTL for traits');
    console.log('  - Adding a timeout with fallback');
    console.log('  - Loading traits asynchronously');
  }
}

runTests().catch(console.error);
