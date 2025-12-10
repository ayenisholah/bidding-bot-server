/**
 * Comprehensive API Speed Test Script
 * Tests all non-bidding/listing APIs used in the bidding bot
 *
 * Usage: npx ts-node test-api-timing.ts
 */

import axios from 'axios';
import { config } from 'dotenv';

config();

const BASE_URL = "https://nfttools.pro/kong";
const API_KEY = process.env.API_KEY;

// Test parameters
const WALLET_ADDRESS = '0x1b8afd0de8a9368ce635be8b9572c65d8f590b21';
const CONTRACT_ADDRESS = '0x48c7f48622a2c31fc72b326332cf1e2357c8ef30';
const TOKEN_ID = '4427';
const SLUG = 'say-gm';

interface TestResult {
  name: string;
  category: string;
  endpoint: string;
  time: number;
  status: 'success' | 'error' | 'timeout';
  responseSize?: number;
  error?: string;
  httpStatus?: number;
}

const results: TestResult[] = [];

async function timedFetch(name: string, category: string, endpoint: string, fn: () => Promise<any>, timeoutMs = 30000): Promise<TestResult> {
  const start = Date.now();

  try {
    const timeoutPromise = new Promise((_, reject) =>
      setTimeout(() => reject(new Error('TIMEOUT')), timeoutMs)
    );

    const result = await Promise.race([fn(), timeoutPromise]);
    const time = Date.now() - start;
    const responseSize = JSON.stringify(result).length;

    const timeColor = time < 1000 ? '\x1b[32m' : time < 3000 ? '\x1b[33m' : '\x1b[31m';
    console.log(`${timeColor}✓\x1b[0m ${time.toString().padStart(5)}ms | ${(responseSize / 1024).toFixed(1).padStart(7)}KB | ${name}`);
    console.log(`  \x1b[90m→ ${endpoint}\x1b[0m`);

    return { name, category, endpoint, time, status: 'success', responseSize };
  } catch (error: any) {
    const time = Date.now() - start;
    const isTimeout = error.message === 'TIMEOUT';
    const errorMsg = isTimeout ? 'TIMEOUT' : String(error.response?.data?.message || error.message || 'Unknown error');
    const httpStatus = error.response?.status;

    console.log(`\x1b[31m✗\x1b[0m ${time.toString().padStart(5)}ms | ${String(httpStatus || 'N/A').padStart(7)} | ${name} - ${errorMsg.substring(0, 50)}`);
    console.log(`  \x1b[90m→ ${endpoint}\x1b[0m`);

    return { name, category, endpoint, time, status: isTimeout ? 'timeout' : 'error', error: errorMsg, httpStatus };
  }
}

// ============================================================================
// OPENSEA REST APIs
// ============================================================================

async function openseaGetCollection() {
  const url = `${BASE_URL}/opensea/api/v2/collections/${SLUG}`;
  const { data } = await axios.get(url, {
    headers: { 'x-api-key': API_KEY }
  });
  return data;
}

async function openseaGetCollectionStats() {
  const url = `${BASE_URL}/opensea/api/v2/collections/${SLUG}/stats`;
  const { data } = await axios.get(url, {
    headers: { 'x-api-key': API_KEY }
  });
  return data;
}

async function openseaGetTraits() {
  const url = `${BASE_URL}/opensea/api/v2/traits/${SLUG}`;
  const { data } = await axios.get(url, {
    headers: { 'x-api-key': API_KEY }
  });
  return data;
}

async function openseaGetBestListings() {
  const url = `${BASE_URL}/opensea/api/v2/listings/collection/${SLUG}/best?limit=3`;
  const { data } = await axios.get(url, {
    headers: { 'x-api-key': API_KEY }
  });
  return data;
}

async function openseaGetCollectionEvents() {
  const url = `${BASE_URL}/opensea/api/v2/events/collection/${SLUG}`;
  const { data } = await axios.get(url, {
    headers: { 'x-api-key': API_KEY },
    params: { event_type: 'sale', limit: 10 }
  });
  return data;
}

async function openseaGetOffers() {
  const url = `${BASE_URL}/opensea/api/v2/orders/ethereum/seaport/offers`;
  const { data } = await axios.get(url, {
    headers: { 'x-api-key': API_KEY },
    params: {
      asset_contract_address: CONTRACT_ADDRESS,
      token_ids: TOKEN_ID,
      limit: 5
    }
  });
  return data;
}

// ============================================================================
// OPENSEA GRAPHQL APIs
// ============================================================================

async function openseaGraphqlWethBalance() {
  const query = {
    operationName: "WethBalanceQuery",
    query: `query WethBalanceQuery($address: Address!) {
      addressBalances(address: $address) {
        currency {
          contractAddress
          chain { identifier }
        }
        quantity
      }
    }`,
    variables: { address: WALLET_ADDRESS }
  };

  const { data } = await axios.post(`${BASE_URL}/opensea/graphql`, query, {
    headers: { 'content-type': 'application/json', 'x-api-key': API_KEY }
  });
  return data;
}

async function openseaGraphqlTokenMetadata() {
  const query = {
    operationName: "ItemAttributesQuery",
    query: `query ItemAttributesQuery($contractAddress: String!, $tokenId: String!) {
      contract(address: $contractAddress) {
        address
        name
        nft(tokenId: $tokenId) {
          tokenId
          tokenUrl
          tokenMetadata {
            name
            description
            attributes { traitType, value, displayType }
          }
        }
      }
    }`,
    variables: { contractAddress: CONTRACT_ADDRESS, tokenId: TOKEN_ID }
  };

  const { data } = await axios.post(`${BASE_URL}/opensea/graphql`, query, {
    headers: { 'content-type': 'application/json', 'x-api-key': API_KEY }
  });
  return data;
}

async function openseaGraphqlCollectionTraits() {
  const query = {
    operationName: "CollectionTraitsQuery",
    query: `query CollectionTraitsQuery($slug: String!, $limit: Int!, $cursor: String) {
      collectionBySlug(slug: $slug) {
        slug
        attributes(cursor: $cursor, limit: $limit) {
          items {
            key
            values {
              value
              count
              floorPrice { unit, symbol, usd }
            }
          }
          nextPageCursor
        }
      }
    }`,
    variables: { slug: SLUG, limit: 100, cursor: null }
  };

  const { data } = await axios.post(`${BASE_URL}/opensea/graphql`, query, {
    headers: { 'content-type': 'application/json', 'x-api-key': API_KEY }
  });
  return data;
}

async function openseaGraphqlCollectionOffers() {
  const query = {
    operationName: "CollectionOfferAggregatesQuery",
    query: `query CollectionOfferAggregatesQuery($collectionSlug: String!, $sort: CollectionOfferAggregateSort!, $cursor: String, $limit: Int!) {
      collectionOfferAggregates(collectionSlug: $collectionSlug, sort: $sort, cursor: $cursor, limit: $limit) {
        items {
          totalOffers
          offerPrice { usd, token { unit, symbol } }
          bidders { address }
        }
        nextPageCursor
      }
    }`,
    variables: { collectionSlug: SLUG, limit: 5, cursor: null, sort: { by: "OFFER_PRICE", direction: "DESC" } }
  };

  const { data } = await axios.post(`${BASE_URL}/opensea/graphql`, query, {
    headers: { 'content-type': 'application/json', 'x-api-key': API_KEY }
  });
  return data;
}

async function openseaGraphqlItemOffers() {
  const query = {
    operationName: "ItemOffersQuery",
    query: `query ItemOffersQuery($filter: ItemOfferFilter!, $identifier: ItemIdentifier!, $limit: Int!) {
      itemOffers(filter: $filter, identifier: $identifier, limit: $limit) {
        items {
          id
          pricePerItem { usd, token { unit, symbol } }
          maker { address }
          endTime
        }
      }
    }`,
    variables: {
      filter: { chain: "ethereum", contractAddress: CONTRACT_ADDRESS, tokenId: TOKEN_ID },
      identifier: { chain: "ethereum", contractAddress: CONTRACT_ADDRESS, tokenId: TOKEN_ID },
      limit: 5
    }
  };

  const { data } = await axios.post(`${BASE_URL}/opensea/graphql`, query, {
    headers: { 'content-type': 'application/json', 'x-api-key': API_KEY }
  });
  return data;
}

async function openseaGraphqlRarityTokens() {
  const query = {
    operationName: "CollectionItemsQuery",
    query: `query CollectionItemsQuery($collectionSlug: String!, $cursor: String, $limit: Int!, $sort: CollectionItemsSort) {
      collectionItems(collectionSlug: $collectionSlug, cursor: $cursor, limit: $limit, sort: $sort) {
        items {
          tokenId
          rarity { category, rank }
        }
        nextPageCursor
      }
    }`,
    variables: { collectionSlug: SLUG, limit: 20, cursor: null, sort: { by: "RARITY", direction: "DESC" } }
  };

  const { data } = await axios.post(`${BASE_URL}/opensea/graphql`, query, {
    headers: { 'content-type': 'application/json', 'x-api-key': API_KEY }
  });
  return data;
}

async function openseaGraphqlProfileItems() {
  const query = {
    operationName: "ProfileItemsQuery",
    query: `query ProfileItemsQuery($owner: Address!, $cursor: String, $limit: Int!) {
      profileItems(owner: $owner, cursor: $cursor, limit: $limit) {
        items {
          tokenId
          name
          chain { name }
          standard
          collection { slug, name }
          attributes { traitType, value, floorPrice { unit } }
        }
        nextPageCursor
      }
    }`,
    variables: { owner: WALLET_ADDRESS, limit: 10, cursor: null }
  };

  const { data } = await axios.post(`${BASE_URL}/opensea/graphql`, query, {
    headers: { 'content-type': 'application/json', 'x-api-key': API_KEY }
  });
  return data;
}

// ============================================================================
// BLUR APIs
// ============================================================================

async function blurGetCollectionTokens() {
  const url = `${BASE_URL}/blur/v1/collections/${SLUG}/tokens`;
  const { data } = await axios.get(url, {
    headers: { 'x-api-key': API_KEY },
    params: { filters: JSON.stringify({}) }
  });
  return data;
}

async function blurGetTraits() {
  const url = `${BASE_URL}/blur/v1/traits/${CONTRACT_ADDRESS}`;
  const { data } = await axios.get(url, {
    headers: { 'x-api-key': API_KEY }
  });
  return data;
}

async function blurGetExecutableBids() {
  const url = `${BASE_URL}/blur/v1/collections/${SLUG}/executable-bids`;
  const { data } = await axios.get(url, {
    headers: { 'x-api-key': API_KEY }
  });
  return data;
}

async function blurGetTraitFloor() {
  const filters = {
    traits: [{ type: "address count", values: ["7"] }],
    hasAsks: true,
  };
  const url = `${BASE_URL}/blur/v1/collections/${SLUG}/tokens?filters=${encodeURIComponent(JSON.stringify(filters))}`;
  const { data } = await axios.get(url, {
    headers: { 'x-api-key': API_KEY },
    timeout: 60000
  });
  return data;
}

// ============================================================================
// MAGICEDEN APIs
// ============================================================================

async function magicedenGetCollectionStats() {
  const url = `${BASE_URL}/magiceden/v3/rtp/ethereum/collections/${CONTRACT_ADDRESS}/stats/v1`;
  const { data } = await axios.get(url, {
    headers: { accept: 'application/json', 'x-api-key': API_KEY }
  });
  return data;
}

async function magicedenGetAttributes() {
  const url = `${BASE_URL}/magiceden/v3/rtp/ethereum/collections/${CONTRACT_ADDRESS}/attributes/all/v4`;
  const { data } = await axios.get(url, {
    headers: { accept: 'application/json', 'x-api-key': API_KEY }
  });
  return data;
}

async function magicedenGetCollectionBids() {
  const url = `${BASE_URL}/magiceden/v3/rtp/ethereum/collections/${CONTRACT_ADDRESS}/bids/v1?type=collection&sortBy=price&limit=5`;
  const { data } = await axios.get(url, {
    headers: { 'content-type': 'application/json', 'x-api-key': API_KEY }
  });
  return data;
}

async function magicedenGetTokenBids() {
  const url = `${BASE_URL}/magiceden/v3/rtp/ethereum/orders/bids/v6?token=${CONTRACT_ADDRESS}:${TOKEN_ID}&sortBy=price&status=active&limit=5`;
  const { data } = await axios.get(url, {
    headers: { 'content-type': 'application/json', 'x-api-key': API_KEY }
  });
  return data;
}

async function magicedenGetTokensV7() {
  const url = `${BASE_URL}/magiceden/v3/rtp/ethereum/tokens/v7?collection=${CONTRACT_ADDRESS}&limit=5`;
  const { data } = await axios.get(url, {
    headers: { 'x-api-key': API_KEY }
  });
  return data;
}

async function magicedenGetAssets() {
  const url = `${BASE_URL}/magiceden/v4/assets`;
  const { data } = await axios.get(url, {
    headers: { 'x-api-key': API_KEY },
    params: {
      chain: 'ethereum',
      collectionId: CONTRACT_ADDRESS,
      includeTopBid: false,
      offset: 0
    }
  });
  return data;
}

async function magicedenGetCollections() {
  const { data } = await axios.post(`${BASE_URL}/magiceden/v4/collections`, {
    chain: "ethereum",
    slug: SLUG,
  }, {
    headers: { 'content-type': 'application/json', 'x-api-key': API_KEY }
  });
  return data;
}

// ============================================================================
// RUN TESTS
// ============================================================================

async function runTests() {
  console.log('\n');
  console.log('═'.repeat(80));
  console.log('  COMPREHENSIVE API SPEED TEST');
  console.log('═'.repeat(80));
  console.log(`  Wallet:   ${WALLET_ADDRESS}`);
  console.log(`  Contract: ${CONTRACT_ADDRESS}`);
  console.log(`  Token ID: ${TOKEN_ID}`);
  console.log(`  Slug:     ${SLUG}`);
  console.log(`  API Key:  ${API_KEY ? API_KEY.substring(0, 8) + '...' : 'NOT SET'}`);
  console.log('═'.repeat(80));

  if (!API_KEY) {
    console.error('\n\x1b[31mERROR: API_KEY not set in environment\x1b[0m');
    process.exit(1);
  }

  // OpenSea REST APIs
  console.log('\n\x1b[36m── OPENSEA REST APIs ──\x1b[0m');
  results.push(await timedFetch('OS: Get Collection', 'opensea-rest', `GET /opensea/api/v2/collections/${SLUG}`, openseaGetCollection));
  results.push(await timedFetch('OS: Get Collection Stats', 'opensea-rest', `GET /opensea/api/v2/collections/${SLUG}/stats`, openseaGetCollectionStats));
  results.push(await timedFetch('OS: Get Traits (REST)', 'opensea-rest', `GET /opensea/api/v2/traits/${SLUG}`, openseaGetTraits));
  results.push(await timedFetch('OS: Get Best Listings', 'opensea-rest', `GET /opensea/api/v2/listings/collection/${SLUG}/best`, openseaGetBestListings));
  results.push(await timedFetch('OS: Get Collection Events', 'opensea-rest', `GET /opensea/api/v2/events/collection/${SLUG}`, openseaGetCollectionEvents));
  results.push(await timedFetch('OS: Get Offers (REST)', 'opensea-rest', `GET /opensea/api/v2/orders/ethereum/seaport/offers?token=${CONTRACT_ADDRESS}:${TOKEN_ID}`, openseaGetOffers));

  // OpenSea GraphQL APIs
  console.log('\n\x1b[36m── OPENSEA GRAPHQL APIs ──\x1b[0m');
  results.push(await timedFetch('OS GQL: WETH Balance', 'opensea-graphql', `POST /opensea/graphql [WethBalanceQuery]`, openseaGraphqlWethBalance));
  results.push(await timedFetch('OS GQL: Token Metadata', 'opensea-graphql', `POST /opensea/graphql [ItemAttributesQuery] token=${CONTRACT_ADDRESS}:${TOKEN_ID}`, openseaGraphqlTokenMetadata));
  results.push(await timedFetch('OS GQL: Collection Traits', 'opensea-graphql', `POST /opensea/graphql [CollectionTraitsQuery] slug=${SLUG}`, openseaGraphqlCollectionTraits));
  results.push(await timedFetch('OS GQL: Collection Offers', 'opensea-graphql', `POST /opensea/graphql [CollectionOfferAggregatesQuery] slug=${SLUG}`, openseaGraphqlCollectionOffers));
  results.push(await timedFetch('OS GQL: Item Offers', 'opensea-graphql', `POST /opensea/graphql [ItemOffersQuery] token=${CONTRACT_ADDRESS}:${TOKEN_ID}`, openseaGraphqlItemOffers));
  results.push(await timedFetch('OS GQL: Rarity Tokens', 'opensea-graphql', `POST /opensea/graphql [CollectionItemsQuery] slug=${SLUG}`, openseaGraphqlRarityTokens));
  results.push(await timedFetch('OS GQL: Profile Items (NFTs)', 'opensea-graphql', `POST /opensea/graphql [ProfileItemsQuery] owner=${WALLET_ADDRESS.slice(0,10)}...`, openseaGraphqlProfileItems));

  // Blur APIs
  console.log('\n\x1b[36m── BLUR APIs ──\x1b[0m');
  results.push(await timedFetch('Blur: Get Collection Tokens (Floor)', 'blur', `GET /blur/v1/collections/${SLUG}/tokens`, blurGetCollectionTokens));
  results.push(await timedFetch('Blur: Get Traits', 'blur', `GET /blur/v1/traits/${CONTRACT_ADDRESS}`, blurGetTraits));
  results.push(await timedFetch('Blur: Get Executable Bids', 'blur', `GET /blur/v1/collections/${SLUG}/executable-bids`, blurGetExecutableBids));
  results.push(await timedFetch('Blur: Get Trait Floor', 'blur', `GET /blur/v1/collections/${SLUG}/tokens?filters={traits}`, blurGetTraitFloor, 60000));

  // MagicEden APIs
  console.log('\n\x1b[36m── MAGICEDEN APIs ──\x1b[0m');
  results.push(await timedFetch('ME: Get Collection Stats', 'magiceden', `GET /magiceden/v3/rtp/ethereum/collections/${CONTRACT_ADDRESS}/stats/v1`, magicedenGetCollectionStats));
  results.push(await timedFetch('ME: Get Attributes', 'magiceden', `GET /magiceden/v3/rtp/ethereum/collections/${CONTRACT_ADDRESS}/attributes/all/v4`, magicedenGetAttributes));
  results.push(await timedFetch('ME: Get Collection Bids', 'magiceden', `GET /magiceden/v3/rtp/ethereum/collections/${CONTRACT_ADDRESS}/bids/v1`, magicedenGetCollectionBids));
  results.push(await timedFetch('ME: Get Token Bids', 'magiceden', `GET /magiceden/v3/rtp/ethereum/orders/bids/v6?token=${CONTRACT_ADDRESS}:${TOKEN_ID}`, magicedenGetTokenBids));
  results.push(await timedFetch('ME: Get Tokens v7', 'magiceden', `GET /magiceden/v3/rtp/ethereum/tokens/v7?collection=${CONTRACT_ADDRESS}`, magicedenGetTokensV7));
  results.push(await timedFetch('ME: Get Assets v4', 'magiceden', `GET /magiceden/v4/assets?collectionId=${CONTRACT_ADDRESS}`, magicedenGetAssets));
  results.push(await timedFetch('ME: Get Collections v4', 'magiceden', `POST /magiceden/v4/collections slug=${SLUG}`, magicedenGetCollections));

  // ══════════════════════════════════════════════════════════════════════════
  // SUMMARY
  // ══════════════════════════════════════════════════════════════════════════
  console.log('\n');
  console.log('═'.repeat(80));
  console.log('  SUMMARY');
  console.log('═'.repeat(80));

  const successful = results.filter(r => r.status === 'success');
  const failed = results.filter(r => r.status === 'error');
  const timedOut = results.filter(r => r.status === 'timeout');

  console.log(`\n  Total: ${results.length} | ✓ Success: ${successful.length} | ✗ Failed: ${failed.length} | ⏱ Timeout: ${timedOut.length}`);

  // By category
  console.log('\n\x1b[36m── BY CATEGORY ──\x1b[0m');
  const categories = [...new Set(results.map(r => r.category))];
  for (const cat of categories) {
    const catResults = results.filter(r => r.category === cat && r.status === 'success');
    if (catResults.length === 0) {
      console.log(`  ${cat}: No successful requests`);
      continue;
    }
    const avgTime = catResults.reduce((sum, r) => sum + r.time, 0) / catResults.length;
    const maxTime = Math.max(...catResults.map(r => r.time));
    const minTime = Math.min(...catResults.map(r => r.time));
    console.log(`  ${cat.padEnd(18)} | avg: ${avgTime.toFixed(0).padStart(5)}ms | min: ${minTime.toString().padStart(5)}ms | max: ${maxTime.toString().padStart(5)}ms`);
  }

  // Slowest APIs
  console.log('\n\x1b[36m── SLOWEST APIs (Top 10) ──\x1b[0m');
  const sortedByTime = [...successful].sort((a, b) => b.time - a.time).slice(0, 10);
  for (const r of sortedByTime) {
    const timeColor = r.time < 1000 ? '\x1b[32m' : r.time < 3000 ? '\x1b[33m' : '\x1b[31m';
    console.log(`  ${timeColor}${r.time.toString().padStart(5)}ms\x1b[0m | ${r.name}`);
    console.log(`    \x1b[90m→ ${r.endpoint}\x1b[0m`);
  }

  // Fastest APIs
  console.log('\n\x1b[36m── FASTEST APIs (Top 5) ──\x1b[0m');
  const sortedByTimeFastest = [...successful].sort((a, b) => a.time - b.time).slice(0, 5);
  for (const r of sortedByTimeFastest) {
    console.log(`  \x1b[32m${r.time.toString().padStart(5)}ms\x1b[0m | ${r.name}`);
    console.log(`    \x1b[90m→ ${r.endpoint}\x1b[0m`);
  }

  // Failed APIs
  if (failed.length > 0 || timedOut.length > 0) {
    console.log('\n\x1b[36m── FAILED/TIMEOUT APIs ──\x1b[0m');
    for (const r of [...failed, ...timedOut]) {
      console.log(`  \x1b[31m${r.status.toUpperCase().padEnd(7)}\x1b[0m | ${r.name} | ${r.error?.substring(0, 60)}`);
      console.log(`    \x1b[90m→ ${r.endpoint}\x1b[0m`);
    }
  }

  // Total time estimates
  const totalSequential = results.filter(r => r.status === 'success').reduce((sum, r) => sum + r.time, 0);
  const maxParallel = Math.max(...results.filter(r => r.status === 'success').map(r => r.time));

  console.log('\n\x1b[36m── TIMING ESTIMATES ──\x1b[0m');
  console.log(`  Sequential (all APIs):    ${(totalSequential / 1000).toFixed(2)}s`);
  console.log(`  Parallel (slowest wins):  ${(maxParallel / 1000).toFixed(2)}s`);

  // Warnings
  const slowApis = successful.filter(r => r.time > 5000);
  if (slowApis.length > 0) {
    console.log('\n\x1b[33m⚠ WARNING: APIs taking > 5 seconds:\x1b[0m');
    for (const r of slowApis) {
      console.log(`  - ${r.name} (${r.time}ms)`);
    }
  }

  console.log('\n' + '═'.repeat(80) + '\n');
}

runTests().catch(console.error);
