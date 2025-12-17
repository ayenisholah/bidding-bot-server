/**
 * Test script for tokenHasTrait - standalone version using plain axios
 *
 * Usage: npx ts-node src/scripts/test-token-has-trait.ts
 */

import { config } from 'dotenv';
config();

import axios from 'axios';

// ============================================
// CONFIGURE TEST VARIABLES HERE
// ============================================
const CONTRACT_ADDRESS = '0x23581767a106ae21c074b2276d25e5c3e136a68b'; // moonbirds
const TOKEN_ID = '8203';
const TRAIT_TYPE = 'Background';  // optional - set to empty string to skip trait check
const TRAIT_VALUE = 'Yellow';     // optional - set to empty string to skip trait check
// ============================================

const BASE_URL = process.env.BASE_URL || 'https://nfttools.pro/kong';
const API_KEY = process.env.API_KEY;

// Debug: Check if API_KEY loaded
if (!API_KEY) {
  console.error('ERROR: API_KEY not found in environment variables');
  console.error('Make sure .env file exists and contains API_KEY=your_key');
  process.exit(1);
}
console.log(`API_KEY loaded: ${API_KEY.slice(0, 8)}...`);

const ItemAttributesQuery = `
  query ItemAttributesQuery($identifier: ItemIdentifierInput!) {
    itemByIdentifier(identifier: $identifier) {
      __typename
      ... on Item {
        id
        tokenId
        contractAddress
        attributes {
          traitType
          value
          __typename
        }
      }
    }
  }
`;

interface TokenTrait {
  trait_type: string;
  value: any;
  display_type?: string;
}

async function fetchTokenMetadata(contractAddress: string, tokenId: string): Promise<TokenTrait[] | null> {
  try {
    const response = await axios.post(
      `${BASE_URL}/opensea/graphql`,
      {
        operationName: 'ItemAttributesQuery',
        query: ItemAttributesQuery,
        variables: {
          identifier: {
            chain: 'ethereum',
            contractAddress,
            tokenId
          }
        }
      },
      {
        headers: {
          'x-api-key': API_KEY,
          'Content-Type': 'application/json'
        }
      }
    );

    // Debug: Log full response
    console.log('  Full API Response:', JSON.stringify(response.data, null, 2));

    if (!response.data?.data?.itemByIdentifier) {
      const responseKeys = Object.keys(response.data || {}).join(',') || 'empty';
      const dataKeys = Object.keys(response.data?.data || {}).join(',') || 'empty';
      console.log(`  API returned NULL item | responseKeys=[${responseKeys}] | dataKeys=[${dataKeys}]`);
      return null;
    }

    const item = response.data.data.itemByIdentifier;
    const attributes = item.attributes || [];

    return attributes.map((attr: any) => ({
      trait_type: attr.traitType,
      value: attr.value,
      display_type: attr.displayType
    }));

  } catch (error: any) {
    console.error('  API Error:', error.response?.data || error.message);
    return null;
  }
}

function tokenHasTrait(traits: TokenTrait[], traitType: string, traitValue: any): boolean {
  return traits.some(trait =>
    trait.trait_type === traitType &&
    trait.value === traitValue
  );
}

async function main() {
  console.log('='.repeat(60));
  console.log('TOKEN HAS TRAIT TEST');
  console.log('='.repeat(60));
  console.log(`Contract: ${CONTRACT_ADDRESS}`);
  console.log(`Token ID: ${TOKEN_ID}`);
  console.log(`API URL: ${BASE_URL}/opensea/graphql`);
  if (TRAIT_TYPE) {
    console.log(`Trait Type: ${TRAIT_TYPE}`);
    console.log(`Trait Value: ${TRAIT_VALUE}`);
  }
  console.log('='.repeat(60));

  // Step 1: Fetch metadata
  console.log('\n[1] Fetching token metadata from API...');
  const startTime = Date.now();
  const traits = await fetchTokenMetadata(CONTRACT_ADDRESS, TOKEN_ID);
  const elapsed = Date.now() - startTime;

  if (!traits) {
    console.log(`  FAILED - API returned null (elapsed: ${elapsed}ms)`);
    process.exit(1);
  }

  console.log(`  SUCCESS - Found ${traits.length} traits (elapsed: ${elapsed}ms)`);

  // Step 2: Display all traits
  console.log('\n[2] Token Traits:');
  if (traits.length === 0) {
    console.log('  (No traits found)');
  } else {
    traits.forEach((trait, index) => {
      console.log(`  ${index + 1}. ${trait.trait_type}: ${trait.value}`);
    });
  }

  // Step 3: Check specific trait if provided
  if (TRAIT_TYPE && TRAIT_VALUE) {
    console.log('\n[3] Checking specific trait...');
    const hasTrait = tokenHasTrait(traits, TRAIT_TYPE, TRAIT_VALUE);
    console.log(`  tokenHasTrait("${TRAIT_TYPE}", "${TRAIT_VALUE}") = ${hasTrait}`);

    if (!hasTrait) {
      const similarTraits = traits.filter(t =>
        t.trait_type.toLowerCase() === TRAIT_TYPE.toLowerCase()
      );
      if (similarTraits.length > 0) {
        console.log(`  Note: Found traits with same type but different values:`);
        similarTraits.forEach(t => {
          console.log(`    - ${t.trait_type}: ${t.value}`);
        });
      }
    }
  }

  // Step 4: Test duplicate fetch
  console.log('\n[4] Testing second fetch...');
  const startTime2 = Date.now();
  const traits2 = await fetchTokenMetadata(CONTRACT_ADDRESS, TOKEN_ID);
  const elapsed2 = Date.now() - startTime2;
  console.log(`  Second fetch: ${traits2 ? 'SUCCESS' : 'FAILED'} (elapsed: ${elapsed2}ms)`);

  console.log('\n' + '='.repeat(60));
  console.log('TEST COMPLETE');
  console.log('='.repeat(60));

  process.exit(0);
}

main();
