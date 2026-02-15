import { API_KEY } from "..";
import { limiter, axiosInstance } from "../init";
import { BASE_URL } from "../constants";
import { errorLog } from "./debug-file-logger";
import { apiDedup } from "./api-dedup";
import { ChainValue, DEFAULT_CHAIN } from "./chains";
import {
  balanceLRU,
  trackBalanceHit,
  trackBalanceMiss
} from "../marketplace/opensea/cache";

const WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".toLowerCase();

// In-flight balance fetch tracking (Phase 4 migration - replaces distributed locks)
const balanceFetchInFlight = new Map<string, Promise<number>>();

interface BalanceResponse {
  data: {
    addressBalances: Array<{
      currency: {
        contractAddress: string;
        chain: {
          identifier: string;
        };
      };
      quantity: string;
    }>;
  };
}

/**
 * Fetch WETH balance using OpenSea's GraphQL API through Kong proxy.
 * @returns WETH balance in ETH (e.g., 0.5 for 0.5 WETH)
 */
async function fetchWethBalanceGraphQL(walletAddress: string): Promise<number> {
  const query = {
    operationName: "WethBalanceQuery",
    query: `query WethBalanceQuery($address: Address!) {
      addressBalances(address: $address) {
        currency {
          contractAddress
          chain {
            identifier
          }
        }
        quantity
      }
    }`,
    variables: { address: walletAddress }
  };

  try {
    const { data } = await apiDedup(
      `opensea-weth-balance-${walletAddress.toLowerCase()}`,
      () => limiter.schedule({ priority: 0 }, () =>
        axiosInstance.post<BalanceResponse>(
          `${BASE_URL}/opensea/graphql`,
          query,
          {
            headers: {
              'content-type': 'application/json',
              'x-api-key': API_KEY
            }
          }
        )
      ),
      { category: 'Balance' }
    );

    const wethBalance = data.data.addressBalances.find(
      (bal) =>
        bal.currency.contractAddress.toLowerCase() === WETH_ADDRESS &&
        bal.currency.chain.identifier === "ethereum"
    );

    // Quantity is already in ETH, not wei
    return wethBalance ? Number(wethBalance.quantity) : 0;

  } catch (error) {
    errorLog('Balance',
      `Error fetching WETH balance via GraphQL | wallet=${walletAddress.slice(0, 10)}...`,
      error
    );
    throw error;
  }
}

/**
 * Invalidate the balance cache for a specific wallet/token combination.
 * Called when we receive a balance error from marketplace APIs to ensure
 * the next balance check fetches fresh data.
 */
export async function invalidateBalanceCache(walletAddress: string): Promise<void> {
  const normalizedAddress = walletAddress.toLowerCase();

  try {
    balanceLRU.delete(normalizedAddress);
  } catch (error) {
    errorLog('Balance', `Failed to invalidate cache | wallet=${walletAddress.slice(0, 10)}...`, error);
  }
}

// TODO: This function currently uses Kong/OpenSea GraphQL which only supports Ethereum mainnet.
// For true multichain support, migrate to Alchemy RPC like the multi-chain branch had.
export async function fetchERC20Balance(walletAddress: string, _tokenAddress: string, _chain: ChainValue = DEFAULT_CHAIN): Promise<number> {
  // NOTE: chain parameter currently ignored - Kong/OpenSea is Ethereum-only
  const normalizedAddress = walletAddress.toLowerCase();

  // Check LRU cache (< 1ms)
  const lruCached = balanceLRU.get(normalizedAddress);
  if (lruCached !== undefined) {
    trackBalanceHit();
    return lruCached;
  }

  // Cache miss - fetch from GraphQL
  trackBalanceMiss();

  // Use in-flight Map to prevent duplicate concurrent fetches
  let inFlightPromise = balanceFetchInFlight.get(normalizedAddress);

  if (!inFlightPromise) {
    // Create new fetch operation
    inFlightPromise = (async () => {
      try {
        // Double-check LRU after starting fetch
        const lruCachedAfterStart = balanceLRU.get(normalizedAddress);
        if (lruCachedAfterStart !== undefined) {
          return lruCachedAfterStart;
        }

        // Fetch balance via GraphQL
        const balance = await fetchWethBalanceGraphQL(walletAddress);

        // Cache the result
        balanceLRU.set(normalizedAddress, balance);

        return balance;

      } catch (error) {
        errorLog('Balance',
          `Error fetching WETH balance via GraphQL | wallet=${walletAddress.slice(0, 10)}...`,
          error
        );
        throw error;
      } finally {
        // Remove from in-flight tracking
        balanceFetchInFlight.delete(normalizedAddress);
      }
    })();

    balanceFetchInFlight.set(normalizedAddress, inFlightPromise);
  }

  try {
    // Wait for fetch to complete (either new or in-progress)
    const result = await inFlightPromise;
    return result || 0;
  } catch (error) {
    // If we get a network error and no cache, return 0
    errorLog('Balance',
      `Failed to fetch WETH balance (no cached fallback) | wallet=${walletAddress.slice(0, 10)}...`,
      error
    );
    return 0;
  }
}
