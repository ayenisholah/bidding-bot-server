import { ethers } from "ethers";
import { SEAPORT_CONTRACT_ADDRESS, SEAPORT_SPENDER_ADDRESS } from "../constants";

export const CHAIN_OPTIONS = [
  { label: "Abstract", value: "abstract", logo: "/chains/abstract.svg" },
  { label: "Ape Chain", value: "ape_chain", logo: "/chains/ape_chain.svg" },
  { label: "Arbitrum", value: "arbitrum", logo: "/chains/arbitrum.png" },
  {
    label: "Arbitrum Nova",
    value: "arbitrum_nova",
    logo: "/chains/arbitrum_nova.svg",
  },
  { label: "Avalanche", value: "avalanche", logo: "/chains/avalanche.png" },
  { label: "B3", value: "b3", logo: "/chains/b3.svg" },
  { label: "Base", value: "base", logo: "/chains/base.png" },
  { label: "Bera Chain", value: "bera_chain", logo: "/chains/bera_chain.svg" },
  { label: "Blast", value: "blast", logo: "/chains/blast.png" },
  { label: "Ethereum", value: "ethereum", logo: "/chains/ethereum.png" },
  { label: "Flow", value: "flow", logo: "/chains/flow.svg" },
  { label: "Klaytn", value: "klaytn", logo: "/chains/klaytn.png" },
  { label: "Matic", value: "matic", logo: "/chains/matic.png" },
  { label: "MegaETH", value: "megaeth", logo: "/chains/megaeth.svg" },
  { label: "Optimism", value: "optimism", logo: "/chains/optimism.png" },
  { label: "Ronin", value: "ronin", logo: "/chains/ronin.png" },
  { label: "Shape", value: "shape", logo: "/chains/shape.svg" },
  { label: "Solana", value: "solana", logo: "/chains/solana.png" },
  { label: "Soneium", value: "soneium", logo: "/chains/soneium.svg" },
  { label: "Unichain", value: "unichain", logo: "/chains/unichain.svg" },
  { label: "Zora", value: "zora", logo: "/chains/zora.svg" },
] as const;

export type ChainOption = (typeof CHAIN_OPTIONS)[number];

export type ChainValue = ChainOption["value"];

export const DEFAULT_CHAIN: ChainValue = "ethereum";

export const CHAIN_BY_VALUE: Record<ChainValue, ChainOption> =
  CHAIN_OPTIONS.reduce((acc, option) => {
    acc[option.value] = option;
    return acc;
  }, {} as Record<ChainValue, ChainOption>);

type MarketplaceKey = "opensea" | "magiceden" | "blur";

interface ChainCurrencyConfig {
  symbol: string;
  address: string;
  decimals: number;
}

interface ChainMarketplaceConfig {
  [marketplace: string]: string | undefined;
}

interface ChainMarketplaceSpenderConfig {
  [marketplace: string]: string | undefined;
}

interface ChainProviderConfig {
  chainId: number;
  alchemyNetwork?: string;
  rpcUrlEnv?: string;
}

export interface ChainConfig {
  value: ChainValue;
  label: string;
  provider: ChainProviderConfig;
  currencies: Record<string, ChainCurrencyConfig>;
  marketplaces: ChainMarketplaceConfig;
  apiIdentifiers?: Partial<Record<MarketplaceKey, string>>;
  marketplaceSpenders?: ChainMarketplaceSpenderConfig;
}

const CHAIN_CONFIGS: Partial<Record<ChainValue, ChainConfig>> = {
  ethereum: {
    value: "ethereum",
    label: "Ethereum",
    provider: {
      chainId: 1,
      alchemyNetwork: "mainnet",
    },
    currencies: {
      weth: {
        symbol: "WETH",
        address: "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        decimals: 18,
      },
      usdc: {
        symbol: "USDC",
        address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        decimals: 6,
      },
      beth: {
        symbol: "BETH",
        address: "0x0000000000A39bb272e79075ade125fd351887Ac",
        decimals: 18,
      },
    },
    marketplaces: {
      opensea: SEAPORT_CONTRACT_ADDRESS,
      magiceden: "0x9A1D00bEd7CD04BCDA516d721A596eb22Aac6834",
    },
    apiIdentifiers: {
      opensea: "ethereum",
    },
    marketplaceSpenders: {
      opensea: SEAPORT_SPENDER_ADDRESS,
    },
  },
  base: {
    value: "base",
    label: "Base",
    provider: {
      chainId: 8453,
      alchemyNetwork: "base-mainnet",
    },
    currencies: {
      weth: {
        symbol: "WETH",
        address: "0x4200000000000000000000000000000000000006",
        decimals: 18,
      },
      usdc: {
        symbol: "USDC",
        address: "0x833589fCD6eDb6E08f4c7C12D06cC2fC9e74E3B0",
        decimals: 6,
      },
    },
    marketplaces: {
      opensea: SEAPORT_CONTRACT_ADDRESS,
    },
    apiIdentifiers: {
      opensea: "base",
    },
    marketplaceSpenders: {
      opensea: SEAPORT_SPENDER_ADDRESS,
    },
  },
  arbitrum: {
    value: "arbitrum",
    label: "Arbitrum",
    provider: {
      chainId: 42161,
      alchemyNetwork: "arb-mainnet",
      rpcUrlEnv: "ARBITRUM_RPC_URL",
    },
    currencies: {
      weth: {
        symbol: "WETH",
        address: "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        decimals: 18,
      },
      usdc: {
        symbol: "USDC",
        // Native USDC on Arbitrum
        address: "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        decimals: 6,
      },
    },
    marketplaces: {
      opensea: SEAPORT_CONTRACT_ADDRESS,
    },
    apiIdentifiers: {
      opensea: "arbitrum",
    },
    marketplaceSpenders: {
      opensea: SEAPORT_SPENDER_ADDRESS,
    },
  },
  abstract: {
    value: "abstract",
    label: "Abstract",
    provider: {
      chainId: 2741,
      alchemyNetwork: "abstract-mainnet",
    },
    currencies: {
      weth: {
        symbol: "WETH",
        address: "0x3439153eb7af838ad19d56e1571fbd09333c2809",
        decimals: 18,
      },
      // Add stablecoins if needed once confirmed
    },
    marketplaces: {
      opensea: SEAPORT_CONTRACT_ADDRESS,
    },
    apiIdentifiers: {
      // Confirm identifier if/when OpenSea exposes it
      opensea: "abstract",
    },
    marketplaceSpenders: {
      opensea: SEAPORT_SPENDER_ADDRESS,
    },
  },
  megaeth: {
    value: "megaeth",
    label: "MegaETH",
    provider: {
      chainId: 4326,
      alchemyNetwork: "megaeth",
      rpcUrlEnv: "MEGAETH_RPC_URL",
    },
    currencies: {
      weth: {
        symbol: "WETH",
        address: "0x4200000000000000000000000000000000000006",
        decimals: 18,
      },
      usdm: {
        symbol: "USDM",
        address: "0xFAfDdbb3FC7688494971a79cc65DCa3EF82079E7",
        decimals: 18,
      },
    },
    marketplaces: {
      opensea: SEAPORT_CONTRACT_ADDRESS,
    },
    apiIdentifiers: {
      opensea: "megaeth",
    },
    marketplaceSpenders: {
      opensea: SEAPORT_SPENDER_ADDRESS,
    },
  },
};

const providerCache = new Map<ChainValue, ethers.JsonRpcProvider>();

const CHAIN_ALIAS_MAP: Record<string, ChainValue> = {
  // Ethereum
  mainnet: "ethereum",
  // Base
  "base-mainnet": "base",
  base: "base",
  // Arbitrum
  "arb-mainnet": "arbitrum",
  arbitrum: "arbitrum",
  "arbitrum-one": "arbitrum",
  // Abstract
  abstract: "abstract",
  "abstract-mainnet": "abstract",
  // MegaETH
  megaeth: "megaeth",
  "megaeth-mainnet": "megaeth",
};

export function normalizeChainValue(chain?: ChainValue | string | null): ChainValue {
  if (!chain) {
    return DEFAULT_CHAIN;
  }
  const normalized = CHAIN_ALIAS_MAP[String(chain).toLowerCase()];
  const candidate = (normalized ?? chain) as ChainValue;
  return CHAIN_CONFIGS[candidate] ? candidate : DEFAULT_CHAIN;
}

function resolveAlchemyRpcUrl(network: string, apiKey?: string): string | undefined {
  if (network === "base-mainnet" || network === "base") {
    if (apiKey) {
      return `https://base-mainnet.g.alchemy.com/v2/${apiKey}`;
    }
    return "https://mainnet.base.org";
  }
  if (network === "abstract-mainnet" || network === "abstract") {
    if (apiKey) {
      return `https://abstract-mainnet.g.alchemy.com/v2/${apiKey}`;
    }
    return "https://api.mainnet.abs.xyz";
  }
  if (network === "megaeth-mainnet" || network === "megaeth") {
    return "https://mainnet.megaeth.com/rpc";
  }
  if (apiKey) {
    return `https://${network}.g.alchemy.com/v2/${apiKey}`;
  }
  return undefined;
}

export function getChainConfig(chain?: ChainValue | null): ChainConfig {
  const resolved = normalizeChainValue(chain);
  const config = CHAIN_CONFIGS[resolved];

  if (!config) {
    return CHAIN_CONFIGS[DEFAULT_CHAIN]!;
  }

  return config;
}

function createProvider(config: ChainConfig): ethers.JsonRpcProvider {
  if (config.provider.rpcUrlEnv) {
    const rpcUrl = process.env[config.provider.rpcUrlEnv];
    if (rpcUrl) {
      return new ethers.JsonRpcProvider(rpcUrl, config.provider.chainId);
    }
  }

  if (config.provider.alchemyNetwork) {
    const apiKey = process.env.ALCHEMY_API_KEY;
    try {
      return new ethers.AlchemyProvider(config.provider.alchemyNetwork, apiKey);
    } catch (error) {
      const fallbackUrl = resolveAlchemyRpcUrl(config.provider.alchemyNetwork, apiKey);
      if (fallbackUrl) {
        return new ethers.JsonRpcProvider(fallbackUrl, config.provider.chainId);
      }
      throw error;
    }
  }

  return new ethers.AlchemyProvider("mainnet", process.env.ALCHEMY_API_KEY);
}

export function getProviderForChain(chain?: ChainValue | null): ethers.JsonRpcProvider {
  const resolved = normalizeChainValue(chain);
  if (!providerCache.has(resolved)) {
    const config = getChainConfig(resolved);
    providerCache.set(resolved, createProvider(config));
  }
  return providerCache.get(resolved)!;
}

export function getCurrencyAddress(chain: ChainValue | null | undefined, currencySymbol: string): string | undefined {
  const config = getChainConfig(chain);
  return config.currencies[currencySymbol.toLowerCase()]?.address;
}

export function getCurrencyDecimals(chain: ChainValue | null | undefined, currencySymbol: string): number | undefined {
  const config = getChainConfig(chain);
  return config.currencies[currencySymbol.toLowerCase()]?.decimals;
}

export function getMarketplaceAddress(chain: ChainValue | null | undefined, marketplace: MarketplaceKey): string | undefined {
  const config = getChainConfig(chain);
  const address = config.marketplaces[marketplace];
  if (address) {
    return address;
  }
  const fallback = CHAIN_CONFIGS[DEFAULT_CHAIN]?.marketplaces[marketplace];
  return fallback;
}

export function getMarketplaceChainIdentifier(chain: ChainValue | null | undefined, marketplace: MarketplaceKey): string {
  const config = getChainConfig(chain);
  const identifier = config.apiIdentifiers?.[marketplace];
  if (identifier) {
    return identifier;
  }
  const fallback = CHAIN_CONFIGS[DEFAULT_CHAIN]?.apiIdentifiers?.[marketplace];
  if (fallback) {
    return fallback;
  }
  return config.value;
}

export function getMarketplaceSpender(chain: ChainValue | null | undefined, marketplace: MarketplaceKey): string | undefined {
  const config = getChainConfig(chain);
  const spender = config.marketplaceSpenders?.[marketplace];
  if (spender) {
    return spender;
  }
  return CHAIN_CONFIGS[DEFAULT_CHAIN]?.marketplaceSpenders?.[marketplace];
}

// Minimal ABI for Seaport ConduitController.getConduit(bytes32)
const CONDUIT_CONTROLLER_MIN_ABI = [
  {
    inputs: [{ name: "conduitKey", type: "bytes32" }],
    name: "getConduit",
    outputs: [
      { name: "conduit", type: "address" },
      { name: "exists", type: "bool" },
    ],
    stateMutability: "view",
    type: "function",
  },
] as const;

// Minimal ABI for Seaport to obtain the ConduitController address
const SEAPORT_MIN_ABI = [
  {
    inputs: [],
    name: "conduitController",
    outputs: [{ name: "", type: "address" }],
    stateMutability: "view",
    type: "function",
  },
] as const;

/**
 * Resolve OpenSea spender: accepts either an address or a conduit key (bytes32).
 * If a conduit key is provided, derives the conduit address using the chain's ConduitController.
 */
export async function resolveOpenseaSpender(
  chain: ChainValue | null | undefined,
  keyOrAddress: string | undefined
): Promise<string | undefined> {
  if (!keyOrAddress) return undefined;

  // If already an address, return as-is
  if (ethers.isAddress(keyOrAddress)) return keyOrAddress;

  // If it looks like a conduit key (0x + 32 bytes)
  if (ethers.isHexString(keyOrAddress, 32)) {
    try {
      const provider = getProviderForChain(chain ?? DEFAULT_CHAIN);
      // Resolve Seaport address for this chain
      const seaportAddress = getMarketplaceAddress(chain ?? DEFAULT_CHAIN, "opensea");
      if (!seaportAddress || !ethers.isAddress(seaportAddress)) return keyOrAddress;

      // 1) Load Seaport and fetch its ConduitController address
      // @ts-ignore Contract exists on ethers in v6
      const seaportContract = new ethers.Contract(seaportAddress, SEAPORT_MIN_ABI, provider);
      const controllerAddress: string = await seaportContract.conduitController();
      if (!controllerAddress || !ethers.isAddress(controllerAddress)) return keyOrAddress;

      // 2) Query ConduitController.getConduit(bytes32) to derive the conduit address
      // @ts-ignore Contract exists on ethers in v6
      const controllerContract = new ethers.Contract(controllerAddress, CONDUIT_CONTROLLER_MIN_ABI, provider);
      const result: any = await controllerContract.getConduit(keyOrAddress);
      const conduitAddress = Array.isArray(result) ? result[0] : result;
      if (typeof conduitAddress === "string" && ethers.isAddress(conduitAddress)) return conduitAddress;
    } catch (error) {
      // Log and fall through to return the original value
      console.warn('[chains] resolveOpenseaSpender conduit resolution failed:', error);
    }
  }

  return keyOrAddress;
}
