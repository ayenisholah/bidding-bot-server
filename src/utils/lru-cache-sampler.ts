/**
 * LRU Cache Data Sampler
 *
 * Utility to sample and log data from all LRU caches for debugging and analysis.
 * Uses debug-file-logger.ts to write samples to category-specific log files.
 *
 * Usage:
 *   - Call sampleAllCaches() to log samples from all caches
 *   - Call sampleCache(cacheName) to log samples from a specific cache
 *   - Logs written to debug-logs/system-YYYY-MM-DD.log
 */

// Import all LRU caches via getter functions
import { processedOwnBids } from '../state/global-state';
import { getProcessedMessages as getWsServiceProcessedMessages } from '../services/websocket.service';
import {
  getProcessedMessages as getIndexProcessedMessages,
  getSeenOrderHashes,
  getErrorLogCache,
  getCollectionDataLRU,
  getCollectionStatsLRU
} from '../index';
import orderCache from './order-cache';
import {
  collectionTraitsLRU,
  collectionDetailsLRU,
  openseaFloorLRU,
  balanceLRU,
  traitFloorLRU,
  collectionItemsLRU,
  blurFloorLRU,
  magicEdenFloorLRU,
  traitFloorsDetailLRU,
  collectionEventsLRU,
  openseaBestOfferLRU,
  blurBestOfferLRU,
  magicedenBestOfferLRU
} from '../marketplace/opensea/cache';
import { tokenTraitCache } from './token-trait-cache';
import { rarityCache } from './rarity-cache';
import { approvalCache } from './approval-cache';
import { logsCache } from './logs-cache';
import { listingPriceCache } from './listing-price-cache';
import { tradingDisabledCache } from './trading-disabled-cache';
import {
  getContractResolutionLRU,
  getTraitFloorLRU,
  getMultiContractSlugLRU,
  getContractMapLRU,
  getOpenseaWalletCache,
  getOpenseaInsufficientBalanceLogCache
} from '../marketplace/opensea/bid';
import {
  getMagicedenWalletCache,
  getMagicedenInsufficientBalanceLogCache
} from '../marketplace/magiceden/bid';
import { getBlurInsufficientBalanceLogCache } from '../marketplace/blur/bid';
import { getWarmingSlugs } from '../services/collection-data-warmer.service';
import { walletCache } from './wallet-cache';
import { marketplaceUnavailableLRU } from './marketplace-availability';
import { marketplaceApprovalCache } from './marketplace-approval-cache';
import { getLoggedBalanceErrors } from './balance-helpers';
import { getForwardingLatencySamples } from './websocket-manager';
import { getFloorPriceCache, getCollectionDetailsCache } from './collection-cache';
import { getExecutionLatencySamples } from '../services/counterbid-tracking.service';
import { getDecryptedKeyCache } from './crypto';

/**
 * Sample configuration for each cache type
 */
interface CacheSampleConfig {
  name: string;
  cache: any; // LRUCache instance or wrapper object
  category: 'dedup' | 'marketplace' | 'security' | 'monitoring' | 'error' | 'trait' | 'other';
  maxSize: number;
  ttl: string;
  sampleSize: number; // Number of entries to sample
  formatValue: (key: string, value: any) => string; // Custom formatter for log output
}

/**
 * All cache configurations with sample formatters
 */
const CACHE_CONFIGS: CacheSampleConfig[] = [
  // ========================================
  // 🔥 HIGH-FREQUENCY DEDUPLICATION (1M)
  // ========================================
  {
    name: 'processedOwnBids',
    cache: processedOwnBids,
    category: 'dedup',
    maxSize: 1_000_000,
    ttl: '24h',
    sampleSize: 5,
    formatValue: (key, value) => `orderHash=${key} | value=${value}`
  },
  {
    name: 'processedMessages (websocket.service)',
    cache: getWsServiceProcessedMessages(),
    category: 'dedup',
    maxSize: 1_000_000,
    ttl: '24h',
    sampleSize: 5,
    formatValue: (key, value) => `messageId=${key} | value=${value}`
  },
  {
    name: 'processedMessages (index)',
    cache: getIndexProcessedMessages(),
    category: 'dedup',
    maxSize: 1_000_000,
    ttl: '24h',
    sampleSize: 5,
    formatValue: (key, value) => `messageId=${key} | value=${value}`
  },
  {
    name: 'seenOrderHashes',
    cache: getSeenOrderHashes(),
    category: 'dedup',
    maxSize: 1_000_000,
    ttl: '60s',
    sampleSize: 5,
    formatValue: (key, value) => `orderHash=${key} | value=${value}`
  },
  {
    name: 'orderLRU',
    cache: orderCache.getOrderLRU(),
    category: 'marketplace',
    maxSize: 1_000_000,
    ttl: '24h',
    sampleSize: 3,
    formatValue: (key, value) => `identifier=${key} | orders=${Array.isArray(value) ? value.length : 'unknown'} | sample=${JSON.stringify(value?.[0] || null).substring(0, 200)}`
  },
  {
    name: 'collectionTraitsLRU',
    cache: collectionTraitsLRU,
    category: 'trait',
    maxSize: 1_000_000,
    ttl: '24h',
    sampleSize: 3,
    formatValue: (key, value) => `slug=${key} | traits=${JSON.stringify(value).substring(0, 300)}`
  },
  {
    name: 'tokenTraitsLRU',
    cache: tokenTraitCache.getCache(),
    category: 'trait',
    maxSize: 1_000_000,
    ttl: '24h',
    sampleSize: 3,
    formatValue: (key, value) => `key=${key} | traits=${JSON.stringify(value).substring(0, 200)}`
  },
  {
    name: 'rarityCache',
    cache: rarityCache.getCache(),
    category: 'trait',
    maxSize: 1_000_000,
    ttl: '24h',
    sampleSize: 3,
    formatValue: (key, value) => `key=${key} | tokenIds=${Array.isArray(value) ? value.length : 'unknown'} | sample=${JSON.stringify(value?.slice(0, 10) || null)}`
  },

  // ========================================
  // 📊 MARKETPLACE DATA (1K-10K)
  // ========================================
  {
    name: 'openseaFloorLRU',
    cache: openseaFloorLRU,
    category: 'marketplace',
    maxSize: 1_000,
    ttl: '5min',
    sampleSize: 3,
    formatValue: (key, value) => `slug=${key} | floor=${value} ETH`
  },
  {
    name: 'blurFloorLRU',
    cache: blurFloorLRU,
    category: 'marketplace',
    maxSize: 1_000,
    ttl: '5min',
    sampleSize: 3,
    formatValue: (key, value) => `slug=${key} | floor=${value} ETH`
  },
  {
    name: 'magicEdenFloorLRU',
    cache: magicEdenFloorLRU,
    category: 'marketplace',
    maxSize: 1_000,
    ttl: '1min',
    sampleSize: 3,
    formatValue: (key, value) => `slug=${key} | floor=${value} ETH`
  },
  {
    name: 'openseaBestOfferLRU',
    cache: openseaBestOfferLRU,
    category: 'marketplace',
    maxSize: 1_000,
    ttl: '5min',
    sampleSize: 3,
    formatValue: (key, value) => `contract=${key} | bestOffer=${JSON.stringify(value).substring(0, 200)}`
  },
  {
    name: 'blurBestOfferLRU',
    cache: blurBestOfferLRU,
    category: 'marketplace',
    maxSize: 1_000,
    ttl: '5min',
    sampleSize: 3,
    formatValue: (key, value) => `contract=${key} | bestOffer=${JSON.stringify(value).substring(0, 200)}`
  },
  {
    name: 'magicedenBestOfferLRU',
    cache: magicedenBestOfferLRU,
    category: 'marketplace',
    maxSize: 1_000,
    ttl: '5min',
    sampleSize: 3,
    formatValue: (key, value) => `contract=${key} | bestOffer=${JSON.stringify(value).substring(0, 200)}`
  },
  {
    name: 'collectionDetailsLRU',
    cache: collectionDetailsLRU,
    category: 'marketplace',
    maxSize: 1_000,
    ttl: '24h',
    sampleSize: 2,
    formatValue: (key, value) => `slug=${key} | details=${JSON.stringify(value).substring(0, 300)}`
  },
  {
    name: 'collectionDetailsCache',
    cache: getCollectionDetailsCache(),
    category: 'marketplace',
    maxSize: 1_000,
    ttl: '24h',
    sampleSize: 2,
    formatValue: (key, value) => `slug=${key} | details=${JSON.stringify(value).substring(0, 300)}`
  },
  {
    name: 'collectionDataLRU',
    cache: getCollectionDataLRU(),
    category: 'marketplace',
    maxSize: 1_000,
    ttl: '24h',
    sampleSize: 2,
    formatValue: (key, value) => `slug=${key} | dataSize=${JSON.stringify(value).length} bytes | keys=${Object.keys(value || {}).join(', ')}`
  },
  {
    name: 'collectionItemsLRU',
    cache: collectionItemsLRU,
    category: 'marketplace',
    maxSize: 1_000,
    ttl: '1h',
    sampleSize: 2,
    formatValue: (key, value) => `slug=${key} | items=${Array.isArray(value) ? value.length : 'unknown'} | sample=${JSON.stringify(value?.slice(0, 3) || null)}`
  },
  {
    name: 'collectionEventsLRU',
    cache: collectionEventsLRU,
    category: 'marketplace',
    maxSize: 1_000,
    ttl: '10s',
    sampleSize: 2,
    formatValue: (key, value) => `slug=${key} | events=${JSON.stringify(value).substring(0, 200)}`
  },
  {
    name: 'floorPriceCache',
    cache: getFloorPriceCache(),
    category: 'marketplace',
    maxSize: 1_000,
    ttl: '5min',
    sampleSize: 3,
    formatValue: (key, value) => `slug=${key} | floor=${JSON.stringify(value).substring(0, 150)}`
  },
  {
    name: 'collectionStatsLRU',
    cache: getCollectionStatsLRU(),
    category: 'marketplace',
    maxSize: 1_000,
    ttl: '24h',
    sampleSize: 2,
    formatValue: (key, value) => `slug=${key} | stats=${JSON.stringify(value).substring(0, 200)}`
  },

  // ========================================
  // 🔐 SECURITY & WALLET
  // ========================================
  {
    name: 'decryptedKeyCache',
    cache: getDecryptedKeyCache(),
    category: 'security',
    maxSize: 1_000,
    ttl: '24h',
    sampleSize: 2,
    formatValue: (key, value) => `walletId=${key} | keyExists=${!!value} | keyLength=${value?.length || 0} | keyPreview=${value?.substring(0, 10)}...***REDACTED***`
  },
  {
    name: 'walletDataLRU',
    cache: walletCache.getCache(),
    category: 'security',
    maxSize: 1_000,
    ttl: '24h',
    sampleSize: 3,
    formatValue: (key, value) => `walletId=${key} | wallet=${JSON.stringify(value).substring(0, 200)}`
  },
  {
    name: 'walletCache (OpenSea)',
    cache: getOpenseaWalletCache(),
    category: 'security',
    maxSize: 1_000,
    ttl: '1min',
    sampleSize: 2,
    formatValue: (key, value) => `walletId=${key} | walletExists=${!!value} | address=${value?.address || 'N/A'}`
  },
  {
    name: 'walletCache (MagicEden)',
    cache: getMagicedenWalletCache(),
    category: 'security',
    maxSize: 1_000,
    ttl: '1min',
    sampleSize: 2,
    formatValue: (key, value) => `walletId=${key} | walletExists=${!!value} | address=${value?.address || 'N/A'}`
  },
  {
    name: 'balanceLRU',
    cache: balanceLRU,
    category: 'security',
    maxSize: 1_000,
    ttl: '60s',
    sampleSize: 3,
    formatValue: (key, value) => `address=${key} | balance=${JSON.stringify(value).substring(0, 150)}`
  },
  {
    name: 'approvalEntriesLRU',
    cache: approvalCache.getCache(),
    category: 'security',
    maxSize: 50_000,
    ttl: '24h',
    sampleSize: 5,
    formatValue: (key, value) => `key=${key} | allowance=${value}`
  },
  {
    name: 'marketplaceApprovalLRU',
    cache: marketplaceApprovalCache.getCache(),
    category: 'security',
    maxSize: 1_000,
    ttl: '24h',
    sampleSize: 3,
    formatValue: (key, value) => `key=${key} | approved=${value}`
  },

  // ========================================
  // 📈 PERFORMANCE MONITORING
  // ========================================
  {
    name: 'executionLatencySamples',
    cache: getExecutionLatencySamples(),
    category: 'monitoring',
    maxSize: 1_000,
    ttl: '1h',
    sampleSize: 5,
    formatValue: (key, value) => `taskId=${key} | samples=${Array.isArray(value) ? value.length : 'unknown'} | avg=${Array.isArray(value) ? (value.reduce((a, b) => a + b, 0) / value.length).toFixed(2) : 'N/A'}ms | data=${JSON.stringify(value?.slice(0, 5) || null)}`
  },
  {
    name: 'forwardingLatencySamples',
    cache: getForwardingLatencySamples(),
    category: 'monitoring',
    maxSize: 1_000,
    ttl: '5min',
    sampleSize: 5,
    formatValue: (key, value) => `key=${key} | samples=${Array.isArray(value) ? value.length : 'unknown'} | avg=${Array.isArray(value) ? (value.reduce((a, b) => a + b, 0) / value.length).toFixed(2) : 'N/A'}ms | data=${JSON.stringify(value?.slice(0, 5) || null)}`
  },

  // ========================================
  // 🚫 ERROR DEDUPLICATION
  // ========================================
  {
    name: 'errorLogCache',
    cache: getErrorLogCache(),
    category: 'error',
    maxSize: 5_000,
    ttl: '24h',
    sampleSize: 5,
    formatValue: (key, value) => `errorKey=${key} | value=${value}`
  },
  {
    name: 'bidLogsLRU',
    cache: logsCache.getCache(),
    category: 'error',
    maxSize: 10_000,
    ttl: '24h',
    sampleSize: 5,
    formatValue: (key, value) => `taskId=${key} | logs=${JSON.stringify(value).substring(0, 300)}`
  },
  {
    name: 'loggedBalanceErrors',
    cache: getLoggedBalanceErrors(),
    category: 'error',
    maxSize: 1_000,
    ttl: '1min',
    sampleSize: 3,
    formatValue: (key, value) => `walletId=${key} | value=${value}`
  },
  {
    name: 'insufficientBalanceLogCache (OpenSea)',
    cache: getOpenseaInsufficientBalanceLogCache(),
    category: 'error',
    maxSize: 1_000,
    ttl: '1min',
    sampleSize: 3,
    formatValue: (key, value) => `walletId=${key} | value=${value}`
  },
  {
    name: 'insufficientBalanceLogCache (MagicEden)',
    cache: getMagicedenInsufficientBalanceLogCache(),
    category: 'error',
    maxSize: 1_000,
    ttl: '1min',
    sampleSize: 3,
    formatValue: (key, value) => `walletId=${key} | value=${value}`
  },
  {
    name: 'insufficientBalanceLogCache (Blur)',
    cache: getBlurInsufficientBalanceLogCache(),
    category: 'error',
    maxSize: 1_000,
    ttl: '1min',
    sampleSize: 3,
    formatValue: (key, value) => `walletId=${key} | value=${value}`
  },

  // ========================================
  // 🎯 TRAIT & PRICING
  // ========================================
  {
    name: 'traitFloorLRU (opensea/cache)',
    cache: traitFloorLRU,
    category: 'trait',
    maxSize: 10_000,
    ttl: '5min',
    sampleSize: 5,
    formatValue: (key, value) => `traitKey=${key} | floor=${value} ETH`
  },
  {
    name: 'traitFloorLRU (opensea/bid)',
    cache: getTraitFloorLRU(),
    category: 'trait',
    maxSize: 10_000,
    ttl: '5min',
    sampleSize: 5,
    formatValue: (key, value) => `traitKey=${key} | floor=${value} ETH`
  },
  {
    name: 'traitFloorsDetailLRU',
    cache: traitFloorsDetailLRU,
    category: 'trait',
    maxSize: 10_000,
    ttl: '1min',
    sampleSize: 5,
    formatValue: (key, value) => `traitKey=${key} | details=${JSON.stringify(value).substring(0, 250)}`
  },
  {
    name: 'priceCache',
    cache: listingPriceCache.getCache(),
    category: 'trait',
    maxSize: 10_000,
    ttl: '5min',
    sampleSize: 5,
    formatValue: (key, value) => `identifier=${key} | price=${value} ETH`
  },

  // ========================================
  // 🔧 OTHER UTILITIES
  // ========================================
  {
    name: 'tradingDisabledCache',
    cache: tradingDisabledCache,
    category: 'other',
    maxSize: 5_000,
    ttl: '5min',
    sampleSize: 3,
    formatValue: (key, value) => `slug=${key} | disabled=${value}`
  },
  {
    name: 'contractResolutionLRU',
    cache: getContractResolutionLRU(),
    category: 'other',
    maxSize: 5_000,
    ttl: '24h',
    sampleSize: 3,
    formatValue: (key, value) => `slug=${key} | contract=${value}`
  },
  {
    name: 'multiContractSlugLRU',
    cache: getMultiContractSlugLRU(),
    category: 'other',
    maxSize: 1_000,
    ttl: '24h',
    sampleSize: 3,
    formatValue: (key, value) => `slug=${key} | isMultiContract=${value}`
  },
  {
    name: 'contractMapLRU',
    cache: getContractMapLRU(),
    category: 'other',
    maxSize: 1_000,
    ttl: '24h',
    sampleSize: 3,
    formatValue: (key, value) => `tokenId=${key} | contract=${value}`
  },
  {
    name: 'warmingSlugs',
    cache: getWarmingSlugs(),
    category: 'other',
    maxSize: 1_000,
    ttl: '24h',
    sampleSize: 5,
    formatValue: (key, value) => `slug=${key} | refCount=${value}`
  },
  {
    name: 'marketplaceUnavailableLRU',
    cache: marketplaceUnavailableLRU,
    category: 'other',
    maxSize: 1_000,
    ttl: '1h',
    sampleSize: 3,
    formatValue: (key, value) => `key=${key} | unavailable=${value}`
  },
];

/**
 * Extract sample entries from an LRU cache
 */
function extractSamples(cache: any, sampleSize: number): Array<{ key: string; value: any }> {
  const samples: Array<{ key: string; value: any }> = [];

  try {
    // Handle LRUCache instances (most common)
    if (cache && typeof cache.forEach === 'function') {
      let count = 0;
      cache.forEach((value: any, key: string) => {
        if (count < sampleSize) {
          samples.push({ key, value });
          count++;
        }
      });
    }
    // Handle wrapper objects with .cache property (e.g., rarityCache.cache)
    else if (cache && cache.cache && typeof cache.cache.forEach === 'function') {
      let count = 0;
      cache.cache.forEach((value: any, key: string) => {
        if (count < sampleSize) {
          samples.push({ key, value });
          count++;
        }
      });
    }
    // Handle objects with dump() method
    else if (cache && typeof cache.dump === 'function') {
      const dump = cache.dump();
      const entries = Array.isArray(dump) ? dump : [];
      for (let i = 0; i < Math.min(sampleSize, entries.length); i++) {
        const entry = entries[i];
        if (entry && entry.k !== undefined) {
          samples.push({ key: String(entry.k), value: entry.v });
        }
      }
    }
  } catch (error) {
    // Silently fail - cache might not be initialized yet
  }

  return samples;
}

/**
 * Get cache size (number of entries)
 */
function getCacheSize(cache: any): number {
  try {
    if (cache && typeof cache.size === 'number') {
      return cache.size;
    }
    if (cache && cache.cache && typeof cache.cache.size === 'number') {
      return cache.cache.size;
    }
    if (cache && typeof cache.calculatedSize === 'number') {
      return cache.calculatedSize;
    }
  } catch {
    // Silently fail
  }
  return 0;
}

/**
 * Sample data from a specific cache
 */
export function sampleCache(cacheName: string): void {
  const config = CACHE_CONFIGS.find(c => c.name === cacheName);

  if (!config) {
    return;
  }

  const size = getCacheSize(config.cache);
  const samples = extractSamples(config.cache, config.sampleSize);


  if (samples.length === 0) {
    return;
  }

  samples.forEach((sample, index) => {
    const formatted = config.formatValue(sample.key, sample.value);
  });

}

/**
 * Sample data from all caches
 */
export function sampleAllCaches(): void {

  // Group by category
  const categories = ['dedup', 'marketplace', 'security', 'monitoring', 'error', 'trait', 'other'] as const;

  categories.forEach(category => {
    const categoryConfigs = CACHE_CONFIGS.filter(c => c.category === category);
    if (categoryConfigs.length === 0) return;


    categoryConfigs.forEach(config => {
      sampleCache(config.name);
    });
  });

  // Summary statistics
  const totalCaches = CACHE_CONFIGS.length;
  const totalMaxCapacity = CACHE_CONFIGS.reduce((sum, c) => sum + c.maxSize, 0);
  const totalCurrentSize = CACHE_CONFIGS.reduce((sum, c) => sum + getCacheSize(c.cache), 0);


  categories.forEach(category => {
    const categoryConfigs = CACHE_CONFIGS.filter(c => c.category === category);
    const categoryMax = categoryConfigs.reduce((sum, c) => sum + c.maxSize, 0);
    const categoryCurrent = categoryConfigs.reduce((sum, c) => sum + getCacheSize(c.cache), 0);
  });

}

/**
 * Sample caches by category
 */
export function sampleCachesByCategory(category: 'dedup' | 'marketplace' | 'security' | 'monitoring' | 'error' | 'trait' | 'other'): void {
  const categoryConfigs = CACHE_CONFIGS.filter(c => c.category === category);

  if (categoryConfigs.length === 0) {
    return;
  }


  categoryConfigs.forEach(config => {
    sampleCache(config.name);
  });
}

/**
 * Get cache statistics without sampling data
 */
export function getCacheStatistics(): {
  total: number;
  byCategory: Record<string, number>;
  totalMaxCapacity: number;
  totalCurrentSize: number;
  fillRate: number;
} {
  const stats = {
    total: CACHE_CONFIGS.length,
    byCategory: {} as Record<string, number>,
    totalMaxCapacity: 0,
    totalCurrentSize: 0,
    fillRate: 0
  };

  CACHE_CONFIGS.forEach(config => {
    stats.byCategory[config.category] = (stats.byCategory[config.category] || 0) + 1;
    stats.totalMaxCapacity += config.maxSize;
    stats.totalCurrentSize += getCacheSize(config.cache);
  });

  stats.fillRate = (stats.totalCurrentSize / stats.totalMaxCapacity) * 100;

  return stats;
}
