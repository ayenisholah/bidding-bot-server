/**
 * LRU Cache Insertion Logger
 *
 * Utility to log all LRU cache insertions for:
 * 1. Building accurate TypeScript interfaces
 * 2. Finding duplicate caches (same key/value patterns)
 * 3. Debugging cache behavior
 *
 * Uses debug-file-logger.ts to write to category-specific log files.
 */

import { debugLog } from './debug-file-logger';

/**
 * Configuration for cache insertion logging
 */
const ENABLE_CACHE_INSERTION_LOGGING = process.env.LOG_CACHE_INSERTIONS === 'true';

/**
 * Maximum value size to log (truncate large objects)
 */
const MAX_VALUE_SIZE = 500; // characters

/**
 * Sanitize and truncate value for logging
 */
function sanitizeValue(value: any): string {
  try {
    if (value === null) return 'null';
    if (value === undefined) return 'undefined';

    const type = typeof value;

    // Primitives - log directly
    if (type === 'string') return `"${value.substring(0, 100)}"${value.length > 100 ? '...' : ''}`;
    if (type === 'number') return String(value);
    if (type === 'boolean') return String(value);

    // Arrays - show length + first few items
    if (Array.isArray(value)) {
      const preview = value.slice(0, 3).map(v => sanitizeValue(v)).join(', ');
      return `Array(${value.length}) [${preview}${value.length > 3 ? ', ...' : ''}]`;
    }

    // Objects - show keys + truncated JSON
    const json = JSON.stringify(value);
    const keys = Object.keys(value);

    if (json.length <= MAX_VALUE_SIZE) {
      return json;
    }

    return `Object{${keys.slice(0, 5).join(', ')}${keys.length > 5 ? ', ...' : ''}} ${json.substring(0, MAX_VALUE_SIZE)}...`;
  } catch {
    return `[Unserializable ${typeof value}]`;
  }
}

/**
 * Get TypeScript type signature from value
 */
function getTypeSignature(value: any): string {
  if (value === null) return 'null';
  if (value === undefined) return 'undefined';

  const type = typeof value;

  if (type === 'string') return 'string';
  if (type === 'number') return 'number';
  if (type === 'boolean') return 'boolean';

  if (Array.isArray(value)) {
    if (value.length === 0) return 'any[]';
    const firstType = getTypeSignature(value[0]);
    return `${firstType}[]`;
  }

  if (type === 'object') {
    try {
      const keys = Object.keys(value);
      if (keys.length === 0) return '{}';

      const signature = keys.slice(0, 10).map(k => {
        const valType = getTypeSignature(value[k]);
        return `${k}: ${valType}`;
      }).join('; ');

      return `{ ${signature}${keys.length > 10 ? '; ...' : ''} }`;
    } catch {
      return 'object';
    }
  }

  return type;
}

/**
 * Log cache insertion for analysis
 *
 * @param cacheName - Name of the LRU cache (e.g., 'orderLRU', 'tokenTraitsLRU')
 * @param key - Cache key being inserted
 * @param value - Value being stored
 * @param options - Optional metadata (ttl, caller context, etc.)
 */
export function logCacheInsertion(
  cacheName: string,
  key: string | number,
  value: any,
  options?: {
    ttl?: number;
    taskId?: string;
    marketplace?: 'blur' | 'magiceden' | 'opensea' | 'system';
    slug?: string;
    caller?: string; // Function/method name that called .set()
  }
): void {
  // Skip if logging disabled
  if (!ENABLE_CACHE_INSERTION_LOGGING) return;

  try {
    const sanitizedValue = sanitizeValue(value);
    const typeSignature = getTypeSignature(value);

    // Build log message
    const parts = [
      `CACHE_INSERT | cache=${cacheName}`,
      `key=${String(key)}`,
      `type=${typeSignature}`,
      `value=${sanitizedValue}`
    ];

    if (options?.ttl) parts.push(`ttl=${options.ttl}ms`);
    if (options?.caller) parts.push(`caller=${options.caller}`);

    const message = parts.join(' | ');

    // Metadata for structured logging
    const metadata = {
      cacheName,
      key: String(key),
      typeSignature,
      ...(options?.taskId && { taskId: options.taskId }),
      ...(options?.marketplace && { marketplace: options.marketplace }),
      ...(options?.slug && { slug: options.slug })
    };

    // Log to 'Cache' prefix -> routes to system-YYYY-MM-DD.log
    debugLog('Cache', message, metadata);
  } catch (error) {
    // Silently fail - don't break cache operations
  }
}

/**
 * Wrapper for cache.set() that logs insertion
 *
 * Usage:
 *   Instead of: cache.set(key, value)
 *   Use: loggedSet(cache, 'cacheName', key, value)
 *
 * @param cache - LRUCache instance
 * @param cacheName - Name of cache for logging
 * @param key - Cache key
 * @param value - Value to store
 * @param options - Optional ttl and metadata
 * @returns The cache instance (for chaining)
 */
export function loggedSet<K, V>(
  cache: any, // LRUCache<K, V>
  cacheName: string,
  key: K,
  value: V,
  options?: {
    ttl?: number;
    taskId?: string;
    marketplace?: 'blur' | 'magiceden' | 'opensea' | 'system';
    slug?: string;
    caller?: string;
  }
): any {
  // Log the insertion
  logCacheInsertion(cacheName, key as any, value, options);

  // Perform actual cache.set()
  if (options?.ttl) {
    cache.set(key, value, { ttl: options.ttl });
  } else {
    cache.set(key, value);
  }

  return cache;
}

/**
 * Analyze cache insertion patterns from logs
 *
 * Use this to parse debug logs and find:
 * - Duplicate caches (same type signatures)
 * - Interface patterns (common object shapes)
 * - Cache usage statistics
 *
 * Example log line to parse:
 * [2026-01-30T10:15:30.123Z] [Cache] CACHE_INSERT | cache=orderLRU | key=0x1234:123 | type={ orderHash: string; marketplace: string; price: string } | value={"orderHash":"0xabc..."}
 */
export interface CacheInsertionLogEntry {
  timestamp: string;
  cacheName: string;
  key: string;
  typeSignature: string;
  value: string;
  ttl?: number;
  caller?: string;
  taskId?: string;
  marketplace?: string;
  slug?: string;
}

/**
 * Parse cache insertion log line
 */
export function parseCacheInsertionLog(line: string): CacheInsertionLogEntry | null {
  try {
    // Extract timestamp
    const timestampMatch = line.match(/^\[([\d\-T:.Z]+)\]/);
    if (!timestampMatch) return null;
    const timestamp = timestampMatch[1];

    // Check if it's a CACHE_INSERT log
    if (!line.includes('CACHE_INSERT')) return null;

    // Extract fields
    const cacheMatch = line.match(/cache=([^\s|]+)/);
    const keyMatch = line.match(/key=([^\s|]+)/);
    const typeMatch = line.match(/type=([^|]+)\s*\|/);
    const valueMatch = line.match(/value=(.+?)(?:\s*\||$)/);
    const ttlMatch = line.match(/ttl=(\d+)/);
    const callerMatch = line.match(/caller=([^\s|]+)/);

    if (!cacheMatch || !keyMatch) return null;

    return {
      timestamp,
      cacheName: cacheMatch[1],
      key: keyMatch[1],
      typeSignature: typeMatch?.[1]?.trim() || 'unknown',
      value: valueMatch?.[1]?.trim() || '',
      ttl: ttlMatch ? parseInt(ttlMatch[1]) : undefined,
      caller: callerMatch?.[1]
    };
  } catch {
    return null;
  }
}

/**
 * Find duplicate caches by analyzing type signatures
 *
 * Returns groups of caches with identical type signatures
 * (potential candidates for consolidation)
 */
export function findDuplicateCaches(entries: CacheInsertionLogEntry[]): Map<string, string[]> {
  const typeMap = new Map<string, Set<string>>();

  for (const entry of entries) {
    const caches = typeMap.get(entry.typeSignature) || new Set<string>();
    caches.add(entry.cacheName);
    typeMap.set(entry.typeSignature, caches);
  }

  // Filter to only duplicates (>1 cache with same type)
  const duplicates = new Map<string, string[]>();

  for (const [typeSignature, caches] of typeMap) {
    if (caches.size > 1) {
      duplicates.set(typeSignature, Array.from(caches));
    }
  }

  return duplicates;
}

/**
 * Build TypeScript interface from cache insertion logs
 *
 * Aggregates type signatures for a specific cache to build interface
 */
export function buildInterfaceFromLogs(cacheName: string, entries: CacheInsertionLogEntry[]): string {
  const cacheEntries = entries.filter(e => e.cacheName === cacheName);

  if (cacheEntries.length === 0) {
    return `// No logs found for cache: ${cacheName}`;
  }

  // Get unique type signatures
  const typeSignatures = new Set(cacheEntries.map(e => e.typeSignature));

  let interfaceStr = `/**\n * ${cacheName} Interface\n * Based on ${cacheEntries.length} insertion log samples\n */\n`;

  if (typeSignatures.size === 1) {
    const type = Array.from(typeSignatures)[0];
    interfaceStr += `export type ${cacheName}Value = ${type};\n`;
  } else {
    interfaceStr += `// Multiple types detected (union type):\n`;
    interfaceStr += `export type ${cacheName}Value =\n`;
    interfaceStr += Array.from(typeSignatures).map(t => `  | ${t}`).join('\n');
    interfaceStr += ';\n';
  }

  return interfaceStr;
}
