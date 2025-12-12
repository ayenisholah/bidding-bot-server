import { ethers, Wallet, formatUnits } from "ethers";
import { limiter, isTaskBlacklisted, axiosInstance } from "../../init";
import { activeTasks, API_KEY, currentTasks, decrementBidCount, pQueue, RESET, trackBidRate, GOLD, updateTaskCancellationPercentage } from "../..";
import { config } from "dotenv";
import { DistributedLockManager } from "../../utils/lock";
import Order from '../../models/order.model';
import { orderCache } from '../../utils/order-cache';
import { withRetry } from '../../utils/db-retry';
import redisClient from '../../utils/redis';
import { isCancel } from "axios";
import { BASE_URL } from "../../constants";
import { fetchERC20Balance } from "../../utils/balance";
import { convertBigNumbers } from "../../utils/bigNumberConverter";
import { InsufficientBalanceError, TransferRestrictedError, MinimumPriceError, InvalidRequestError } from "../errors";
import { blurRateLimiter } from "../../utils/blur-rate-limiter";
import { cancellationTracker } from "../../utils/cancellation-tracker";
import { errorLog, warnLog, debugLog } from "../../utils/debug-file-logger";
import { apiDedup } from "../../utils/api-dedup";

const BLUR_POOL_ADDRESS = "0x0000000000A39bb272e79075ade125fd351887Ac";

config()
const BLUR_API_URL = `${BASE_URL}/blur`;
const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY as string;

const provider = new ethers.AlchemyProvider('mainnet', ALCHEMY_API_KEY);

const lockManager = new DistributedLockManager({
  lockPrefix: 'blur:lock:',
  defaultTTLSeconds: 60 // 1 minute lock timeout
});

const activeRequests = new Map<string, AbortController[]>();

// Track authentication attempts in progress to prevent concurrent auth for same wallet
const authInProgress = new Map<string, Promise<string | undefined>>();

// Authentication retry queue for failed attempts
const authRetryQueue = new Map<string, { retryCount: number; nextRetryTime: number }>();
const AUTH_RETRY_DELAY = 60000; // 60 seconds between retry attempts
const MAX_AUTH_RETRIES = 3;

// Cache for transfer-restricted collections (TTL: 24 hours)
const transferRestrictedCollections = new Map<string, number>();
const RESTRICTION_CACHE_TTL = 24 * 60 * 60 * 1000; // 24 hours

// Authentication metrics
const authMetrics = {
  attempts: 0,
  successes: 0,
  failures: 0,
  lastFailure: null as Date | null,
  failureReasons: new Map<string, number>()
};

// 408 Error metrics for monitoring
const timeoutMetrics = {
  total408Errors: 0,
  auth408Errors: 0,
  format408Errors: 0,
  submit408Errors: 0,
  last408Error: null as Date | null,
  errorsByEndpoint: new Map<string, number>(),
  successAfterRetry: 0,
  failedAfterRetry: 0
};

function isCollectionRestricted(contractAddress: string): boolean {
  const restriction = transferRestrictedCollections.get(contractAddress.toLowerCase());
  if (!restriction) return false;

  // Check if cache entry has expired
  if (Date.now() > restriction) {
    transferRestrictedCollections.delete(contractAddress.toLowerCase());
    return false;
  }

  return true;
}

function addRestrictedCollection(contractAddress: string): void {
  transferRestrictedCollections.set(
    contractAddress.toLowerCase(),
    Date.now() + RESTRICTION_CACHE_TTL
  );
}

function addActiveRequest(taskId: string, controller: AbortController) {
  if (!activeRequests.has(taskId)) {
    activeRequests.set(taskId, []);
  }
  activeRequests.get(taskId)?.push(controller);
}

export async function cancelBlurRequest(taskId: string): Promise<{ success: boolean; cancelledCount: number; error?: string }> {
  // Input validation
  if (!taskId || typeof taskId !== 'string') {
    warnLog('API', `[Blur] Invalid taskId provided to cancelBlurRequest | taskId=${taskId}`);
    return { success: false, cancelledCount: 0, error: 'Invalid taskId' };
  }

  const controllers = activeRequests.get(taskId);
  if (!controllers || controllers.length === 0) {

    return { success: false, cancelledCount: 0 };
  }

  try {
    let cancelledCount = 0;

    // Cancel all controllers with timeout protection
    await Promise.all(controllers.map(async (controller) => {
      try {
        const timeoutPromise = new Promise<void>((_, reject) => {
          setTimeout(() => reject(new Error('Abort timeout')), 5000);
        });

        const abortPromise = new Promise<void>((resolve) => {
          controller.abort();
          resolve();
        });

        await Promise.race([abortPromise, timeoutPromise]);
        cancelledCount++;
      } catch (error) {
        errorLog('API', `[Blur] Failed to abort request controller | taskId=${taskId}`, error);
      }
    }));

    activeRequests.delete(taskId);

    return { success: true, cancelledCount };
  } catch (error) {
    errorLog('API', `[Blur] Failed to cancel requests | taskId=${taskId} | ${error instanceof Error ? error.message : String(error)}`, error);
    return { success: false, cancelledCount: 0, error: error instanceof Error ? error.message : String(error) };
  }
}

/**
 * Creates an offer on Blur.
 * @param walletAddress - The wallet address of the offerer.
 * @param privateKey - The private key of the offerer's wallet.
 * @param contractAddress - The contract address of the collection.
 * @param offerPrice - The price of the offer in wei.
 * @param traits - Optional traits for the offer.
 */
export async function bidOnBlur(
  taskId: string,
  bidCount: string,
  wallet_address: string,
  private_key: string,
  contractAddress: string,
  offer_price: bigint,
  slug: string,
  expiry = 900,
  traits?: string,
  batchInfo?: { batchNumber: number; totalBatches: number; traitIndex: number; totalTraits: number; batchSize?: number }
) {
  try {
    // === ENHANCED BID VALIDATION ===
    // Log batch information if provided
    if (batchInfo) {
      const batchProgress = ((batchInfo.traitIndex + 1) / batchInfo.totalTraits * 100).toFixed(1);
      console.log(`${GOLD}[Blur Batch ${batchInfo.batchNumber}/${batchInfo.totalBatches}] Trait ${batchInfo.traitIndex + 1}/${batchInfo.totalTraits} (${batchProgress}% total progress)${RESET}`);
    }

    // Check if task is blacklisted (stopped)
    if (isTaskBlacklisted(taskId, 'blur')) {
      const error = new Error(`Bid execution blocked - task ${taskId} is stopped for Blur`);
      // console.log(`[Blur] Job execution blocked for stopped task: ${taskId}`);
      throw error;
    }

    // Check if collection is in the restricted cache
    if (isCollectionRestricted(contractAddress)) {
      // console.log(`Blur: Collection ${contractAddress} is cached as transfer-restricted - skipping bid`);
      throw new TransferRestrictedError('blur', contractAddress, true);
    }

    const bethBalance = await fetchERC20Balance(wallet_address, BLUR_POOL_ADDRESS);
    // Round to 6 decimal places first to avoid floating-point precision issues
    const offerPriceNumber = Math.round(Number(offer_price) / 1e18 * 1000000) / 1000000;
    let offerPriceEth: string = (Math.round(offerPriceNumber * 100) / 100).toFixed(2)

    if (Number(offerPriceEth) > bethBalance) {
      const error = new InsufficientBalanceError(
        'blur',
        'BETH',
        Number(offerPriceEth),
        bethBalance,
        wallet_address,
        slug
      );
      errorLog('Balance', `[Blur] Insufficient BETH balance | wallet=${wallet_address.slice(0, 10)}... | slug=${slug} | bid=${offerPriceEth} | available=${bethBalance}`, error);
      throw error;
    }

    const offerPrice = BigInt(offer_price.toString());
    const accessToken = await getAccessToken(private_key);

    // Round to 6 decimal places first to avoid floating-point precision issues
    const formattedPrice = Math.round(Number(formatUnits(offerPrice.toString(), 18)) * 1000000) / 1000000;
    offerPriceEth = (Math.round(formattedPrice * 100) / 100).toFixed(2);

    if (Number(offerPriceEth) === 0) {
      const error = new MinimumPriceError(
        'blur',
        'BETH',
        Number(offerPriceEth),
        0.01,
        slug
      );
      errorLog('API', `[Blur] Bid price below minimum | wallet=${wallet_address.slice(0, 10)}... | slug=${slug} | bid=${offerPriceEth} | min=0.01`, error);
      throw error;
    }

    const wallet = new Wallet(private_key, provider);
    const basePayload = {
      price: {
        unit: 'BETH',
        amount: offerPriceEth,
      },
      quantity: 1,
      expirationTime: new Date(Date.now() + (expiry * 1000)).toISOString(),
      contractAddress: contractAddress,
    };

    const buildPayload = traits ? {
      ...basePayload,
      criteria: {
        type: "TRAIT",
        value: JSON.parse(traits)
      }
    } : basePayload;

    let build: any;

    if (!accessToken) {
      const error = new Error('Access token is undefined');
      errorLog('API', `[Blur] Failed to get access token | taskId=${taskId} | wallet=${wallet_address.slice(0, 10)}... | slug=${slug}`, error);
      throw error;
    }
    build = await formatBidOnBlur(taskId, BLUR_API_URL, accessToken, wallet_address, buildPayload, private_key);

    let data = build?.signatures?.[0];
    if (!data) {
      build = await formatBidOnBlur(taskId, BLUR_API_URL, accessToken, wallet_address, buildPayload, private_key);
      data = build?.signatures?.[0];
    }
    if (!data) {
      return;
    }

    const signObj = await wallet.signTypedData(
      data?.signData?.domain,
      data?.signData?.types,
      convertBigNumbers(data?.signData?.value)
    );

    const submitPayload = {
      signature: signObj,
      marketplaceData: data?.marketplaceData,
    };

    const cancelPayload = {
      contractAddress,
      criteriaPrices: [
        {
          price: offerPriceEth,
          criteria: {
            "type": traits ? "TRAIT" : "COLLECTION",
            value: traits ? JSON.parse(traits) : {}
          }
        }
      ]
    }
    await submitBidToBlur(taskId, bidCount, offer_price, BLUR_API_URL, accessToken, wallet_address, submitPayload, slug, cancelPayload, expiry, traits);
  } catch (error) {
    throw error;
  }
};

/**
 * Gets an access token.
 * @param url - The URL to get the access token from.
 * @param privateKey - The private key of the wallet.
 * @returns The access token.
 */
// Export function to check if wallet is auth blocked
export async function isWalletAuthBlocked(walletAddress: string): Promise<boolean> {
  const redis = redisClient.getClient();
  if (!redis) return false;

  const authBlockKey = `blur:auth:block:${walletAddress}`;
  const authBlockTime = await redis.get(authBlockKey);

  if (authBlockTime) {
    const blockTime = parseInt(authBlockTime);
    return blockTime > Date.now();
  }

  return false;
}

export async function getAccessToken(private_key: string): Promise<string | undefined> {
  const wallet = new Wallet(private_key, provider);
  const lockKey = `auth:${wallet.address}`;

  // Check if limiter is initialized
  if (!limiter) {
    const error = new Error('Limiter not initialized');
    errorLog('API', `[Blur] Limiter not initialized | operation=get_access_token | wallet=${wallet.address}`, error);
    throw error;
  }

  // First, check cache OUTSIDE the lock for immediate return
  const key = `blurAccessToken:${wallet.address}`;
  const redis = redisClient.getClient();

  try {
    const cachedToken = redis ? await redis.get(key) : null;
    if (cachedToken) {
      console.log(`[Auth Cache] Using cached token for ${wallet.address}`);
      return cachedToken;
    }
  } catch {
    warnLog('Cache', `[Blur] Failed to read auth cache | wallet=${wallet.address}`);
  }

  // Check for authentication block in Redis first
  if (redis) {
    const authBlockKey = `blur:auth:block:${wallet.address}`;
    const authBlockTime = await redis.get(authBlockKey);
    if (authBlockTime) {
      const blockTime = parseInt(authBlockTime);
      if (blockTime > Date.now()) {
        const waitTime = blockTime - Date.now();
        console.log(GOLD + `[Auth Block] Wallet ${wallet.address} is. blocked by Blur API` + RESET);
        console.log(GOLD + `[Auth Block] Wait time: ${Math.ceil(waitTime / 1000)} seconds` + RESET);
        throw new Error(`Authentication blocked until ${new Date(blockTime).toISOString()}`);
      } else {
        // Block has expired, remove it
        await redis.del(authBlockKey);
      }
    }
  }

  // Check authentication retry queue
  const retryInfo = authRetryQueue.get(wallet.address);
  if (retryInfo && retryInfo.nextRetryTime > Date.now()) {
    // console.log(`[Auth Retry Queue] Wallet ${wallet.address} is in retry queue. Next retry at ${new Date(retryInfo.nextRetryTime).toISOString()}`);
    throw new Error(`Authentication for ${wallet.address} is temporarily blocked. Retry after ${new Date(retryInfo.nextRetryTime).toISOString()}`);
  }

  // Check if authentication is already in progress for this wallet
  const existingAuthPromise = authInProgress.get(wallet.address);
  if (existingAuthPromise) {
    // console.log(`[Auth State] Authentication already in progress for ${wallet.address}, waiting for result`);
    try {
      const result = await existingAuthPromise;
      // console.log(`[Auth State] Received auth result for ${wallet.address}:`, result ? 'SUCCESS' : 'FAILED');
      return result;
    } catch {
      warnLog('API', `[Blur] Failed to wait for existing auth | wallet=${wallet.address}`);
      // Continue with our own auth attempt if the existing one failed
    }
  }

  // If no cached token and no auth in progress, start authentication
  // console.log(`[Auth State] Starting new authentication for ${wallet.address}`);

  // Create and store the authentication promise
  const authPromise = lockManager.withLock(lockKey, async () => {
    const options = { walletAddress: wallet.address };
    const headers = {
      'content-type': 'application/json',
      'x-api-key': API_KEY
    };

    // Double-check cache inside lock (race condition protection)
    try {
      const cachedToken = redis ? await redis.get(key) : null;
      if (cachedToken) {
        // console.log(`[Auth Cache] Found cached token after acquiring lock for ${wallet.address}`);
        return cachedToken;
      }
    } catch {
      warnLog('Cache', `[Blur] Failed to double-check cache | wallet=${wallet.address}`);
    }

    // console.log(`[Auth] Starting authentication for ${wallet.address}`);

    // Enhanced retry logic with exponential backoff for 408 errors
    let retries = 0;
    const maxRetries = 3;  // Back to 3 for speed
    let lastError: any;

    while (retries < maxRetries) {
      try {
        authMetrics.attempts++;

        let response: any = await limiter.schedule(() => axiosInstance
          .post(`${BLUR_API_URL}/auth/challenge`, options, { headers, timeout: 45000 }));
        const message = response.data.message;
        const signature = await wallet.signMessage(message);
        const data = {
          message: message,
          walletAddress: wallet.address,
          expiresOn: response.data.expiresOn,
          hmac: response.data.hmac,
          signature: signature
        };
        response = await limiter.schedule(() => axiosInstance
          .post(`${BLUR_API_URL}/auth/login`, data, { headers, timeout: 45000 }));
        const accessToken = response.data.accessToken;
        if (redis) {
          // Increase cache TTL from 5 minutes to 10 minutes
          await redis.set(key, accessToken, 'EX', 10 * 60);
          // console.log(`[Auth Cache] Successfully cached token for ${wallet.address} (TTL: 10 minutes)`);
        }

        authMetrics.successes++;

        // Track successful retry after 408
        if (retries > 0 && timeoutMetrics.auth408Errors > 0) {
          timeoutMetrics.successAfterRetry++;
        }

        // console.log(`[Auth] Successfully authenticated ${wallet.address}`);
        return accessToken;
      } catch (error: any) {
        lastError = error;
        authMetrics.failures++;
        authMetrics.lastFailure = new Date();

        // Track failure reasons
        const reason = error.response?.status ? `HTTP_${error.response.status}` : 'NETWORK_ERROR';
        authMetrics.failureReasons.set(reason, (authMetrics.failureReasons.get(reason) || 0) + 1);

        // Log the error details
        errorLog('Blur', `Auth attempt ${retries + 1}/${maxRetries} failed | wallet=${wallet.address} | status=${error.response?.status || 'N/A'} | message=${error.response?.data?.message || error.message}`, error);

        // Check for authentication block error
        const errorMessage = error.response?.data?.message || error.message || '';
        const authBlockMatch = errorMessage.match(/Authentication for .* is temporarily blocked\. Retry after (.+)/);
        if (authBlockMatch) {
          const retryAfterTimestamp = authBlockMatch[1];
          const retryAfterTime = new Date(retryAfterTimestamp).getTime();
          const waitTime = Math.max(retryAfterTime - Date.now(), 0);

          // console.log(RED + `[Auth Block] Wallet ${wallet.address} is blocked by Blur until ${retryAfterTimestamp}` + RESET);
          // console.log(RED + `[Auth Block] Need to wait ${Math.ceil(waitTime / 1000)} seconds` + RESET);

          // Store auth block in Redis
          if (redis) {
            const authBlockKey = `blur:auth:block:${wallet.address}`;
            const ttl = Math.ceil(waitTime / 1000) + 60; // Add 60s buffer
            await redis.set(authBlockKey, retryAfterTime.toString(), 'EX', ttl);
            // console.log(`[Auth Block] Stored block status in Redis with TTL ${ttl}s`);
          }

          // Clear from retry queue and auth in progress
          authRetryQueue.delete(wallet.address);
          authInProgress.delete(wallet.address);

          // Throw specific error for auth block
          throw new Error(`Authentication blocked until ${retryAfterTimestamp}`);
        }

        // Enhanced retry handling based on error type
        if (error.response?.status === 401) {
          // Unauthorized - token expired or invalid, clear cache and retry
          if (redis) {
            await redis.del(key);
            // console.log(`[Auth 401] Cleared cached token for ${wallet.address}, will retry authentication`);
          }
          // Clear auth in progress to allow fresh attempt
          authInProgress.delete(wallet.address);
          // Quick retry for token refresh
          const waitTime = 500 + (retries * 500); // 500ms, 1s, 1.5s
          await new Promise(resolve => setTimeout(resolve, waitTime));
        } else if (error.response?.status === 403) {
          if (redis) {
            await redis.del(key);
          }
          // Exponential backoff: 2s, 4s, 8s, 16s, 32s
          const waitTime = Math.min(Math.pow(2, retries + 1) * 1000, 32000);
          // console.log(`Waiting ${waitTime}ms before retry due to 403 error...`);
          await new Promise(resolve => setTimeout(resolve, waitTime));
        } else if (error.response?.status === 408) {
          // Track 408 errors for monitoring
          timeoutMetrics.total408Errors++;
          timeoutMetrics.auth408Errors++;
          timeoutMetrics.last408Error = new Date();
          timeoutMetrics.errorsByEndpoint.set('auth', (timeoutMetrics.errorsByEndpoint.get('auth') || 0) + 1);

          // Special handling for 408 timeout errors - much faster
          const baseDelay = 1000 + (retries * 500); // 1s, 1.5s, 2s
          const jitter = Math.random() * 200;
          const waitTime = baseDelay + jitter;
          // console.log(`[408 Monitoring] Auth timeout error (attempt ${retries + 1}/${maxRetries}), waiting ${waitTime}ms before retry...`);
          await new Promise(resolve => setTimeout(resolve, waitTime));
        } else if (retries < maxRetries - 1) {
          // For other errors, wait shorter time
          const waitTime = (retries + 1) * 1000;
          await new Promise(resolve => setTimeout(resolve, waitTime));
        }

        retries++;
      }
    }

    // If all retries failed, throw the last error
    errorLog('Blur', `Failed to get access token after ${maxRetries} attempts | wallet=${wallet.address}`, lastError);
    throw lastError || new Error('Failed to authenticate with Blur');

  });

  // Store the auth promise in the map before starting
  authInProgress.set(wallet.address, authPromise);

  try {
    // Execute the authentication
    const result = await authPromise;
    // console.log(`[Auth State] Authentication completed for ${wallet.address}`);

    // Clear from retry queue on success
    authRetryQueue.delete(wallet.address);

    return result;
  } catch (error) {
    errorLog('API', `[Blur] Authentication failed | wallet=${wallet.address}`, error);

    // Add to retry queue on failure
    const retryInfo = authRetryQueue.get(wallet.address) || { retryCount: 0, nextRetryTime: 0 };
    retryInfo.retryCount++;

    if (retryInfo.retryCount < MAX_AUTH_RETRIES) {
      retryInfo.nextRetryTime = Date.now() + AUTH_RETRY_DELAY;
      authRetryQueue.set(wallet.address, retryInfo);
      // console.log(`[Auth Retry Queue] Added ${wallet.address} to retry queue. Retry count: ${retryInfo.retryCount}/${MAX_AUTH_RETRIES}, next retry at ${new Date(retryInfo.nextRetryTime).toISOString()}`);
    } else {
      // console.log(`[Auth Retry Queue] Max retries reached for ${wallet.address}. Authentication permanently failed.`);
      authRetryQueue.delete(wallet.address);
    }

    throw error;
  } finally {
    // Always clean up the auth promise from the map
    authInProgress.delete(wallet.address);
    // console.log(`[Auth State] Cleaned up auth state for ${wallet.address}`);
  }
};

/**
 * Sends a request to format a bid on Blur.
 * @param url - The URL to send the request to.
 * @param accessToken - The access token for authentication.
 * @param walletAddress - The wallet address of the offerer.
 * @param buildPayload - The payload for the bid.
 * @returns The formatted bid data.
 */
async function formatBidOnBlur(
  taskId: string,
  url: string,
  accessToken: string,
  walletAddress: string,
  buildPayload: any,
  privateKey?: string
) {
  const controller = new AbortController();
  addActiveRequest(taskId, controller);

  // Check if limiter is initialized
  if (!limiter) {
    const error = new Error('Limiter not initialized');
    errorLog('API', `[Blur] Limiter not initialized | operation=format_bid | taskId=${taskId} | contract=${buildPayload.contractAddress}`, error);
    throw error;
  }

  let retries = 0;
  const maxRetries = 2;
  const formatStartTime = Date.now();

  while (retries <= maxRetries) {
    try {
      const { data } = await limiter.schedule(async () => {
        // Re-check blacklist AFTER waiting in limiter queue
        if (isTaskBlacklisted(taskId, 'blur')) {
          debugLog('InFlightBid', `FORMAT_BLOCKED_POST_QUEUE | taskId=${taskId} | marketplace=blur | waitTime=${Date.now() - formatStartTime}ms`, { taskId, marketplace: 'blur' });
          throw new Error('Task stopped while waiting in limiter queue');
        }

        return axiosInstance.request<BlurBidResponse>({
          method: 'POST',
          url: `${url}/v1/collection-bids/format`,
          headers: {
            'content-type': 'application/json',
            authToken: accessToken,
            walletAddress: walletAddress.toLowerCase(),
            'x-api-key': API_KEY,
          },
          data: JSON.stringify(buildPayload),
          signal: controller.signal,
          timeout: 60000,
          "axios-retry": { retries: 0 } // Disable automatic retries to prevent duplicate error logging
        });
      });

      // Record successful request for adaptive rate limiting
      blurRateLimiter.recordError(walletAddress, false);

      return data;
    } catch (error: any) {
      if (isCancel(error)) {
        // console.log(`Request for task ${taskId} was cancelled`);
        return null;
      }

      // Record error for adaptive rate limiting
      blurRateLimiter.recordError(walletAddress, true);

      // Track 408 errors for monitoring
      if (error.response?.status === 408) {
        timeoutMetrics.total408Errors++;
        timeoutMetrics.format408Errors++;
        timeoutMetrics.last408Error = new Date();
        timeoutMetrics.errorsByEndpoint.set('format', (timeoutMetrics.errorsByEndpoint.get('format') || 0) + 1);
        // console.log(`[408 Monitoring] Format bid timeout error for ${buildPayload.contractAddress}`);
      }

      // Handle 403 errors - refresh token and retry
      if (error.response?.status === 403 && retries < maxRetries && privateKey) {
        // console.log(`Got 403 error formatting Blur bid, refreshing token and retrying...`);

        // Clear the cached token
        const redis = redisClient.getClient();
        if (redis) {
          const key = `blurAccessToken:${walletAddress}`;
          await redis.del(key);
        }

        // Get a fresh token
        try {
          const newAccessToken = await getAccessToken(privateKey);
          if (newAccessToken) {
            accessToken = newAccessToken;
            retries++;
            continue; // Retry with new token
          }
        } catch (authError) {
          errorLog('API', `[Blur] Failed to refresh access token`, authError);
        }
      }

      // Check for transfer restriction error
      const responseMessage = error.response?.data?.message || '';
      if (responseMessage.includes('restricts transfers') || responseMessage.includes('cannot be bid on')) {
        // Add to restricted collections cache
        addRestrictedCollection(buildPayload.contractAddress);

        // console.log(`Blur: Collection ${buildPayload.contractAddress} restricts transfers - skipping bid`);
        throw new TransferRestrictedError('blur', buildPayload.contractAddress);
      }

      const errorMessage = `Failed to format Blur bid for collection ${buildPayload.contractAddress}: ${error.response?.data?.message || error.message || error}`;
      throw new InvalidRequestError('blur', errorMessage, error.response?.status);
    } finally {
      const controllers = activeRequests.get(taskId) || [];
      const index = controllers.indexOf(controller);
      if (index >= 0) {
        controllers.splice(index, 1);
      }
      if (controllers.length === 0) {
        activeRequests.delete(taskId);
      }
    }
  }
}

/**
 * Submits a bid to Blur.
 * @param url - The URL to send the request to.
 * @param accessToken - The access token for authentication.
 * @param walletAddress - The wallet address of the offerer.
 * @param submitPayload - The payload for the bid submission.
 * @param slug - The slug of the collection.
 */
async function submitBidToBlur(
  taskId: string,
  bidCount: string,
  offer_price: bigint,
  url: string,
  accessToken: string,
  walletAddress: string,
  submitPayload: SubmitPayload,
  slug: string,
  cancelPayload: any,
  expiry = 900,
  traits?: string
) {
  const controller = new AbortController();
  addActiveRequest(taskId, controller);

  let requestSuccessful = false;

  try {
    // Calculate formatted ETH price for logging
    const offerPriceEth = (Math.round(Number(formatUnits(offer_price.toString(), 18)) * 100) / 100).toFixed(2);

    const [parsedTaskId] = bidCount.split(":")
    const bidStartTime = Date.now();

    if (isTaskBlacklisted(parsedTaskId, 'blur')) {
      debugLog('InFlightBid', `BLOCKED_EARLY | taskId=${parsedTaskId} | marketplace=blur | reason=blacklisted`, { taskId: parsedTaskId, marketplace: 'blur' });
      return;
    }
    const currentTask = activeTasks.get(parsedTaskId)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("BLUR".toLowerCase())) return
    let running = currentTasks.find((task) => task.contract.slug.toLowerCase() === slug.toLowerCase())
    if (!running) return

    debugLog('InFlightBid', `ENTERING_LIMITER | taskId=${parsedTaskId} | marketplace=blur | collection=${slug} | timestamp=${bidStartTime}`, { taskId: parsedTaskId, marketplace: 'blur' });

    // Try to reserve a request slot atomically
    let reserved = false;
    let retryCount = 0;

    while (!reserved && retryCount < 3) {
      reserved = blurRateLimiter.reserveRequest(walletAddress);

      if (!reserved) {
        const waitTime = Math.max(blurRateLimiter.getWaitTime(walletAddress), 2000); // Wait at least 2 seconds
        // console.log(`Blur rate limit: Waiting ${waitTime}ms for wallet ${walletAddress} (attempt ${retryCount + 1}/3)`);
        await new Promise(resolve => setTimeout(resolve, waitTime));
        retryCount++;
      }
    }

    // If couldn't reserve after retries, throw error
    if (!reserved) {
      const error = new Error('Blur rate limit exceeded after retries - could not reserve request slot');
      throw new InvalidRequestError('blur', error.message, 429);
    }

    // Now make the request with the reserved slot
    try {
      let response;
      let submitRetries = 0;
      const maxSubmitRetries = 2;

      while (submitRetries <= maxSubmitRetries) {
        try {
          response = await limiter.schedule(async () => {
            // Re-check blacklist AFTER waiting in limiter queue - this is the critical fix
            if (isTaskBlacklisted(parsedTaskId, 'blur')) {
              debugLog('InFlightBid', `BLOCKED_POST_QUEUE | taskId=${parsedTaskId} | marketplace=blur | waitTime=${Date.now() - bidStartTime}ms`, { taskId: parsedTaskId, marketplace: 'blur' });
              throw new Error('Task stopped while waiting in limiter queue');
            }

            debugLog('InFlightBid', `EXECUTING_REQUEST | taskId=${parsedTaskId} | marketplace=blur | waitTime=${Date.now() - bidStartTime}ms`, { taskId: parsedTaskId, marketplace: 'blur' });

            return axiosInstance.request({
              method: 'POST',
              url: `${url}/v1/collection-bids/submit`,
              headers: {
                'content-type': 'application/json',
                authToken: accessToken,
                walletAddress: walletAddress.toLowerCase(),
                'x-api-key': API_KEY,
              },
              data: JSON.stringify(submitPayload),
              signal: controller.signal,
              timeout: 60000,
              "axios-retry": { retries: 0 } // Disable automatic retries to prevent duplicate error logging
            });
          });
          break; // Success, exit retry loop
        } catch (submitError: any) {
          // Handle 403 errors
          if (submitError.response?.status === 403 && submitRetries < maxSubmitRetries) {
            // console.log(`Got 403 error submitting Blur bid (attempt ${submitRetries + 1}/${maxSubmitRetries + 1})`);

            // Wait before retry with exponential backoff
            const waitTime = Math.pow(2, submitRetries) * 1000;
            await new Promise(resolve => setTimeout(resolve, waitTime));

            submitRetries++;
            continue;
          }

          // If not a 403 or retries exhausted, throw the error
          throw submitError;
        }
      }

      if (!response) {
        throw new Error('Failed to get response from Blur API after retries');
      }

      const headers = response.headers;

      // Try both header formats (with and without x- prefix)
      const remaining = parseInt(headers['ratelimit-remaining'] || headers['x-ratelimit-remaining'] || headers['x-ratelimit-remaining-second'] || '40', 10);
      const reset = parseInt(headers['ratelimit-reset'] || headers['x-ratelimit-reset'] || '10', 10);
      const limit = parseInt(headers['ratelimit-limit'] || headers['x-ratelimit-limit'] || headers['x-ratelimit-limit-second'] || '100', 10);

      // Update rate limiter with actual values from API
      if (!isNaN(remaining) && !isNaN(reset) && !isNaN(limit)) {
        blurRateLimiter.updateFromHeaders(walletAddress, {
          remaining,
          reset,
          limit
        });
      }
      const offers = response.data

      if (offers.success) {
        const successMessage = traits ? `🎉 TRAIT OFFER POSTED TO BLUR SUCCESSFULLY FOR: ${slug.toUpperCase()} 🎉 AMOUNT: ${offerPriceEth} BETH 🎉 TRAIT: ${traits}` : `🎉 OFFER POSTED TO BLUR SUCCESSFULLY FOR: ${slug.toUpperCase()} 🎉 AMOUNT: ${offerPriceEth} BETH 🎉`
        console.log(GOLD + successMessage + RESET);

        // Record successful submission for adaptive rate limiting
        blurRateLimiter.recordError(walletAddress, false);

        let identifier = ''
        if (traits) {
          const traitsObj = JSON.parse(traits);
          const [traitType, traitValue] = Object.entries(traitsObj)[0];
          identifier = `${traitType}:${traitValue}`
        } else {
          identifier = "collection"
        }

        const expiresAt = new Date(Date.now() + expiry * 1000);
        const orderData = {
          slug: slug,
          orderHash: JSON.stringify(cancelPayload),
          expiresAt: expiresAt,
          offerPrice: offer_price.toString(),
          identifier: identifier,
          taskId: taskId,
          marketplace: "blur"
        };
        await withRetry(async () => {
          const savedOrder = await Order.create(orderData);
          // Update in-memory cache
          orderCache.set({
            _id: savedOrder._id.toString(),
            orderHash: savedOrder.orderHash,
            expiresAt: savedOrder.expiresAt.getTime(),
            offerPrice: savedOrder.offerPrice,
            createdAt: savedOrder.createdAt.getTime(),
            identifier: savedOrder.identifier,
            marketplace: savedOrder.marketplace,
            slug: savedOrder.slug,
            taskId: savedOrder.taskId
          });
        })
        trackBidRate("blur", taskId)

        // Mark request as successful
        requestSuccessful = true;
      }

      if (offers.errors) {
        errorLog('API', `[Blur] API returned errors | taskId=${taskId} | slug=${slug} | errors=${JSON.stringify(offers.errors)}`);
      }
    } catch (error: any) {
      // Record error for adaptive rate limiting
      blurRateLimiter.recordError(walletAddress, true);

      // Track 408 errors for monitoring
      if (error.response?.status === 408) {
        timeoutMetrics.total408Errors++;
        timeoutMetrics.submit408Errors++;
        timeoutMetrics.last408Error = new Date();
        timeoutMetrics.errorsByEndpoint.set('submit', (timeoutMetrics.errorsByEndpoint.get('submit') || 0) + 1);
        // console.log(`[408 Monitoring] Submit bid timeout error for ${slug}`);
      }

      if (isCancel(error)) {
        // console.log(`Request for task ${taskId} was cancelled`);
        return;
      }

      // Check for rate limit error in different formats
      const errorData = error.response?.data;
      const isRateLimitError = error.response?.status === 429 ||
        error.response?.status === 400 && (
          errorData?.message?.includes('rate limit') ||
          (Array.isArray(errorData) && errorData[0]?.includes('rate limit'))
        );

      if (isRateLimitError) {
        // Extract rate limit info from response headers
        const headers = error.response?.headers || {};
        const retryAfter = headers['retry-after'];
        const remaining = parseInt(headers['ratelimit-remaining'] || headers['x-ratelimit-remaining'] || headers['x-ratelimit-remaining-second'] || '0', 10);
        const reset = parseInt(headers['ratelimit-reset'] || headers['x-ratelimit-reset'] || '10', 10);
        const limit = parseInt(headers['ratelimit-limit'] || headers['x-ratelimit-limit'] || headers['x-ratelimit-limit-second'] || '100', 10);

        // Update rate limiter with actual values
        if (!isNaN(remaining) && !isNaN(reset) && !isNaN(limit)) {
          blurRateLimiter.updateFromHeaders(walletAddress, {
            remaining,
            reset,
            limit
          });
        }

        blurRateLimiter.markRateLimited(walletAddress, retryAfter ? parseInt(retryAfter) : reset);

        // Enhanced rate limit logging
        // console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
        // console.log(RED + `[Blur API Rate Limit Detected]` + RESET);
        // console.log(RED + `Wallet: ${walletAddress}` + RESET);
        // console.log(RED + `Remaining Requests: ${remaining}/${limit}` + RESET);
        // console.log(RED + `Reset In: ${reset} seconds` + RESET);
        // console.log(RED + `Retry After: ${retryAfter || 'Not specified'}` + RESET);
        // console.log(RED + `Collection: ${slug}` + RESET);
        // console.log(RED + `Response: ${error.response?.data?.message || 'Rate limit exceeded'}` + RESET);
        // console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);

        // Store wallet pause info for processBlurJob to check
        const pauseDuration = retryAfter ? parseInt(retryAfter) * 1000 : 60000; // Default to 60 seconds if no retry-after
        await withRetry(async () => {
          const redis = redisClient.getClient();
          if (redis) {
            const pauseKey = `blur:wallet:pause:${walletAddress}`;
            const pauseUntil = Date.now() + pauseDuration;
            await redis.set(pauseKey, pauseUntil.toString(), 'PX', pauseDuration);

            // console.log(GOLD + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
            // console.log(GOLD + `[Blur Rate Limit] Setting Wallet Pause` + RESET);
            // console.log(GOLD + `Wallet: ${walletAddress}` + RESET);
            // console.log(GOLD + `Pause Duration: ${pauseDuration}ms (${Math.round(pauseDuration / 1000)}s)` + RESET);
            // console.log(GOLD + `Resume At: ${new Date(pauseUntil).toLocaleTimeString()}` + RESET);
            // console.log(GOLD + `Redis Key: ${pauseKey}` + RESET);
            // console.log(GOLD + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          }
        });
      }

      const formattedPrice = (Math.round(Number(formatUnits(offer_price.toString(), 18)) * 100) / 100).toFixed(2);
      const errorMessage = `Failed to submit Blur bid for collection ${slug} at price ${formattedPrice} BETH: ${Array.isArray(errorData) ? errorData[0] : errorData?.message || error.message || error}`;
      errorLog('API', `[Blur] ${errorMessage} | taskId=${taskId} | status=${error.response?.status} | rateLimit=${isRateLimitError}`, error);
      throw new InvalidRequestError('blur', errorMessage, error.response?.status);
    }
  } finally {
    // Always release the reserved request slot
    blurRateLimiter.releaseRequest(walletAddress, requestSuccessful);

    const controllers = activeRequests.get(taskId) || [];
    const index = controllers.indexOf(controller);
    if (index >= 0) {
      controllers.splice(index, 1);
    }
    if (controllers.length === 0) {
      activeRequests.delete(taskId);
    }
  }
}

export async function cancelBlurBid(data: BlurCancelPayload) {
  let walletAddress: string | undefined;
  try {
    if (!data || !data.payload || !data.privateKey) return
    const { payload, privateKey, taskId } = data
    const wallet = new Wallet(privateKey, provider);
    walletAddress = wallet.address
    const accessToken = await getAccessToken(privateKey);
    const endpoint = `${BLUR_API_URL}/v1/collection-bids/cancel`
    const { data: cancelResponse } = await limiter.schedule(() => axiosInstance.post(endpoint, payload, {
      headers: {
        'content-type': 'application/json',
        authToken: accessToken,
        walletAddress: walletAddress!.toLowerCase(),
        'x-api-key': API_KEY,
      },
      timeout: 60000
    }))
    decrementBidCount('blur', taskId);
    if (cancelResponse) {
      // console.log(JSON.stringify(cancelResponse));
    }

    // Track cancellation completion
    cancellationTracker.recordCompletion(taskId, 1, 'blur');

    // Update cancellation percentage using centralized function
    await updateTaskCancellationPercentage(taskId, undefined, 'blur');
  } catch (error: any) {
    if (error.response?.data?.message?.message !== 'No bids found') {
      errorLog('API', `[Blur] Failed to cancel bid | taskId=${data.taskId} | wallet=${walletAddress || 'unknown'} | status=${error?.response?.status}`, error);
    }
  }
}

export async function fetchBlurBid(collection: string, criteriaType: 'TRAIT' | 'COLLECTION', criteriaValue: Record<string, string>) {
  const url = `${BASE_URL}/blur/v1/collections/${collection}/executable-bids`;

  if (criteriaType === 'TRAIT') {
    const tasks = [
      () => apiDedup(
        `blur-bids-trait-${collection}-${JSON.stringify(criteriaValue)}`,
        () => limiter.schedule(() => axiosInstance.get<BlurBidResponse>(url, {
          params: {
            filters: JSON.stringify({
              criteria: {
                type: 'TRAIT',
                value: criteriaValue
              }
            })
          },
          headers: {
            'content-type': 'application/json',
            'x-api-key': API_KEY,
          },
          timeout: 60000
        })),
        { category: 'Blur' }
      ),
      () => apiDedup(
        `blur-bids-collection-${collection}`,
        () => limiter.schedule(() => axiosInstance.get<BlurBidResponse>(url, {
          params: {
            filters: JSON.stringify({
              criteria: {
                type: 'COLLECTION',
                value: {}
              }
            })
          },
          headers: {
            'content-type': 'application/json',
            'x-api-key': API_KEY,
          },
          timeout: 60000
        })),
        { category: 'Blur' }
      )
    ];

    const [traitResponse, collectionResponse] = await pQueue.addAll(tasks, {
      throwOnTimeout: true,
    });

    const traitData = traitResponse.data;
    const collectionData = collectionResponse.data;

    // Combine trait and collection price levels
    const combinedData = {
      success: traitData.success && collectionData.success,
      priceLevels: [
        ...(traitData.priceLevels || []),
        ...(collectionData.priceLevels || [])
      ].sort((a, b) => Number(b.price) - Number(a.price))
    };

    return combinedData;

  } else {
    const { data } = await apiDedup(
      `blur-bids-collection-${collection}`,
      () => limiter.schedule(() => axiosInstance.get<BlurBidResponse>(url, {
        params: {
          filters: JSON.stringify({
            criteria: {
              type: 'COLLECTION',
              value: {}
            }
          })
        },
        headers: {
          'content-type': 'application/json',
          'x-api-key': API_KEY,
        },
        timeout: 60000
      })),
      { category: 'Blur' }
    );
    return data;
  }

}

export async function fetchBlurCollectionStats(slug: string) {
  const CACHE_TTL = 30; // 30 seconds cache
  const cacheKey = `blur:stats:${slug}`;
  const lockKey = `stats:${slug}`;

  // Try to get from cache first
  try {
    const redis = redisClient.getClient();
    if (redis) {
      const cachedStats = await redis.get(cacheKey);
      if (cachedStats) {
        return JSON.parse(cachedStats);
      }
    }
  } catch {
    warnLog('Cache', `[Blur] Error reading stats from Redis | slug=${slug}`);
  }

  // If not in cache, try to acquire lock and fetch
  const result = await lockManager.withLock(lockKey, async () => {

    const redis = redisClient.getClient();
    if (redis) {
      const cachedStats = await redis.get(cacheKey);
      if (cachedStats) {
        return JSON.parse(cachedStats);
      }
    }

    try {
      const url = `${BASE_URL}/blur/v1/collections/${slug}/tokens`;
      const { data } = await apiDedup(
        `blur-stats-${slug}`,
        () => limiter.schedule({ priority: 0 }, () => axiosInstance.get<BlurTokensResponse>(url, {
          headers: {
            'content-type': 'application/json',
            'x-api-key': API_KEY,
          },
          timeout: 60000
        })),
        { category: 'Blur' }
      );

      const listings = data?.tokens?.sort((a, b) => Number(a?.price?.amount) - Number(b?.price?.amount));
      const floor_price = Number(listings[0]?.price?.amount) || 0;

      if (redis) {
        await redis.set(cacheKey, JSON.stringify(floor_price), 'EX', CACHE_TTL);
      }
      return floor_price || 0;
    } catch (error: any) {
      errorLog('Collection', `[Blur] Failed to fetch collection stats | slug=${slug} | status=${error?.response?.status}`, error);
      return 0;
    }

  });

  if (!result) {
    return 0
  }
  return result;
}

export async function fetchBlurTraitFloor(slug: string, trait: { type: string; value: string; }) {
  try {
    const filters = {
      traits: [{
        type: trait.type,
        values: [trait.value],
      }],
      hasAsks: true,
    };
    const encodedFilters = encodeURIComponent(JSON.stringify(filters));

    const url = `${BASE_URL}/blur/v1/collections/${slug}/tokens?filters=${encodedFilters}`;
    const { data } = await apiDedup(
      `blur-trait-floor-${slug}-${trait.type}-${trait.value}`,
      () => limiter.schedule(() =>
        axiosInstance.get<BlurTokensResponse>(url, {
          headers: {
            'x-api-key': API_KEY,
          },
          timeout: 60000
        })
      ),
      { category: 'Blur' }
    );
    const amount = data?.tokens[0]?.price?.amount;
    return amount !== undefined ? Number(amount) : undefined
  } catch (error: any) {
    errorLog('API', `[Blur] Failed to fetch trait floor | slug=${slug} | trait=${trait.type}:${trait.value} | status=${error?.response?.status}`, error);
    throw error;
  }
}

interface PriceLevel {
  criteriaType: string;
  criteriaValue: Record<string, unknown>;
  price: string;
  executableSize: number;
  numberBidders: number;
  bidderAddressesSample: any[];
}

interface BlurBidResponse {
  success: boolean;
  priceLevels: PriceLevel[];
}

interface BlurCancelPayload {
  payload: {
    contractAddress: string;
    criteriaPrices: Array<{
      price: string;
      criteria?: {
        type: string;
        value: Record<string, string>;
      }
    }>;
  };
  privateKey: string;
  taskId: string;
}

interface SubmitPayload {
  signature: string;
  marketplaceData: string[];
  [key: string]: any;
}

export interface BlurTokensResponse {
  success: boolean;
  contractAddress: string;
  totalCount: number;
  tokens: IBlurToken[];
}

export interface IBlurToken {
  tokenId: string;
  name: string;
  imageUrl: string;
  traits: {
    Tier?: string;
    State: string;
    [key: string]: string | undefined;
  };
  rarityScore: number;
  rarityRank: number;
  price: TokenPrice | null;
  highestBid: TokenPrice | null;
  lastSale: TokenPrice | null;
  lastCostBasis: TokenPrice | null;
  owner: {
    address: string;
    username: string | null;
  };
  numberOwnedByOwner: number;
  isSuspicious: boolean;
}

export interface TokenPrice {
  amount: string;
  unit: string;
  listedAt: string;
  marketplace?: string;
}

export function getAuthMetrics() {
  return {
    ...authMetrics,
    successRate: authMetrics.attempts > 0 ? (authMetrics.successes / authMetrics.attempts) * 100 : 0,
    failureReasons: Array.from(authMetrics.failureReasons.entries()).map(([reason, count]) => ({
      reason,
      count
    }))
  };
}

export function getTimeoutMetrics() {
  return {
    ...timeoutMetrics,
    errorsByEndpoint: Array.from(timeoutMetrics.errorsByEndpoint.entries()).map(([endpoint, count]) => ({
      endpoint,
      count
    })),
    retrySuccessRate: timeoutMetrics.total408Errors > 0
      ? (timeoutMetrics.successAfterRetry / timeoutMetrics.total408Errors) * 100
      : 0,
  };
}
