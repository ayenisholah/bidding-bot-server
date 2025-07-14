import { Wallet } from "ethers";
import { BASE_URL } from "../../constants";
import { approveNFT } from "../../functions/approve";
import { axiosInstance } from "../../init";
import Listing from "../../models/listing.model";
import { logger } from "../../utils/logger";
import Bottleneck from "bottleneck";
import { convertDurationForMagicEden } from "../../utils/duration";

const API_KEY = process.env.API_KEY;

// Create a separate rate limiter for Magic Eden
// Magic Eden has a strict limit: 20 requests per 60 seconds
const magicEdenLimiter = new Bottleneck({
  minTime: 3000,        // 3 seconds between requests (20 req/60s = 1 req/3s)
  maxConcurrent: 1,     // Only 1 concurrent request at a time
  reservoir: 20,        // Maximum 20 requests
  reservoirRefreshAmount: 20,
  reservoirRefreshInterval: 60 * 1000, // Refill every 60 seconds
});

/**
 * Lists an NFT on Magic Eden marketplace
 * 
 * @param walletAddress - The wallet address of the NFT owner
 * @param contract - The contract address of the NFT collection
 * @param tokenId - The token ID of the NFT to list
 * @param listingPrice - The listing price in ETH (e.g., 1.5 for 1.5 ETH)
 * @param listDuration - Duration object containing value and unit
 * @param wallet - The wallet instance used for signing the listing transaction
 * @returns Promise<any|null> - Returns the listing response data on success, null on failure
 * 
 * @example
 * ```typescript
 * await listOnMagicEden(
 *   "0x1234567890123456789012345678901234567890",
 *   "0x524cab2ec69124574082676e6f654a18df49a048",
 *   "1234",
 *   1.5, // 1.5 ETH
 *   { value: 24, unit: "hours" },
 *   wallet
 * );
 * ```
 * 
 * @description
 * This function handles the complete Magic Eden listing process:
 * 1. Approves the NFT for Magic Eden's Payment Processor V2 contract
 * 2. Creates a listing payload with wei-converted price
 * 3. Calls Magic Eden's execute/list endpoint to get signing data
 * 4. Signs the typed data with the wallet
 * 5. Submits the signed order to Magic Eden
 * 6. Updates the database with listing details and expiration
 * 
 * The function uses Magic Eden's Payment Processor V2 orderbook system
 * and handles automated royalties through their platform.
 */
export async function listOnMagicEden(walletAddress: string, contract: string, tokenId: string, listingPrice: number, listDuration: { value: number; unit: string }, wallet: Wallet) {
  const listEndpoint =
    `${BASE_URL}/magiceden/v3/rtp/ethereum/execute/list/v5`;
  const signEndpoint =
    `${BASE_URL}/magiceden/v3/rtp/ethereum/order/v4`;
  const headers = {
    'x-api-key': API_KEY
  };
  const roundedPrice = Math.round(listingPrice * 10000) / 10000;
  const weiPrice = (roundedPrice * 1e18).toString()

  // Convert duration to seconds using Magic Eden-specific converter
  const durationInSeconds = convertDurationForMagicEden(listDuration);
  console.log(`[MAGICEDEN] Duration: ${listDuration.value} ${listDuration.unit} = ${durationInSeconds} seconds`);

  const expirationTime = (
    Math.floor(Date.now() / 1000) + durationInSeconds
  ).toString()

  try {

    await approveNFT(
      contract,
      wallet,
      '0x9A1D00bEd7CD04BCDA516d721A596eb22Aac6834'
    );

    const listPayload = {
      maker: walletAddress,
      source: "magiceden.io",
      params: [
        {
          token: `${contract}:${tokenId}`,
          weiPrice: weiPrice,
          orderbook: "reservoir",
          orderKind: "payment-processor-v2",
          quantity: 1,
          currency: "0x0000000000000000000000000000000000000000",
          expirationTime: expirationTime,
          automatedRoyalties: false,
          options: {
            "payment-processor-v2": { useOffChainCancellation: true },
          },
        },
      ],
    };

    const listResponse = await magicEdenLimiter.schedule(() =>
      axiosInstance.post(
        listEndpoint,
        listPayload,
        { headers }
      )
    );

    const signStep = listResponse.data.steps.find(
      (step: any) => step.id === "order-signature"
    );

    if (!signStep) {
      await logger.logMagicEdenListingError('No order-signature step found in response', {
        walletAddress,
        contractAddress: contract,
        tokenId,
        operation: 'create_listing',
        availableSteps: listResponse.data.steps?.map((s: any) => s.id),
        errorCode: 'MISSING_SIGNATURE_STEP'
      });
      console.error('No order-signature step found in response');
      console.error('Available steps:', listResponse.data.steps);
      throw new Error('No order-signature step found');
    }

    const signData = signStep.items[0].data.sign;

    const signature = await wallet.signTypedData(
      signData.domain,
      signData.types,
      signData.value
    );

    const { seller, marketplace, ...restOfSignData } = signData.value;

    // Override marketplace address to Magic Eden's expected value
    const correctMarketplace = "0xca9337244b5f04cb946391bc8b8a980e988f9a6a";

    if (marketplace && marketplace !== correctMarketplace) {
      console.log(`Overriding marketplace address from ${marketplace} to ${correctMarketplace}`);
    }

    const order = {
      items: [
        {
          order: {
            kind: "payment-processor-v2",
            data: {
              kind: "sale-approval",
              sellerOrBuyer: seller,
              ...restOfSignData,
              marketplace: correctMarketplace, // Use the correct marketplace address
              r: "0x0000000000000000000000000000000000000000000000000000000000000000",
              s: "0x0000000000000000000000000000000000000000000000000000000000000000",
              v: 0,
            },
          },
          orderbook: "reservoir",
        },
      ],
      source: "magiceden.io",
    };

    const finalListingResponse = await magicEdenLimiter.schedule(() =>
      axiosInstance.post(
        `${signEndpoint}?signature=${encodeURIComponent(signature)}`,
        order,
        { headers }
      )
    );

    console.log("NFT listed successfully on Magic Eden");

    // Update finalListingPrice.magiceden and expiration in the database
    try {
      const expirationTime = new Date(Date.now() + durationInSeconds * 1000);
      const updatedListing = await Listing.findOneAndUpdate(
        {
          "contract.contractAddress": contract,
          tokenId: tokenId
        },
        [
          {
            $set: {
              "finalListingPrice.magiceden": roundedPrice,
              "listingExpiration.magiceden": expirationTime,
              lastListedAt: {
                $mergeObjects: [
                  { $cond: [{ $eq: [{ $type: "$lastListedAt" }, "object"] }, "$lastListedAt", {}] },
                  { magiceden: new Date() }
                ]
              },
              listed: true,
              // Extend instant price matching if it's active
              instantPriceMatchingActiveUntil: {
                $cond: [
                  { $and: [
                    { $ne: ["$instantPriceOverride", null] },
                    { $gt: ["$instantPriceMatchingActiveUntil", new Date()] }
                  ]},
                  expirationTime,
                  "$instantPriceMatchingActiveUntil"
                ]
              }
            }
          }
        ],
        { new: true }
      );

      if (updatedListing) {
        console.log(`Updated listing ${contract} #${tokenId} with magiceden price: ${roundedPrice} ETH, expires at: ${expirationTime.toISOString()}`);
        if (updatedListing.instantPriceOverride && updatedListing.instantPriceMatchingActiveUntil) {
          console.log(`Extended instant price matching until: ${updatedListing.instantPriceMatchingActiveUntil.toISOString()}`);
        }
      }
    } catch (updateError) {
      await logger.logMagicEdenListingError('Error updating listing in database', {
        contractAddress: contract,
        tokenId,
        operation: 'update_listing_price',
        price: roundedPrice,
        error: updateError as Error,
        errorMessage: updateError instanceof Error ? updateError.message : String(updateError)
      });
      console.error('Error updating listing in database:', updateError);
    }

    return finalListingResponse.data;
  } catch (error: any) {
    // Extract the specific error details
    const errorData = error?.response?.data;
    const errorStatus = error?.response?.status;
    
    // Format the error message for better readability
    let formattedError = "Magic Eden listing failed";
    if (errorData?.results?.[0]?.message) {
      const result = errorData.results[0];
      if (result.message === 'invalid-signature') {
        formattedError = `Magic Eden: Invalid signature for order ${result.orderId || 'unknown'}`;
      } else {
        formattedError = `Magic Eden: ${result.message}`;
      }
    } else if (errorData?.message) {
      formattedError = `Magic Eden: ${errorData.message}`;
    }

    await logger.logMagicEdenListingError(formattedError, {
      walletAddress,
      contractAddress: contract,
      tokenId,
      price: roundedPrice,
      operation: 'submit_listing',
      error: error,
      errorCode: errorStatus,
      errorMessage: errorData || error?.message || String(error),
      responseData: errorData
    });
    
    // Display graceful error message
    console.error(`❌ ${formattedError}`);
    if (errorData?.results?.[0]) {
      const result = errorData.results[0];
      console.error(`   Contract: ${contract}`);
      console.error(`   Token ID: ${tokenId}`);
      console.error(`   Price: ${roundedPrice} ETH`);
      if (result.orderId) {
        console.error(`   Order ID: ${result.orderId}`);
      }
    }

    // Check if the error is because user no longer owns the NFT
    const errorMessage = JSON.stringify(error?.response?.data || error?.message || '').toLowerCase();
    if (errorMessage.includes('not owned') || errorMessage.includes('ownership') || errorMessage.includes('not the owner') || errorMessage.includes('unauthorized')) {
      console.log(`User no longer owns NFT ${contract}/${tokenId}, deleting listing...`);

      try {
        await Listing.findOneAndDelete({
          "contract.contractAddress": contract,
          tokenId: tokenId
        });
        console.log(`Deleted listing for ${contract}/${tokenId} as user no longer owns it`);
      } catch (deleteError) {
        console.error('Error deleting listing:', deleteError);
      }
    }

    return null;
  }
}