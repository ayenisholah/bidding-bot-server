import { config } from "dotenv";

const GOLD = '\x1b[33m';
const RESET = '\x1b[0m';
import { approveNFT } from "../../functions/approve";
import { BASE_URL } from "../../constants";
import { axiosInstance, limiter } from "../../init";
import { Wallet } from "ethers";
import Listing from "../../models/listing.model";
import { convertDurationForBlur } from "../../utils/duration";
import { listingPriceCache } from "../../utils/listing-price-cache";
import { roundToNineDecimals } from "../../utils/decimal-precision";
import { getAccessToken } from "./bid";
import { getCollectionDetails } from "../../functions/collection";
import { IFee } from "../../types/listing.types";

config()
const API_KEY = process.env.API_KEY;

/**
 * Lists an NFT on Blur marketplace
 * 
 * @param nftAddress - The contract address of the NFT collection
 * @param tokenId - The token ID of the NFT to list
 * @param price - The listing price as a string in ETH (e.g., "1.5" for 1.5 ETH)
 * @param wallet - The wallet instance used for signing the listing transaction
 * @param privateKey - Private key for fetching Blur auth token
 * @param contractSlug - The collection slug for fetching collection details
 * @param listDuration - Duration object containing value and unit
 * @returns Promise<void> - Resolves when the listing is successfully created or throws an error
 * 
 * @example
 * ```typescript
 * await listOnBlur(
 *   "0x524cab2ec69124574082676e6f654a18df49a048",
 *   "1234",
 *   "1.5", // 1.5 ETH as string
 *   wallet,
 *   privateKey,
 *   "cool-cats-nft",
 *   { value: 24, unit: "hours" }
 * );
 * ```
 * 
 * @description
 * This function handles the complete Blur listing process:
 * 1. Fetches Blur access token
 * 2. Fetches collection details for creator fees
 * 3. Approves the NFT for Blur's contract
 * 4. Formats the order via Blur API
 * 5. Signs the typed data with the wallet
 * 6. Submits the signed order to Blur
 * 7. Updates the database with listing details
 */
export async function listOnBlur(
  nftAddress: string,
  tokenId: string,
  price: string,
  wallet: Wallet,
  privateKey: string,
  contractSlug: string,
  listDuration: { value: number; unit: string }) {

  try {
    // Fetch Blur access token
    console.log(`${GOLD}[BLUR] Fetching access token...${RESET}`);
    let authToken: string;
    try {
      const token = await getAccessToken(privateKey);
      if (!token) {
        throw new Error('Failed to get Blur access token');
      }
      authToken = token;
      console.log(`${GOLD}[BLUR] Access token fetched successfully${RESET}`);
    } catch (tokenError) {
      console.error(`${GOLD}[BLUR] Failed to fetch access token${RESET}`, tokenError);
      throw new Error(`Blur access token fetch failed: ${tokenError instanceof Error ? tokenError.message : 'Unknown error'}`);
    }
    
    // Fetch collection details for creator fees
    console.log(`${GOLD}[BLUR] Fetching collection details for ${contractSlug}...${RESET}`);
    let feeRate = 0;
    try {
      const collectionData = await getCollectionDetails(contractSlug, nftAddress);
      const creatorFees = collectionData?.creator_fees as IFee;
      const enforceCreatorFee = collectionData?.enforceCreatorFee as boolean;
      const totalCreatorFee = enforceCreatorFee && creatorFees ? Object.values(creatorFees).reduce((sum, fee) => sum + fee, 0) : 0;
      feeRate = totalCreatorFee / 10000; // Convert basis points to decimal
      console.log(`${GOLD}[BLUR] Collection details fetched - Total creator fee: ${totalCreatorFee} (${feeRate * 100}%)${RESET}`);
    } catch (error) {
      console.error(`${GOLD}[BLUR] Error fetching collection details, proceeding without creator fees${RESET}`);
    }
    
    await approveNFT(
      nftAddress,
      wallet,
      '0x2f18f339620a63e43f0839eeb18d7de1e1be4dfb'
    );

    // Convert duration to seconds using Blur-specific converter
    const durationInSeconds = convertDurationForBlur(listDuration);
    console.log(`${GOLD}[BLUR] Duration: ${listDuration.value} ${listDuration.unit} = ${durationInSeconds} seconds${RESET}`);
    
    // Add buffer time to account for processing delays and API latency
    const bufferSeconds = 120; // 120 second buffer to ensure we meet Blur's requirements
    const finalDuration = durationInSeconds + bufferSeconds;
    
    // Calculate expiration time right before sending to ensure freshness
    const durationMs = finalDuration * 1000;
    const nowTimestamp = Date.now();
    const expirationTimestamp = nowTimestamp + durationMs;
    const expirationTime = new Date(expirationTimestamp).toISOString();

    console.log(`${GOLD}=== Blur Expiration Time Calculation ===${RESET}`);
    console.log(`${GOLD}Now: ${new Date(nowTimestamp).toISOString()}${RESET}`);
    console.log(`${GOLD}Duration requested (seconds): ${durationInSeconds}${RESET}`);
    console.log(`${GOLD}Buffer added (seconds): ${bufferSeconds}${RESET}`);
    console.log(`${GOLD}Final duration (seconds): ${finalDuration}${RESET}`);
    console.log(`${GOLD}Final duration (minutes): ${finalDuration / 60}${RESET}`);
    console.log(`${GOLD}Expiration time: ${expirationTime}${RESET}`);
    console.log(`${GOLD}Time until expiration (minutes): ${(expirationTimestamp - nowTimestamp) / (1000 * 60)}${RESET}`);

    // Recalculate expiration time just before sending to minimize delay
    const requestNowTimestamp = Date.now();
    const requestExpirationTimestamp = requestNowTimestamp + durationMs;
    const requestExpirationTime = new Date(requestExpirationTimestamp).toISOString();
    
    console.log(`${GOLD}Request time calculation:${RESET}`);
    console.log(`${GOLD}- Request sent at: ${new Date(requestNowTimestamp).toISOString()}${RESET}`);
    console.log(`${GOLD}- Time since initial calculation (ms): ${requestNowTimestamp - nowTimestamp}${RESET}`);
    console.log(`${GOLD}- Updated expiration time: ${requestExpirationTime}${RESET}`);

    const options = {
      method: 'POST',
      url: `${BASE_URL}/blur/v1/orders/format`,
      headers: {
        'content-type': 'application/json',
        authToken: authToken,
        walletAddress: wallet.address,
        'x-api-key': API_KEY
      },
      data: {
        marketplace: 'BLUR',
        orders: [
          {
            price: {
              amount: price,
              unit: 'ETH'
            },
            tokenId: tokenId,
            feeRate: feeRate,
            contractAddress: nftAddress,
            expirationTime: requestExpirationTime
          }
        ]
      }
    };

    const data = await limiter.schedule(() => axiosInstance
      .request(options))
      .then((res) => res.data)
      .catch((err) => {
        console.error('Error formatting Blur order:', err?.response?.data || err.message);
        return { errors: err?.response?.data ?? err };
      });

    if (data?.errors) {
      // Format the error message for better readability
      let formattedError = "Blur listing failed";
      const errorMsg = data.errors?.message || JSON.stringify(data.errors);
      
      if (errorMsg.includes('expiration') || errorMsg.includes('future')) {
        formattedError = "Blur: Invalid expiration time (too far in the future)";
        const nowCheck = Date.now();
        const expirationCheck = new Date(requestExpirationTime).getTime();
        console.error(`❌ ${formattedError}`);
        console.error(`   Contract: ${nftAddress}`);
        console.error(`   Token ID: ${tokenId}`);
        console.error(`   Time until expiration: ${Math.round((expirationCheck - nowCheck) / (1000 * 60))} minutes`);
      } else if (errorMsg.includes('You need to own this NFT')) {
        formattedError = "Blur: User no longer owns this NFT";
        console.error(`❌ ${formattedError}`);
        console.error(`   Contract: ${nftAddress}`);
        console.error(`   Token ID: ${tokenId}`);
      } else {
        console.error(`❌ ${formattedError}: ${errorMsg}`);
        console.error(`   Contract: ${nftAddress}`);
        console.error(`   Token ID: ${tokenId}`);
        console.error(`   Price: ${price} ETH`);
      }

      // Check if the error is because user no longer owns the NFT
      const errorMessage = data.errors?.message || '';
      if (errorMessage.includes('You need to own this NFT')) {
        console.log(`${GOLD}[BLUR] User no longer owns NFT ${nftAddress}/${tokenId}, deleting listing...${RESET}`);

        try {
          await Listing.findOneAndDelete({
            "contract.contractAddress": nftAddress,
            tokenId: tokenId
          });
          console.log(`${GOLD}[BLUR] Deleted listing for ${nftAddress}/${tokenId} as user no longer owns it${RESET}`);
        } catch (deleteError) {
          console.error(`${GOLD}[BLUR] Error deleting listing:${RESET}`, deleteError);
        }
      }

      return;
    }

    // Convert BigNumber objects to strings for ethers v6 compatibility
    const signData = data?.signatures?.[0]?.signData;
    if (!signData) {
      console.error('No sign data received from Blur');
      console.error('Full response:', JSON.stringify(data, null, 2));
      return;
    }

    // Helper function to convert BigNumber objects to strings
    function convertBigNumbers(obj: any): any {
      if (obj === null || obj === undefined) return obj;

      // Check if it's a BigNumber object
      if (obj.type === 'BigNumber' && obj.hex) {
        return BigInt(obj.hex).toString();
      }

      // Recursively process arrays
      if (Array.isArray(obj)) {
        return obj.map(item => convertBigNumbers(item));
      }

      // Recursively process objects
      if (typeof obj === 'object') {
        const result: any = {};
        for (const key in obj) {
          result[key] = convertBigNumbers(obj[key]);
        }
        return result;
      }

      return obj;
    }

    // Convert the signing data
    const convertedValue = convertBigNumbers(signData.value);

    // sign the offer
    try {
      const signObj = await wallet.signTypedData(
        signData.domain,
        signData.types,
        convertedValue
      );

      const payload = {
        signature: signObj,
        marketplace: 'BLUR',
        marketplaceData: data?.signatures?.[0]?.marketplaceData
      };

      const submitOptions = {
        method: 'POST',
        url: `${BASE_URL}/blur/v1/orders/submit`,
        headers: {
          'content-type': 'application/json',
          authToken: authToken,
          walletAddress: wallet.address,
          'x-api-key': API_KEY
        },
        data: JSON.stringify(payload)
      };

      const listings = await limiter.schedule(() => axiosInstance
        .request(submitOptions))
        .then((res) => res.data)
        .catch((err) => {
          console.error('Error submitting to Blur:', err?.response?.data || err.message);
          return { errors: err?.response?.data ?? err };
        });

      if (listings.errors) {
        // Display graceful error for submit errors
        const submitErrorMsg = listings.errors?.message || JSON.stringify(listings.errors);
        console.error(`❌ Blur submit failed: ${submitErrorMsg}`);
        console.error(`   Contract: ${nftAddress}`);
        console.error(`   Token ID: ${tokenId}`);

        // Check if the error is because user no longer owns the NFT
        const errorMessage = listings.errors?.message || '';
        if (errorMessage.includes('You need to own this NFT')) {
          console.log(`${GOLD}[BLUR] User no longer owns NFT ${nftAddress}/${tokenId}, deleting listing...${RESET}`);

          try {
            await Listing.findOneAndDelete({
              "contract.contractAddress": nftAddress,
              tokenId: tokenId
            });
            console.log(`${GOLD}[BLUR] Deleted listing for ${nftAddress}/${tokenId} as user no longer owns it${RESET}`);
          } catch (deleteError) {
            console.error(`${GOLD}[BLUR] Error deleting listing:${RESET}`, deleteError);
          }
        }
      } else {
        console.log(`${GOLD}[BLUR - LISTING] ✅ Successfully listed ${nftAddress} #${tokenId} for ${price} ETH${RESET}`);

        // Update listing price cache
        const priceNumber = parseFloat(price);
        await listingPriceCache.updatePrice(
          nftAddress.toLowerCase(),
          tokenId,
          priceNumber,
          wallet.address.toLowerCase()
        );

        // Update finalListingPrice.blur and expiration in the database
        try {
          // Convert price string to number for database storage and round to 9 decimal places
          const priceInEth = roundToNineDecimals(parseFloat(price));
          // Use the same expiration timestamp that was sent to Blur
          const dbExpirationTime = new Date(requestExpirationTimestamp);

          const updatedListing = await Listing.findOneAndUpdate(
            {
              "contract.contractAddress": nftAddress,
              tokenId: tokenId
            },
            [
              {
                $set: {
                  "finalListingPrice.blur": priceInEth,
                  "listingExpiration.blur": dbExpirationTime,
                  lastListedAt: {
                    $mergeObjects: [
                      { $cond: [{ $eq: [{ $type: "$lastListedAt" }, "object"] }, "$lastListedAt", {}] },
                      { blur: new Date() }
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
                      dbExpirationTime,
                      "$instantPriceMatchingActiveUntil"
                    ]
                  }
                }
              }
            ],
            { new: true }
          );

          if (updatedListing) {
            console.log(`${GOLD}[BLUR - LISTING] Updated DB: ${nftAddress} #${tokenId} | Price: ${priceInEth} ETH | Expires: ${dbExpirationTime.toISOString()}${RESET}`);
            if (updatedListing.instantPriceOverride && updatedListing.instantPriceMatchingActiveUntil) {
              console.log(`${GOLD}[BLUR - LISTING] Extended instant price matching until: ${updatedListing.instantPriceMatchingActiveUntil.toISOString()}${RESET}`);
            }
          } else {
            console.log(`${GOLD}[BLUR - LISTING] ⚠️  No listing found in DB for ${nftAddress} #${tokenId}${RESET}`);
          }
        } catch (updateError) {
          console.error(`${GOLD}[BLUR] Error updating listing in database:${RESET}`, updateError);
        }
      }
    } catch (signError) {
      console.error('Error during signing:', signError);
      throw signError;
    }
  } catch (error) {
    // General error handling for unexpected errors
    console.error(`❌ Blur listing error: ${error instanceof Error ? error.message : 'Unknown error'}`);
    console.error(`   Contract: ${nftAddress}`);
    console.error(`   Token ID: ${tokenId}`);
  }
}