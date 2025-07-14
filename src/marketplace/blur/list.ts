import { config } from "dotenv";
import { approveNFT } from "../../functions/approve";
import { BASE_URL } from "../../constants";
import { axiosInstance, limiter } from "../../init";
import { Wallet } from "ethers";
import Listing from "../../models/listing.model";
import { convertDurationForBlur } from "../../utils/duration";

config()
const API_KEY = process.env.API_KEY;

/**
 * Lists an NFT on Blur marketplace
 * 
 * @param nftAddress - The contract address of the NFT collection
 * @param tokenId - The token ID of the NFT to list
 * @param price - The listing price as a string in ETH (e.g., "1.5" for 1.5 ETH)
 * @param wallet - The wallet instance used for signing the listing transaction
 * @param authToken - Blur authentication token for API access
 * @param listDuration - Duration object containing value and unit
 * @param feeRate - Creator fee rate as a decimal (e.g., 0.025 for 2.5% creator fee)
 * @returns Promise<void> - Resolves when the listing is successfully created or throws an error
 * 
 * @example
 * ```typescript
 * await listOnBlur(
 *   "0x524cab2ec69124574082676e6f654a18df49a048",
 *   "1234",
 *   "1.5", // 1.5 ETH as string
 *   wallet,
 *   "blur_auth_token_xyz",
 *   { value: 24, unit: "hours" },
 *   0.025 // 2.5% creator fee
 * );
 * ```
 * 
 * @description
 * This function handles the complete Blur listing process:
 * 1. Approves the NFT for Blur's contract
 * 2. Formats the order via Blur API
 * 3. Signs the typed data with the wallet
 * 4. Submits the signed order to Blur
 * 5. Updates the database with listing details
 */
export async function listOnBlur(
  nftAddress: string,
  tokenId: string,
  price: string,
  wallet: Wallet,
  authToken: string,
  listDuration: { value: number; unit: string },
  feeRate: number) {

  try {
    await approveNFT(
      nftAddress,
      wallet,
      '0x2f18f339620a63e43f0839eeb18d7de1e1be4dfb'
    );

    // Convert duration to seconds using Blur-specific converter
    const durationInSeconds = convertDurationForBlur(listDuration);
    console.log(`[BLUR] Duration: ${listDuration.value} ${listDuration.unit} = ${durationInSeconds} seconds`);
    
    // Add buffer time to account for processing delays and API latency
    const bufferSeconds = 120; // 120 second buffer to ensure we meet Blur's requirements
    const finalDuration = durationInSeconds + bufferSeconds;
    
    // Calculate expiration time right before sending to ensure freshness
    const durationMs = finalDuration * 1000;
    const nowTimestamp = Date.now();
    const expirationTimestamp = nowTimestamp + durationMs;
    const expirationTime = new Date(expirationTimestamp).toISOString();

    console.log('=== Blur Expiration Time Calculation ===');
    console.log('Now:', new Date(nowTimestamp).toISOString());
    console.log('Duration requested (seconds):', durationInSeconds);
    console.log('Buffer added (seconds):', bufferSeconds);
    console.log('Final duration (seconds):', finalDuration);
    console.log('Final duration (minutes):', finalDuration / 60);
    console.log('Expiration time:', expirationTime);
    console.log('Time until expiration (minutes):', (expirationTimestamp - nowTimestamp) / (1000 * 60));


    // Recalculate expiration time just before sending to minimize delay
    const requestNowTimestamp = Date.now();
    const requestExpirationTimestamp = requestNowTimestamp + durationMs;
    const requestExpirationTime = new Date(requestExpirationTimestamp).toISOString();
    
    console.log('Request time calculation:');
    console.log('- Request sent at:', new Date(requestNowTimestamp).toISOString());
    console.log('- Time since initial calculation (ms):', requestNowTimestamp - nowTimestamp);
    console.log('- Updated expiration time:', requestExpirationTime);

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
      console.error('Blur format errors:', JSON.stringify(data.errors, null, 2));
      
      // Log timing information when expiration error occurs
      const errorMessage = JSON.stringify(data.errors);
      if (errorMessage.includes('expiration') || errorMessage.includes('future')) {
        const nowCheck = Date.now();
        const expirationCheck = new Date(requestExpirationTime).getTime();
        console.error('=== Blur Expiration Error Debug ===');
        console.error('Error received at:', new Date(nowCheck).toISOString());
        console.error('Expiration time sent:', requestExpirationTime);
        console.error('Time difference (minutes):', (expirationCheck - nowCheck) / (1000 * 60));
        console.error('Request-to-error delay (ms):', nowCheck - requestNowTimestamp);
        console.error('Full error:', data.errors);
      }

      // Check if the error is because user no longer owns the NFT
      const errorMsg = data.errors?.message || '';
      if (errorMsg.includes('You need to own this NFT')) {
        console.log(`User no longer owns NFT ${nftAddress}/${tokenId}, deleting listing...`);

        try {
          await Listing.findOneAndDelete({
            "contract.contractAddress": nftAddress,
            tokenId: tokenId
          });
          console.log(`Deleted listing for ${nftAddress}/${tokenId} as user no longer owns it`);
        } catch (deleteError) {
          console.error('Error deleting listing:', deleteError);
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
        console.error('Blur submit errors:', JSON.stringify(listings.errors, null, 2));

        // Check if the error is because user no longer owns the NFT
        const errorMessage = listings.errors?.message || '';
        if (errorMessage.includes('You need to own this NFT')) {
          console.log(`User no longer owns NFT ${nftAddress}/${tokenId}, deleting listing...`);

          try {
            await Listing.findOneAndDelete({
              "contract.contractAddress": nftAddress,
              tokenId: tokenId
            });
            console.log(`Deleted listing for ${nftAddress}/${tokenId} as user no longer owns it`);
          } catch (deleteError) {
            console.error('Error deleting listing:', deleteError);
          }
        }
      } else {
        console.log('NFT listed successfully on blur');

        // Update finalListingPrice.blur and expiration in the database
        try {
          // Convert price string to number for database storage
          const priceInEth = parseFloat(price);
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
            console.log(`Updated listing ${nftAddress} #${tokenId} with blur price: ${priceInEth} ETH, expires at: ${dbExpirationTime.toISOString()}`);
            if (updatedListing.instantPriceOverride && updatedListing.instantPriceMatchingActiveUntil) {
              console.log(`Extended instant price matching until: ${updatedListing.instantPriceMatchingActiveUntil.toISOString()}`);
            }
          }
        } catch (updateError) {
          console.error('Error updating listing in database:', updateError);
        }
      }
    } catch (signError) {
      console.error('Error during signing:', signError);
      throw signError;
    }
  } catch (error) {
    console.error('=== Blur Listing Failed ===');
    console.error('Error details:', error);
    if (error instanceof Error) {
      console.error('Error message:', error.message);
      console.error('Error stack:', error.stack);
    }
  }
}