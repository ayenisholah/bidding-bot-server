import { AlchemyProvider, Contract, parseEther, Wallet } from "ethers";
import { approveNFT } from "../../functions/approve";
import { IFee } from "../../types/listing.types";
import { axiosInstance, limiter } from "../../init";
import { config } from "dotenv";
import { getCollectionDetails } from "../../functions/collection";

// const BLUE = '\x1b[34m';
// const RESET = '\x1b[0m';
import { BASE_URL, domain, minABI, NETWORK } from "../../constants";
import Listing from "../../models/listing.model";
import { errorLog } from "../../utils/debug-file-logger";
import { convertDurationForOpenSea } from "../../utils/duration";
import { roundToNineDecimals } from "../../utils/decimal-precision";
import { getOpenseaMarketplaceFee } from "../../utils/marketplace-fee-cache";

config()

const divider = 10000;
const openseaFee = Number(getOpenseaMarketplaceFee());

// Ceiling division for BigInt - rounds UP to ensure fees are never under-calculated
// Standard BigInt division truncates toward zero, which can result in 0 fees for small amounts
function ceilDiv(numerator: bigint, denominator: bigint): bigint {
  return (numerator + denominator - 1n) / denominator;
}
const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY
const API_KEY = process.env.API_KEY;
const types = {
  OrderComponents: [
    {
      name: 'offerer',
      type: 'address'
    },
    {
      name: 'zone',
      type: 'address'
    },
    {
      name: 'offer',
      type: 'OfferItem[]'
    },
    {
      name: 'consideration',
      type: 'ConsiderationItem[]'
    },
    {
      name: 'orderType',
      type: 'uint8'
    },
    {
      name: 'startTime',
      type: 'uint256'
    },
    {
      name: 'endTime',
      type: 'uint256'
    },
    {
      name: 'zoneHash',
      type: 'bytes32'
    },
    {
      name: 'salt',
      type: 'uint256'
    },
    {
      name: 'conduitKey',
      type: 'bytes32'
    },
    {
      name: 'counter',
      type: 'uint256'
    }
  ],
  OfferItem: [
    {
      name: 'itemType',
      type: 'uint8'
    },
    {
      name: 'token',
      type: 'address'
    },
    {
      name: 'identifierOrCriteria',
      type: 'uint256'
    },
    {
      name: 'startAmount',
      type: 'uint256'
    },
    {
      name: 'endAmount',
      type: 'uint256'
    }
  ],
  ConsiderationItem: [
    {
      name: 'itemType',
      type: 'uint8'
    },
    {
      name: 'token',
      type: 'address'
    },
    {
      name: 'identifierOrCriteria',
      type: 'uint256'
    },
    {
      name: 'startAmount',
      type: 'uint256'
    },
    {
      name: 'endAmount',
      type: 'uint256'
    },
    {
      name: 'recipient',
      type: 'address'
    }
  ]
};
const provider = new AlchemyProvider(NETWORK, ALCHEMY_API_KEY)

/**
 * Lists an NFT on OpenSea marketplace
 * 
 * @param nftAddress - The contract address of the NFT collection
 * @param tokenId - The token ID of the NFT to list
 * @param price - The listing price in ETH (e.g., 1.5 for 1.5 ETH)
 * @param wallet - The wallet instance used for signing the listing transaction
 * @param contractSlug - The collection slug for fetching collection details
 * @param listDuration - Duration object containing value and unit
 * @returns Promise<void> - Resolves when the listing is successfully created or throws an error
 * 
 * @example
 * ```typescript
 * await listOnOpensea(
 *   "0x524cab2ec69124574082676e6f654a18df49a048",
 *   "1234",
 *   1.5, // 1.5 ETH
 *   wallet,
 *   "cool-cats-nft",
 *   { value: 24, unit: "hours" }
 * );
 * ```
 */
export const listOnOpensea = async (
  nftAddress: string,
  tokenId: string,
  price: number,
  wallet: Wallet,
  contractSlug: string,
  listDuration: { value: number; unit: string },
) => {

  // Fetch collection details for creator fees
  let creatorFees: IFee | undefined;
  let enforceCreatorFee = false;
  
  try {
    // console.log(`${BLUE}[OPENSEA] Fetching collection details for ${contractSlug}...${RESET}`);
    const collectionData = await getCollectionDetails(contractSlug, nftAddress);
    creatorFees = collectionData?.creator_fees as IFee;
    enforceCreatorFee = collectionData?.enforceCreatorFee as boolean;
    // console.log(`${BLUE}[OPENSEA] Collection details fetched - Enforce creator fee: ${enforceCreatorFee}${RESET}`);
  } catch (error) {
    errorLog('Listing', `[OpenSea] Error fetching collection details | contract=${nftAddress} | wallet=${wallet.address.slice(0, 10)}...`, error);
  }
  
  try {
    await approveNFT(
      nftAddress,
      wallet,
      '0x1E0049783F008A0085193E00003D00cd54003c71'
    );
  } catch (error) {
    errorLog('Listing', `[OpenSea] Error approving NFT | contract=${nftAddress} | wallet=${wallet.address.slice(0, 10)}...`, error);
  }

  const listingPayload: any = {
    criteria: {
      collection: {
        slug: 'cool-cats-nft'
      }
    },
    protocol_data: {
      parameters: {
        offerer: '0x85e9C0C52BE6fa83ec02120F419E9874e3707E7E',
        offer: [
          {
            itemType: 1,
            token: '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
            identifierOrCriteria: 0,
            startAmount: (Date.now() / 1000).toString(),
            endAmount: (Date.now() / 1000 + 100000).toString()
          }
        ],
        consideration: [],
        startTime: '1666480886',
        endTime: '1666680886',
        orderType: 2,
        zone: '0x000056f7000000ece9003ca63978907a00ffd100',
        zoneHash:
          '0x0000000000000000000000000000000000000000000000000000000000000000',
        conduitKey:
          '0x0000007b02230091a7ed01230072f7006a004d60a8d4e71d599b8104250f0000',
        totalOriginalConsiderationItems: 2,
        counter: '0'
      },
      signature: '0x0'
    },
    protocol_address: '0x0000000000000068f116a894984e2db1123eb395'
  };

  listingPayload.parameters = listingPayload.protocol_data.parameters;

  // Round price to 9 decimal places to avoid floating point precision errors
  const roundedPrice = roundToNineDecimals(price);
  const offerPrice = parseEther(roundedPrice.toString());

  // Payload data
  // Convert duration to seconds using OpenSea-specific converter
  const durationInSeconds = convertDurationForOpenSea(listDuration);
  // console.log(`${BLUE}[OPENSEA] Duration: ${listDuration.value} ${listDuration.unit} = ${durationInSeconds} seconds${RESET}`);
  
  const currentTime = Math.floor(Date.now() / 1000);
  // const endTime = Math.floor(Date.now() / 1000 + durationInSeconds);

  // console.log(`[OPENSEA] Start time: ${currentTime} (${new Date(currentTime * 1000).toISOString()})`);
  // console.log(`[OPENSEA] End time: ${currentTime + durationInSeconds} (${new Date((currentTime + durationInSeconds) * 1000).toISOString()})`);
  // console.log(`[OPENSEA] Difference: ${durationInSeconds} seconds`);
  
  listingPayload.parameters.startTime = BigInt(currentTime).toString();
  listingPayload.parameters.endTime = BigInt(
    currentTime + durationInSeconds
  ).toString();
  const itemType = 2;
  listingPayload.parameters.offerer = wallet.address;
  listingPayload.parameters.offer[0].itemType = itemType;
  listingPayload.parameters.offer[0].token = nftAddress;
  listingPayload.parameters.offer[0].identifierOrCriteria = tokenId;
  listingPayload.parameters.offer[0].startAmount = "1";
  listingPayload.parameters.offer[0].endAmount = "1";

  let totalCreatorFee = 0;
  const creatorFeeConsideration = [];
  listingPayload.parameters.totalOriginalConsiderationItems = 2;

  if (enforceCreatorFee && creatorFees) {
    for (const address in creatorFees) {
      let fee = creatorFees[address];
      if (address !== 'null') {
        const consideration_item = {
          itemType: 0,
          token: "0x0000000000000000000000000000000000000000",
          identifierOrCriteria: 0,
          startAmount: ceilDiv(offerPrice * BigInt(fee), BigInt(divider)).toString(),
          endAmount: ceilDiv(offerPrice * BigInt(fee), BigInt(divider)).toString(),
          recipient: address
        };
        creatorFeeConsideration.push(consideration_item);
        listingPayload.parameters.totalOriginalConsiderationItems += 1;
        totalCreatorFee += fee;
      }
    }
  }

  // assign first consideration item
  const ownerConsideration = {
    itemType: 0,
    token: "0x0000000000000000000000000000000000000000",
    identifierOrCriteria: 0,
    startAmount: ((offerPrice * BigInt(divider - totalCreatorFee - openseaFee)) / BigInt(divider)).toString(),
    endAmount: ((offerPrice * BigInt(divider - totalCreatorFee - openseaFee)) / BigInt(divider)).toString(),
    recipient: wallet.address
  };

  // build second consideration item
  // This is the opensea fee
  // console.log(((offerPrice * BigInt(openseaFee)) / BigInt(divider)).toString());
  const openseaConsideration = {
    itemType: 0,
    token: "0x0000000000000000000000000000000000000000",
    identifierOrCriteria: 0,
    startAmount: ceilDiv(offerPrice * BigInt(openseaFee), BigInt(divider)).toString(),
    endAmount: ceilDiv(offerPrice * BigInt(openseaFee), BigInt(divider)).toString(),
    recipient: '0x0000a26b00c1f0df003000390027140000faa719'
  };

  listingPayload.parameters.consideration = enforceCreatorFee
    ? [
      ownerConsideration,
      openseaConsideration,
      ...creatorFeeConsideration
    ]
    : [
      ownerConsideration,
      openseaConsideration
    ];

  listingPayload.parameters.salt = Math.floor(
    Math.random() * 100_000
  ).toString();

  const seaportContractAddress = '0x00000000000001ad428e4906aE43D8F9852d0dD6';
  const seaportContract = new Contract(seaportContractAddress, minABI, provider);

  // request the value for the counter from the seaport contract and set it in payload
  const counter = await seaportContract.getCounter(wallet.address);
  listingPayload.parameters.counter = counter.toString();

  // sign the offer
  const sigObj = await wallet.signTypedData(
    domain,
    types,
    listingPayload.parameters
  );

  listingPayload.signature = sigObj;
  const openseaRapidEndpoint = `${BASE_URL}/opensea/api/v1/`

  // Retry logic for timeout errors
  let retryCount = 0;
  const maxRetries = 3;
  let listings: any;
  
  while (retryCount < maxRetries) {
    try {
      listings = await limiter
        .schedule(() =>
          axiosInstance.request({
            method: 'POST',
            url: `${openseaRapidEndpoint.replace(
              'v1',
              'v2'
            )}orders/ethereum/seaport/listings`,
            headers: {
              'content-type': 'application/json',
              'x-api-key': API_KEY
            },
            data: JSON.stringify(listingPayload),
            // Increase timeout to 120 seconds for OpenSea listings
            timeout: 120000
          })
        )
        .then((res) => res.data);
      
      // If successful, break out of retry loop
      break;
    } catch (err: any) {
      retryCount++;
      
      // Check if it's a timeout error
      if (err.code === 'ECONNABORTED' && retryCount < maxRetries) {
        // console.log(`${BLUE}[OPENSEA] Listing timeout (attempt ${retryCount}/${maxRetries}), retrying in ${retryCount * 5} seconds...${RESET}`);
        await new Promise(resolve => setTimeout(resolve, retryCount * 5000));
        continue;
      }
      
      // For other errors or final retry, return error object
      listings = { errors: err?.response?.data ?? err };
      break;
    }
  }

  if (listings.errors) {
    const errorString = JSON.stringify(listings.errors);
    const isTimeout = errorString.includes('ECONNABORTED') || errorString.includes('timeout');
    
    // Format the error message for better readability
    let formattedError = "OpenSea listing failed";
    if (isTimeout) {
      formattedError = `OpenSea: Request timeout after ${retryCount} attempts`;
    } else if (listings.errors?.message) {
      formattedError = `OpenSea: ${listings.errors.message}`;
    } else if (errorString.toLowerCase().includes('not owned') || errorString.toLowerCase().includes('ownership')) {
      formattedError = "OpenSea: User no longer owns this NFT";
    }
    
    // Build detailed error context
    let errorContext = `${formattedError} | contract=${nftAddress} | token=${tokenId} | price=${roundedPrice} | retries=${retryCount}`;
    if (isTimeout) {
      errorContext += ' | note=API slow or under heavy load';
    }

    errorLog('Listing', `[OpenSea] ${errorContext}`, listings.errors);
    
    // Check if the error is because user no longer owns the NFT
    const errorMessage = errorString.toLowerCase();
    if (errorMessage.includes('not owned') || errorMessage.includes('ownership') || errorMessage.includes('not the owner')) {
      // console.log(`${BLUE}[OPENSEA] User no longer owns NFT ${nftAddress}/${tokenId}, deleting listing...${RESET}`);
      
      try {
        await Listing.findOneAndDelete({
          "contract.contractAddress": nftAddress,
          tokenId: tokenId
        });
        // console.log(`${BLUE}[OPENSEA] Deleted listing for ${nftAddress}/${tokenId} as user no longer owns it${RESET}`);
      } catch (deleteError) {
        errorLog('Database', `[OpenSea] Error deleting listing | contract=${nftAddress} | token=${tokenId}`, deleteError);
      }
    }
  } else {
    // console.log(`${BLUE}[OPENSEA - LISTING] ✅ Successfully listed ${nftAddress} #${tokenId} for ${roundedPrice} ETH${RESET}`);

    // REMOVED: Cache update - we're using real-time prices now!
    // await listingPriceCache.updatePrice(...)

    // Update finalListingPrice.opensea and expiration in the database
    try {
      const expirationTime = new Date(Date.now() + durationInSeconds * 1000);
      const updatedListing = await Listing.findOneAndUpdate(
        {
          "contract.contractAddress": nftAddress,
          tokenId: tokenId
        },
        [
          {
            $set: {
              "finalListingPrice.opensea": roundedPrice,
              "listingExpiration.opensea": expirationTime,
              lastListedAt: {
                $mergeObjects: [
                  { $cond: [{ $eq: [{ $type: "$lastListedAt" }, "object"] }, "$lastListedAt", {}] },
                  { opensea: new Date() }
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
        // console.log(`${BLUE}[OPENSEA - LISTING] Updated DB: ${nftAddress} #${tokenId} | Price: ${roundedPrice} ETH | Expires: ${expirationTime.toISOString()}${RESET}`);
        if (updatedListing.instantPriceOverride && updatedListing.instantPriceMatchingActiveUntil) {
          // console.log(`${BLUE}[OPENSEA - LISTING] Extended instant price matching until: ${updatedListing.instantPriceMatchingActiveUntil.toISOString()}${RESET}`);
        }
      } else {
        // console.log(`${BLUE}[OPENSEA - LISTING] ⚠️  No listing found in DB for ${nftAddress} #${tokenId}${RESET}`);
      }
    } catch (updateError) {
      errorLog('Database', `[OpenSea] Error updating listing | contract=${nftAddress} | token=${tokenId} | price=${roundedPrice}`, updateError);
    }

  }
};
