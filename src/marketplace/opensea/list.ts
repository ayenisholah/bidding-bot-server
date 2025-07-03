import { AlchemyProvider, Contract, parseEther, Wallet } from "ethers";
import { approveNFT } from "../../functions/approve";
import { IFee } from "../../types/listing.types";
import { axiosInstance, limiter } from "../../init";
import { config } from "dotenv";
import { BASE_URL, domain, minABI, NETWORK } from "../../constants";
import Listing from "../../models/listing.model";
import { logger } from "../../utils/logger";

config()

const divider = 10000;
const openseaFee = 50;
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
 * @param enforceCreatorFee - Whether to enforce creator royalties on the listing
 * @param durationInSeconds - The listing duration in seconds from now
 * @param creatorFees - Object containing creator fee addresses and their respective fee percentages (in basis points)
 * @returns Promise<void> - Resolves when the listing is successfully created or throws an error
 * 
 * @example
 * ```typescript
 * await listOnOpensea(
 *   "0x524cab2ec69124574082676e6f654a18df49a048",
 *   "1234",
 *   1.5, // 1.5 ETH
 *   wallet,
 *   true,
 *   86400, // 24 hours
 *   { "0x123...": 250 } // 2.5% creator fee
 * );
 * ```
 */
export const listOnOpensea = async (
  nftAddress: string,
  tokenId: string,
  price: number,
  wallet: Wallet,
  enforceCreatorFee: boolean,
  durationInSeconds: number,
  creatorFees: IFee | undefined,
) => {

  try {
    await approveNFT(
      nftAddress,
      wallet,
      '0x1E0049783F008A0085193E00003D00cd54003c71'
    );
  } catch (error) {
    await logger.logOpenseaListingError('Error approving NFT for OpenSea listing', {
      contractAddress: nftAddress,
      walletAddress: wallet.address,
      operation: 'approve_nft',
      error: error as Error,
      errorMessage: error instanceof Error ? error.message : String(error)
    });
    console.log(error);
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

  // Round price to 4 decimal places to avoid floating point precision errors
  const roundedPrice = Math.round(price * 10000) / 10000;
  const offerPrice = parseEther(roundedPrice.toString());

  // Payload data
  listingPayload.parameters.startTime = BigInt(
    Math.floor(Date.now() / 1000)
  ).toString();
  listingPayload.parameters.endTime = BigInt(
    Math.floor(Date.now() / 1000 + durationInSeconds)
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
          startAmount: ((offerPrice * BigInt(fee)) / BigInt(divider)).toString(),
          endAmount: ((offerPrice * BigInt(fee)) / BigInt(divider)).toString(),
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
  // console.log(offerPrice.mul(openseaFee).div(divider).toString());
  const openseaConsideration = {
    itemType: 0,
    token: "0x0000000000000000000000000000000000000000",
    identifierOrCriteria: 0,
    startAmount: ((offerPrice * BigInt(50)) / BigInt(divider)).toString(),
    endAmount: ((offerPrice * BigInt(50)) / BigInt(divider)).toString(),
    recipient: '0x0000a26b00c1F0DF003000390027140000fAa719'
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

  const listings = await limiter
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
        data: JSON.stringify(listingPayload)
      })
    )
    .then((res) => res.data)
    .catch((err) => {
      return { errors: err?.response?.data ?? err };
    });

  if (listings.errors) {
    await logger.logOpenseaListingError('Failed to list NFT on OpenSea', {
      contractAddress: nftAddress,
      tokenId: tokenId,
      price: roundedPrice,
      walletAddress: wallet.address,
      operation: 'submit_listing',
      error: listings.errors,
      errorMessage: JSON.stringify(listings.errors)
    });
    console.log('Error: ', JSON.stringify(listings.errors));
  } else {
    console.log('NFT listed successfully on opensea');

    // Update finalListingPrice.opensea and expiration in the database
    try {
      const expirationTime = new Date(Date.now() + durationInSeconds * 1000);
      const updatedListing = await Listing.findOneAndUpdate(
        {
          "contract.contractAddress": nftAddress,
          tokenId: tokenId
        },
        {
          $set: {
            "finalListingPrice.opensea": roundedPrice,
            "listingExpiration.opensea": expirationTime,
            "lastListedAt.opensea": new Date(),
            listed: true
          }
        },
        { new: true }
      );

      if (updatedListing) {
        console.log(`Updated listing ${nftAddress} #${tokenId} with opensea price: ${roundedPrice} ETH, expires at: ${expirationTime.toISOString()}`);
      } else {
        console.log(`No listing found for ${nftAddress} #${tokenId}`);
      }
    } catch (updateError) {
      await logger.logOpenseaListingError('Error updating listing in database', {
        contractAddress: nftAddress,
        tokenId: tokenId,
        operation: 'update_listing_price',
        price: roundedPrice,
        error: updateError as Error,
        errorMessage: updateError instanceof Error ? updateError.message : String(updateError)
      });
      console.error('Error updating listing in database:', updateError);
    }

  }
};
