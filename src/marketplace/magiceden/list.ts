import { Wallet } from "ethers";
import { BASE_URL } from "../../constants";
import { approveNFT } from "../../functions/approve";
import { axiosInstance, limiter } from "../../init";
import Listing from "../../models/listing.model";

const API_KEY = process.env.API_KEY;


export async function listOnMagicEden(walletAddress: string, contract: string, tokenId: string, listingPrice: number, durationInSeconds: number, wallet: Wallet) {
  const listEndpoint =
    `${BASE_URL}/magiceden/v3/rtp/ethereum/execute/list/v5`;
  const signEndpoint =
    `${BASE_URL}/magiceden/v3/rtp/ethereum/order/v4`;
  const headers = {
    'x-api-key': API_KEY
  };
  const roundedPrice = Math.round(listingPrice * 10000) / 10000;
  const weiPrice = (roundedPrice * 1e18).toString()

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
    
    const listResponse = await limiter.schedule(() =>
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

    const { seller, ...restOfSignData } = signData.value;

    const order = {
      items: [
        {
          order: {
            kind: "payment-processor-v2",
            data: {
              kind: "sale-approval",
              sellerOrBuyer: seller,
              ...restOfSignData,
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

    const finalListingResponse = await limiter.schedule(() =>
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
        {
          $set: {
            "finalListingPrice.magiceden": roundedPrice,
            "listingExpiration.magiceden": expirationTime,
            "lastListedAt.magiceden": new Date(),
            listed: true
          }
        },
        { new: true }
      );
      
      if (updatedListing) {
        console.log(`Updated listing ${contract} #${tokenId} with magiceden price: ${roundedPrice} ETH, expires at: ${expirationTime.toISOString()}`);
      }
    } catch (updateError) {
      console.error('Error updating listing in database:', updateError);
    }
    
    return finalListingResponse.data;
  } catch (error: any) {
    console.error("=== Magic Eden Listing Failed ===");
    console.error("Error details:", error?.response?.data || error?.response || error);
    if (error?.response?.data) {
      console.error('Response data:', JSON.stringify(error.response.data, null, 2));
    }
    if (error?.response?.status) {
      console.error('Response status:', error.response.status);
    }
    if (error instanceof Error) {
      console.error('Error message:', error.message);
      console.error('Error stack:', error.stack);
    }
    return null;
  }
}