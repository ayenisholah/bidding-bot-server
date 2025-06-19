import { approveNFT } from "../../functions/approve";

export async function listToBlurWithRapid(
  nftAddress: string,
  tokenId: string | number,
  price: string,
  wallet: any,
  authToken: string,
  feeRate: number) {
  try {
    await approveNFT(
      nftAddress,
      wallet,
      '0x2f18f339620a63e43f0839eeb18d7de1e1be4dfb'
    );

    const options = {
      method: 'POST',
      url: `${config.blurRapidEndpoint}orders/format`,
      headers: {
        'content-type': 'application/json',
        authToken: authToken,
        walletAddress: wallet.address,
        'X-NFT-API-Key': config.apiKey
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
            expirationTime: new Date(Date.now() + 1000000).toISOString()
          }
        ]
      }
    };

    const data = await limiter.schedule(() => axiosInstance
      .request(options))
      .then((res) => res.data)
      .catch((err) => {
        return { errors: err?.response?.data ?? err };
      });

    if (data?.errors) {
      console.log(data.errors);
      return;
    }

    // sign the offer
    const signObj = await wallet._signTypedData(
      data?.signatures?.[0]?.signData?.domain,
      data?.signatures?.[0]?.signData?.types,
      data?.signatures?.[0]?.signData?.value
    );

    const payload = {
      signature: signObj,
      marketplace: 'BLUR',
      marketplaceData: data?.signatures?.[0]?.marketplaceData
    };

    console.log(payload);

    const submitOptions = {
      method: 'POST',
      url: `${config.blurRapidEndpoint}orders/submit`,
      headers: {
        'content-type': 'application/json',
        authToken: authToken,
        walletAddress: wallet.address,
        'X-NFT-API-Key': config.apiKey
      },
      data: JSON.stringify(payload)
    };

    const listings = await limiter.schedule(() => axiosInstance
      .request(submitOptions))
      .then((res) => res.data)
      .catch((err) => {
        return { errors: err?.response?.data ?? err };
      });


    if (listings.errors) {
      console.log('Error: ', JSON.stringify(listings.errors));
    } else {
      console.log('NFT listed successfully on blur');
    }
  } catch (error) {
    console.log(error);
  }
}