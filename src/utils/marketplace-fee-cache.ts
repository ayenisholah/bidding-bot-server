const DEFAULT_OPENSEA_FEE_BPS = 100n;

let cachedOpenseaFeeBps: bigint | undefined;

export function setOpenseaMarketplaceFee(feeBps: bigint) {
  if (typeof feeBps !== 'bigint') {
    return;
  }

  if (feeBps < 0n) {
    return;
  }

  cachedOpenseaFeeBps = feeBps;
}

export function getOpenseaMarketplaceFee(): bigint {
  return cachedOpenseaFeeBps ?? DEFAULT_OPENSEA_FEE_BPS;
}

export { DEFAULT_OPENSEA_FEE_BPS };
