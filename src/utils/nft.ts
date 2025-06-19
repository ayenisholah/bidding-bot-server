import { BASE_URL } from '../constants';
import { axiosInstance, limiter } from '../init';
import { ethers } from 'ethers';

const API_KEY = process.env.API_KEY as string;

const RED = '\x1b[31m';
const RESET = '\x1b[0m';

interface NFTItem {
  contractAddress: string;
  tokenId: string;
  wallet: string;
  walletId: string
  tokenType: string;
  name?: string;
  imageUrl?: string;
  collection?: {
    name?: string;
    slug?: string;
    isVerified?: boolean;
    floorPrice?: {
      usd?: number;
      token?: {
        unit?: number;
        symbol?: string;
      };
    };
  };
  attributes?: Array<{
    trait_type: string;
    value: string | number;
    floorPrice?: {
      usd?: number;
      token?: {
        unit?: number;
        symbol?: string;
      };
    };
    topOfferPrice?: {
      usd?: number;
      token?: {
        unit?: number;
        symbol?: string;
      };
    };
  }>;
  lastSale?: {
    usd?: number;
    token?: {
      unit?: number;
      symbol?: string;
    };
  };
  bestListing?: {
    id?: string;
    pricePerItem?: {
      usd?: number;
      token?: {
        unit?: number;
        symbol?: string;
        contractAddress?: string;
      };
    };
    maker?: {
      address?: string;
    };
    marketplace?: {
      identifier?: string;
    };
    quantityRemaining?: number;
    startTime?: string;
    endTime?: string;
  };
  bestOffer?: {
    id?: string;
    pricePerItem?: {
      usd?: number;
      token?: {
        unit?: number;
        symbol?: string;
        contractAddress?: string;
      };
    };
    maker?: {
      address?: string;
    };
    quantityRemaining?: number;
    endTime?: string;
  };
}

export interface WalletNFTsResponse {
  wallet: string;
  walletId: string;
  nfts: NFTItem[];
  totalCount: number;
}


/**
 * Fetches NFTs for multiple wallets using OpenSea GraphQL API
 * @param wallets Array of wallet addresses
 * @returns Array of NFT contents for each wallet
 */
export async function fetchWalletNFTs(wallets: { address: string, walletId: string }[]): Promise<WalletNFTsResponse[]> {
  if (!API_KEY) {
    throw new Error('API_KEY is not configured');
  }

  const results: WalletNFTsResponse[] = [];

  for (const wallet of wallets) {
    // Validate wallet address
    if (!ethers.isAddress(wallet.address)) {
      console.error(`Invalid wallet address: ${wallet.address}`);
      results.push({
        wallet: wallet.address,
        walletId: wallet.walletId,
        nfts: [],
        totalCount: 0
      });
      continue;
    }

    try {
      try {
        const nfts = await fetchNFTsFromOpenSea({ wallet: wallet.address, walletId: wallet.walletId });

        const response: WalletNFTsResponse = {
          wallet: wallet.address,
          walletId: wallet.walletId,
          nfts,
          totalCount: nfts.length
        };

        results.push(response);
      } catch (error) {
        console.log(error);
      }
    } catch (error) {
      console.error(`Error fetching NFTs for wallet ${wallet}:`, error);
      results.push({
        wallet: wallet.address,
        walletId: wallet.walletId,
        nfts: [],
        totalCount: 0
      });
    }
  }

  return results;
}

async function fetchNFTsFromOpenSea({ wallet, walletId }: { wallet: string; walletId: string }): Promise<NFTItem[]> {
  const nfts: NFTItem[] = [];
  let cursor: string | undefined;

  const query = {
    "operationName": "ProfileItemsListQuery",
    "query": "query ProfileItemsListQuery($address: Address!, $limit: Int, $cursor: String, $sort: ProfileItemsSort!, $filter: ProfileItemsFilter) {\n  profileItems(\n    addresses: [$address]\n    limit: $limit\n    sort: $sort\n    filter: $filter\n    cursor: $cursor\n  ) {\n    nextPageCursor\n    items {\n      version\n      id\n      collection {\n        id\n        slug\n        __typename\n      }\n      enforcement {\n        isDelisted\n        __typename\n      }\n      isFungible\n      tokenId\n      contractAddress\n      attributes {\n        traitType\n        value\n        floorPrice {\n          native {\n            unit\n            __typename\n          }\n          ...TokenPrice\n          __typename\n        }\n        topOfferPrice {\n          native {\n            unit\n            __typename\n          }\n          ...TokenPrice\n          __typename\n        }\n        stats {\n          itemCount\n          percent\n          __typename\n        }\n        __typename\n      }\n      ...ItemOwnedQuantity\n      lastTransferAt\n      ownership(address: $address) {\n        id\n        isHidden\n        __typename\n      }\n      chain {\n        identifier\n        __typename\n      }\n      bestListing {\n        id\n        startTime\n        endTime\n        quantityRemaining\n        pricePerItem {\n          usd\n          token {\n            unit\n            symbol\n            contractAddress\n            __typename\n          }\n          __typename\n        }\n        maker {\n          address\n          __typename\n        }\n        marketplace {\n          identifier\n          __typename\n        }\n        __typename\n      }\n      bestOffer {\n        id\n        endTime\n        quantityRemaining\n        pricePerItem {\n          usd\n          token {\n            unit\n            symbol\n            contractAddress\n            __typename\n          }\n          __typename\n        }\n        maker {\n          address\n          __typename\n        }\n        __typename\n      }\n      lowestListingForOwner(address: $address) {\n        pricePerItem {\n          usd\n          __typename\n        }\n        __typename\n      }\n      ...ProfileItemsTableRowFragment\n      ...ProfileItemsCardFragment\n      ...profileItemsSelection\n      ...ProfileItemPaginator\n      __typename\n    }\n    __typename\n  }\n}\nfragment ProfileItemsTableRowFragment on Item {\n  id\n  chain {\n    ...ChainBadge\n    __typename\n  }\n  isFungible\n  name\n  lastTransferAt\n  ownership(address: $address) {\n    id\n    __typename\n  }\n  rarity {\n    rank\n    category\n    __typename\n  }\n  collection {\n    floorPrice {\n      pricePerItem {\n        ...TokenPrice\n        __typename\n      }\n      __typename\n    }\n    name\n    isVerified\n    ...CollectionLink\n    ...CollectionPreviewTooltip\n    __typename\n  }\n  enforcement {\n    isCompromised\n    __typename\n  }\n  lowestListingForOwner(address: $address) {\n    pricePerItem {\n      ...TokenPrice\n      __typename\n    }\n    __typename\n  }\n  lastSale {\n    ...TokenPrice\n    __typename\n  }\n  standard\n  ...SellItemTableButton\n  ...ItemLink\n  ...ItemAvatar\n  ...profileItemsSelection\n  ...EnforcementBadge\n  ...OwnedQuantity\n  ...RarityTooltip\n  ...isItemRarityDisabled\n  ...ItemPreviewTooltip\n  ...BulkActionsDisabledTooltip\n  __typename\n}\nfragment ChainBadge on Chain {\n  identifier\n  name\n  __typename\n}\nfragment ItemLink on BaseItem {\n  ...itemUrl\n  chain {\n    identifier\n    __typename\n  }\n  tokenId\n  contractAddress\n  imageUrl\n  animationUrl\n  ...useSetItemQuickView\n  __typename\n}\nfragment itemUrl on ItemIdentifier {\n  chain {\n    identifier\n    arch\n    __typename\n  }\n  tokenId\n  contractAddress\n  __typename\n}\nfragment useSetItemQuickView on Item {\n  ...itemUrl\n  ...ItemViewModal\n  __typename\n}\nfragment ItemViewModal on Item {\n  tokenId\n  imageUrl\n  id\n  name\n  ...itemIdentifier\n  ...ItemViewSkeleton\n  ...itemUrl\n  __typename\n}\nfragment itemIdentifier on ItemIdentifier {\n  chain {\n    identifier\n    __typename\n  }\n  tokenId\n  contractAddress\n  __typename\n}\nfragment ItemViewSkeleton on Item {\n  name\n  ...ItemPageMedia\n  ...ItemTabs\n  ...ItemAbout\n  __typename\n}\nfragment ItemPageMedia on Item {\n  ...ItemMedia\n  __typename\n}\nfragment ItemMedia on Item {\n  imageUrl\n  animationUrl\n  backgroundColor\n  collection {\n    imageUrl\n    __typename\n  }\n  __typename\n}\nfragment ItemTabs on Item {\n  isFungible\n  collection {\n    slug\n    __typename\n  }\n  __typename\n}\nfragment ItemAbout on Item {\n  id\n  name\n  tokenId\n  tokenUri\n  contractAddress\n  chain {\n    name\n    identifier\n    arch\n    __typename\n  }\n  standard\n  description\n  details {\n    name\n    value\n    __typename\n  }\n  collection {\n    ...CollectionOwner\n    name\n    description\n    owner {\n      displayName\n      ...AccountLockup\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\nfragment CollectionOwner on Collection {\n  owner {\n    displayName\n    isVerified\n    address\n    ...profileUrl\n    ...ProfilePreviewTooltip\n    __typename\n  }\n  standard\n  __typename\n}\nfragment profileUrl on ProfileIdentifier {\n  address\n  __typename\n}\nfragment ProfilePreviewTooltip on ProfileIdentifier {\n  address\n  ...ProfilePreviewTooltipContent\n  __typename\n}\nfragment ProfilePreviewTooltipContent on ProfileIdentifier {\n  address\n  __typename\n}\nfragment AccountLockup on ProfileIdentifier {\n  address\n  displayName\n  imageUrl\n  ...profileUrl\n  __typename\n}\nfragment TokenPrice on Price {\n  usd\n  token {\n    unit\n    symbol\n    contractAddress\n    chain {\n      identifier\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\nfragment profileItemsSelection on Item {\n  id\n  imageUrl\n  chain {\n    identifier\n    __typename\n  }\n  contractAddress\n  tokenId\n  bestOffer {\n    __typename\n  }\n  bestListing {\n    maker {\n      address\n      __typename\n    }\n    __typename\n  }\n  collection {\n    slug\n    __typename\n  }\n  lowestListingForOwner(address: $address) {\n    __typename\n  }\n  isFungible\n  ...useTransferItems\n  ...useAcceptOffers\n  ...useListItems\n  ...useCancelItemsListings\n  ...isItemTradable\n  ...HideButton\n  __typename\n}\nfragment useTransferItems on Item {\n  tokenId\n  chain {\n    identifier\n    arch\n    __typename\n  }\n  contractAddress\n  ...ItemOwnedQuantity\n  ...isItemTradable\n  __typename\n}\nfragment ItemOwnedQuantity on Item {\n  ownership(address: $address) {\n    id\n    quantity\n    __typename\n  }\n  __typename\n}\nfragment isItemTradable on Item {\n  collection {\n    isTradingDisabled\n    __typename\n  }\n  enforcement {\n    isCompromised\n    isDisabled\n    __typename\n  }\n  isTradingDisabled\n  __typename\n}\nfragment useAcceptOffers on Item {\n  chain {\n    identifier\n    arch\n    __typename\n  }\n  contractAddress\n  tokenId\n  collection {\n    isTradingDisabled\n    __typename\n  }\n  bestOffer {\n    pricePerItem {\n      token {\n        unit\n        address\n        __typename\n      }\n      __typename\n    }\n    maker {\n      address\n      __typename\n    }\n    __typename\n  }\n  enforcement {\n    isCompromised\n    __typename\n  }\n  __typename\n}\nfragment useListItems on Item {\n  tokenId\n  chain {\n    identifier\n    arch\n    __typename\n  }\n  collection {\n    isTradingDisabled\n    __typename\n  }\n  contractAddress\n  ...ItemOwnedQuantity\n  ...isItemTradable\n  __typename\n}\nfragment useCancelItemsListings on Item {\n  chain {\n    arch\n    identifier\n    __typename\n  }\n  contractAddress\n  tokenId\n  lowestListingForOwner(address: $address) {\n    __typename\n  }\n  __typename\n}\nfragment HideButton on Item {\n  ownership(address: $address) {\n    id\n    isHidden\n    __typename\n  }\n  __typename\n}\nfragment ItemAvatar on Item {\n  imageUrl\n  tokenId\n  backgroundColor\n  collection {\n    imageUrl\n    __typename\n  }\n  __typename\n}\nfragment EnforcementBadge on EnforcedEntity {\n  __typename\n  enforcement {\n    isCompromised\n    isDisabled\n    isOwnershipDisputed\n    __typename\n  }\n}\nfragment OwnedQuantity on Item {\n  ...ItemOwnedQuantity\n  __typename\n}\nfragment RarityTooltip on Item {\n  rarity {\n    category\n    rank\n    totalSupply\n    __typename\n  }\n  ...isItemRarityDisabled\n  __typename\n}\nfragment isItemRarityDisabled on Item {\n  collection {\n    id\n    slug\n    __typename\n  }\n  __typename\n}\nfragment CollectionLink on CollectionIdentifier {\n  slug\n  __typename\n}\nfragment SellItemTableButton on Item {\n  ...bestItemOffer\n  ...useAcceptOffers\n  __typename\n}\nfragment bestItemOffer on Item {\n  bestItemOffer {\n    pricePerItem {\n      native {\n        unit\n        __typename\n      }\n      ...TokenPrice\n      __typename\n    }\n    maker {\n      address\n      __typename\n    }\n    __typename\n  }\n  collection {\n    id\n    topOffer {\n      pricePerItem {\n        native {\n          unit\n          __typename\n        }\n        ...TokenPrice\n        __typename\n      }\n      maker {\n        address\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\nfragment ItemPreviewTooltip on ItemIdentifier {\n  ...ItemPreviewTooltipContent\n  __typename\n}\nfragment ItemPreviewTooltipContent on ItemIdentifier {\n  ...itemIdentifier\n  __typename\n}\nfragment CollectionPreviewTooltip on CollectionIdentifier {\n  ...CollectionPreviewTooltipContent\n  __typename\n}\nfragment CollectionPreviewTooltipContent on CollectionIdentifier {\n  slug\n  __typename\n}\nfragment BulkActionsDisabledTooltip on Item {\n  collection {\n    slug\n    __typename\n  }\n  __typename\n}\nfragment ProfileItemsCardFragment on Item {\n  id\n  isFungible\n  chain {\n    identifier\n    __typename\n  }\n  contractAddress\n  tokenId\n  ...bestItemOffer\n  lowestListingForOwner(address: $address) {\n    id\n    pricePerItem {\n      ...TokenPrice\n      __typename\n    }\n    maker {\n      address\n      __typename\n    }\n    marketplace {\n      identifier\n      __typename\n    }\n    ...useCancelOrders\n    __typename\n  }\n  owner {\n    address\n    __typename\n  }\n  enforcement {\n    isDisabled\n    isCompromised\n    __typename\n  }\n  collection {\n    name\n    slug\n    isVerified\n    ...CollectionLink\n    ...CollectionPreviewTooltip\n    __typename\n  }\n  ...useBuyItems\n  ...useMakeOffer\n  ...useListItems\n  ...profileItemsSelection\n  ...useAcceptOffers\n  ...QuantityBadge\n  ...ItemCardMedia\n  ...ItemCardNameFragment\n  ...RarityBadgeFragment\n  ...ItemLink\n  ...OwnedQuantity\n  ...EnforcementBadge\n  ...useCancelItemsListings\n  ...ItemOwnedQuantity\n  ...isItemTradable\n  __typename\n}\nfragment ItemCardMedia on Item {\n  id\n  tokenId\n  ...ItemMedia\n  __typename\n}\nfragment ItemCardNameFragment on Item {\n  name\n  __typename\n}\nfragment RarityBadgeFragment on Item {\n  rarity {\n    rank\n    category\n    __typename\n  }\n  ...RarityTooltip\n  ...isItemRarityDisabled\n  __typename\n}\nfragment useBuyItems on Item {\n  chain {\n    identifier\n    arch\n    __typename\n  }\n  contractAddress\n  tokenId\n  collection {\n    slug\n    isTradingDisabled\n    __typename\n  }\n  bestListing {\n    pricePerItem {\n      token {\n        unit\n        address\n        symbol\n        ...currencyIdentifier\n        __typename\n      }\n      __typename\n    }\n    maker {\n      address\n      __typename\n    }\n    __typename\n  }\n  ...isItemListed\n  ...isItemTradable\n  __typename\n}\nfragment isItemListed on Item {\n  bestListing {\n    __typename\n  }\n  __typename\n}\nfragment currencyIdentifier on ContractIdentifier {\n  contractAddress\n  chain {\n    identifier\n    __typename\n  }\n  __typename\n}\nfragment useMakeOffer on Item {\n  tokenId\n  chain {\n    identifier\n    arch\n    __typename\n  }\n  contractAddress\n  ...isItemTradable\n  __typename\n}\nfragment QuantityBadge on Item {\n  bestListing {\n    quantityRemaining\n    __typename\n  }\n  totalSupply\n  __typename\n}\nfragment useCancelOrders on BaseOrder {\n  id\n  marketplace {\n    identifier\n    __typename\n  }\n  maker {\n    address\n    __typename\n  }\n  __typename\n}\nfragment ProfileItemPaginator on Item {\n  id\n  createdAt\n  rarity {\n    rank\n    __typename\n  }\n  collection {\n    floorPrice {\n      pricePerItem {\n        usd\n        __typename\n      }\n      __typename\n    }\n    topOffer {\n      pricePerItem {\n        usd\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  bestListing {\n    startTime\n    pricePerItem {\n      usd\n      __typename\n    }\n    __typename\n  }\n  bestOffer {\n    pricePerItem {\n      usd\n      __typename\n    }\n    __typename\n  }\n  lastSaleAt\n  lastSale {\n    usd\n    __typename\n  }\n  lastTransferAt\n  ownership(address: $address) {\n    id\n    __typename\n  }\n  lowestListingForOwner(address: $address) {\n    pricePerItem {\n      usd\n      __typename\n    }\n    __typename\n  }\n  __typename\n}",
    "variables": {
      "address": wallet,
      "limit": 50,
      "sort": {
        "by": "RECEIVED_DATE",
        "direction": "DESC"
      },
      ...(cursor && { cursor })
    }
  };

  do {
    // Update query variables with cursor
    query.variables = {
      "address": wallet,
      "limit": 50,
      "sort": {
        "by": "RECEIVED_DATE",
        "direction": "DESC"
      },
      ...(cursor && { cursor })
    };

    try {
      const { data } = await limiter.schedule(() =>
        axiosInstance.post(
          `${BASE_URL}/opensea/graphql`,
          query,
          {
            headers: {
              'content-type': 'application/json',
              'x-api-key': API_KEY
            }
          }
        )
      );

      if (data?.data?.profileItems?.items) {
        for (const item of data.data.profileItems.items) {
          // Filter for ERC721 tokens on Ethereum chain only
          const tokenType = item.standard || 'ERC721';
          const chainName = item.chain?.name || '';

          if (tokenType !== 'ERC721' || chainName !== 'Ethereum') {
            continue;
          }

          // Use attributes field if available (with floor prices), fallback to details
          const attributes = item.attributes ? item.attributes.map((attr: any) => ({
            trait_type: attr.traitType,
            value: attr.value,
            floorPrice: attr.floorPrice ? {
              usd: attr.floorPrice.usd,
              token: {
                unit: attr.floorPrice.token?.unit,
                symbol: attr.floorPrice.token?.symbol
              }
            } : undefined,
            topOfferPrice: attr.topOfferPrice ? {
              usd: attr.topOfferPrice.usd,
              token: {
                unit: attr.topOfferPrice.token?.unit,
                symbol: attr.topOfferPrice.token?.symbol
              }
            } : undefined,
          })) : item.details ? item.details.map((detail: any) => ({
            trait_type: detail.name,
            value: detail.value
          })) : [];

          nfts.push({
            walletId,
            wallet,
            contractAddress: item.contractAddress,
            tokenId: item.tokenId,
            tokenType: tokenType,
            name: item.name || `${item.collection?.name} #${item.tokenId}`,
            imageUrl: item.imageUrl || '',
            collection: {
              name: item.collection?.name || '',
              slug: item.collection?.slug || '',
              isVerified: item.collection?.isVerified || false,
              floorPrice: item.collection?.floorPrice ? {
                usd: item.collection.floorPrice.pricePerItem?.usd,
                token: {
                  unit: item.collection.floorPrice.pricePerItem?.token?.unit,
                  symbol: item.collection.floorPrice.pricePerItem?.token?.symbol
                }
              } : undefined
            },
            attributes,
            lastSale: item.lastSale ? {
              usd: item.lastSale.usd,
              token: item.lastSale.token ? {
                unit: item.lastSale.token.unit,
                symbol: item.lastSale.token.symbol
              } : undefined
            } : undefined,

            bestListing: item.bestListing ? {
              id: item.bestListing.id,
              pricePerItem: item.bestListing.pricePerItem ? {
                usd: item.bestListing.pricePerItem.usd,
                token: item.bestListing.pricePerItem.token ? {
                  unit: item.bestListing.pricePerItem.token.unit,
                  symbol: item.bestListing.pricePerItem.token.symbol,
                  contractAddress: item.bestListing.pricePerItem.token.contractAddress
                } : undefined
              } : undefined,
              maker: item.bestListing.maker ? {
                address: item.bestListing.maker.address
              } : undefined,
              marketplace: item.bestListing.marketplace ? {
                identifier: item.bestListing.marketplace.identifier
              } : undefined,
              quantityRemaining: item.bestListing.quantityRemaining,
              startTime: item.bestListing.startTime,
              endTime: item.bestListing.endTime
            } : undefined,
            bestOffer: item.bestOffer ? {
              id: item.bestOffer.id,
              pricePerItem: item.bestOffer.pricePerItem ? {
                usd: item.bestOffer.pricePerItem.usd,
                token: item.bestOffer.pricePerItem.token ? {
                  unit: item.bestOffer.pricePerItem.token.unit,
                  symbol: item.bestOffer.pricePerItem.token.symbol,
                  contractAddress: item.bestOffer.pricePerItem.token.contractAddress
                } : undefined
              } : undefined,
              maker: item.bestOffer.maker ? {
                address: item.bestOffer.maker.address
              } : undefined,
              quantityRemaining: item.bestOffer.quantityRemaining,
              endTime: item.bestOffer.endTime
            } : undefined
          });
        }

        cursor = data.data.profileItems.nextPageCursor;
      } else {
        break;
      }
    } catch (error) {
      console.error(RED + "Error fetching NFTs from OpenSea:", error + RESET);
      throw error;
    }
  } while (cursor);

  return nfts;
}