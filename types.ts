interface Chain {
  identifier: string;
  arch?: string;
  __typename: string;
}

interface TokenPrice {
  unit: number;
  __typename: string;
  contractAddress: string;
  symbol: string;
  chain: Chain;
}

interface Price {
  token: TokenPrice;
  __typename: string;
  usd: number;
}

interface Bidder {
  quantity: number;
  __typename: string;
  imageUrl: string | null;
  address: string;
  displayName: string;
}

interface CollectionOfferAggregate {
  id: string;
  totalOffers: number;
  offerPrice: Price;
  totalValue: Price;
  bidders: Bidder[];
  __typename: string;
}

interface CollectionFlags {
  isOffersEnabled: boolean;
  __typename: string;
}

interface Collection {
  __typename: string;
  slug: string;
  flags: CollectionFlags;
  traitOffersEnabled: boolean;
  chain: Chain;
}

interface DebugInformation {
  'x-trace-id': string;
  'x-cache-status': string;
  age: number;
  'cache-control': string;
  'x-ratelimit-remaining': number;
}

interface CollectionOfferAggregatesConnection {
  items: CollectionOfferAggregate[];
  nextPageCursor: string;
  __typename: string;
}

interface TransformResponse {
  data: {
    collectionOfferAggregates: CollectionOfferAggregatesConnection;
    collectionBySlug: Collection;
  };
  extensions: {
    debugInfo: {
      additionalInformation: DebugInformation;
    };
  };
}

interface OpenseaOffer {
  amount: number;
  owner: string;
}

export type OpenseaOfferResponse = [OpenseaOffer, OpenseaOffer]; 