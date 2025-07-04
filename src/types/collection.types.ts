export interface ICollectionFee {
  fee: number;
  recipient: string;
  required: boolean;
}

export interface ICreatorFees {
  [walletAddress: string]: number; // wallet address -> percentage (e.g., 2.5 for 2.5%)
}

export interface ICollectionContract {
  address: string;
  chain: string;
}

export interface ICollectionRarity {
  calculated_at: string;
  max_rank: number;
  total_supply: number;
  strategy_id: string;
  strategy_version: string;
}

export interface ICollectionDetails {
  collection: string;
  name: string;
  description: string;
  image_url: string;
  banner_image_url: string;
  owner: string;
  safelist_status: string;
  category: string;
  is_disabled: boolean;
  is_nsfw: boolean;
  trait_offers_enabled: boolean;
  collection_offers_enabled: boolean;
  opensea_url: string;
  project_url: string;
  wiki_url: string;
  discord_url: string;
  telegram_url: string;
  twitter_username: string;
  instagram_username: string;
  contracts: ICollectionContract[];
  editors: string[];
  fees: ICollectionFee[];
  rarity: ICollectionRarity;
  total_supply: number;
  created_date: string;
  payment_tokens: any[]; // You can create a more specific interface if needed
  
  // Additional fields that might be returned by the function
  creator_fees?: { [walletAddress: string]: number };
  enforceCreatorFee?: boolean;
}