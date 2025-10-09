export type MarketLogLevel = 'info' | 'warn' | 'error' | 'debug';

export type MarketLogStage =
  | 'start'
  | 'validation'
  | 'preprocessing'
  | 'processing'
  | 'queueing'
  | 'completion'
  | 'error'
  | 'early_return'
  | 'cancellation'
  | 'counterbid'
  | 'job_routing'
  | 'wallet_selection'
  | 'floor_price_check'
  | 'bid_calculation'
  | 'order_check'
  | 'trait_filtering'
  | 'batch_processing'
  | 'api_call'
  | 'rate_limit'
  | 'scheduling';

export type Marketplace = 'blur' | 'opensea' | 'magiceden' | 'system';

export interface MarketLogMetadata {
  [key: string]: any;
}

export interface MarketLogEntry {
  timestamp: string;
  marketplace: Marketplace;
  taskId: string;
  function: string; // allow marketplace-specific names
  stage: MarketLogStage;
  level: MarketLogLevel;
  message: string;
  metadata: MarketLogMetadata;
}

export interface MarketLogFilter {
  taskId?: string;
  function?: string;
  stage?: MarketLogStage;
  level?: MarketLogLevel | 'all';
  timeRange?: '5m' | '10m' | '1h' | '24h' | 'custom';
  startTime?: Date;
  endTime?: Date;
}

export interface MarketLogSummary {
  total: number;
  byLevel: Record<MarketLogLevel, number>;
  byFunction: Record<string, number>;
  byStage: Record<string, number>;
}

