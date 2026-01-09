export type Category =
  | 'positioning'  // v1.1: renamed from 'liquidity'
  | 'credit'
  | 'volatility'
  | 'breadth'
  | 'macro'
  | 'global'
  | 'crypto';

export type DataSource =
  | 'fred'
  | 'yahoo'
  | 'cboe'
  | 'aaii'
  | 'barchart'
  | 'farside'
  | 'defillama'
  | 'coinglass'
  | 'calculated';

export type NormalizationMethod =
  | 'percentile'
  | 'percentile_inverted'
  | 'zscore'
  | 'direct'
  | 'bellcurve';

export type UpdateFrequency =
  | 'realtime'
  | 'daily'
  | 'weekly'
  | 'monthly';

export interface IndicatorDefinition {
  id: string;
  name: string;
  category: Category;
  source: DataSource;
  ticker: string;
  frequency: UpdateFrequency;
  normalization: NormalizationMethod;
  inverted: boolean;
  description: string;
}

export interface IndicatorValue {
  indicatorId: string;
  date: Date;
  value: number;
  normalizedValue?: number;
}

export interface CategoryScore {
  category: Category;
  score: number;
  weight: number;
  weightedScore: number;
  indicators: {
    id: string;
    name: string;
    rawValue: number;
    normalizedValue: number;
  }[];
}

export interface PXIResult {
  date: Date;
  score: number;
  label: string;
  status: 'max_pamp' | 'pamping' | 'neutral' | 'soft' | 'dumping';
  categories: CategoryScore[];
  delta7d?: number;
  delta30d?: number;
}

// v1.1 category weights
export const CATEGORY_WEIGHTS: Record<Category, number> = {
  positioning: 0.15,  // v1.1: was liquidity 0.22
  credit: 0.20,       // v1.1: was 0.18
  volatility: 0.20,   // v1.1: was 0.18
  breadth: 0.15,      // v1.1: was 0.12
  macro: 0.10,
  global: 0.10,
  crypto: 0.10,
};
