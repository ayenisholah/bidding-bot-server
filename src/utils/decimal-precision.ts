/**
 * Utility functions for consistent decimal precision handling
 * All price calculations should use exactly 9 decimal places
 */

/**
 * Rounds a number to exactly 9 decimal places
 * @param value - The number to round
 * @returns The rounded number with 9 decimal precision
 */
export function roundToNineDecimals(value: number): number {
  return Math.round(value * 1000000000) / 1000000000;
}

/**
 * Tolerance for price comparisons (9 decimal places)
 * Use this instead of hardcoded values like 0.0001
 */
export const PRICE_COMPARISON_TOLERANCE = 0.000000001;

/**
 * Compares two prices with appropriate tolerance
 * @param price1 - First price to compare
 * @param price2 - Second price to compare
 * @returns true if prices are equal within tolerance
 */
export function pricesEqual(price1: number, price2: number): boolean {
  return Math.abs(price1 - price2) < PRICE_COMPARISON_TOLERANCE;
}

/**
 * Converts a price string to a number with 9 decimal precision
 * @param priceStr - Price as a string
 * @returns Price as a number with 9 decimal precision
 */
export function parsePrice(priceStr: string): number {
  return roundToNineDecimals(parseFloat(priceStr));
}