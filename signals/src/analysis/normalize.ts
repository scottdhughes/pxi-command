/**
 * Normalizes an array of values using z-score standardization.
 *
 * Z-score formula: (value - mean) / standard_deviation
 *
 * This transforms values to have mean=0 and std=1, making it possible
 * to compare and combine metrics that have different scales.
 *
 * @param values - Array of numeric values to normalize
 * @returns Array of z-scores (same length as input)
 *
 * @example
 * ```typescript
 * zScore([1, 2, 3, 4, 5])  // [-1.41, -0.71, 0, 0.71, 1.41]
 * zScore([])               // []
 * zScore([5, 5, 5])        // [0, 0, 0] (std defaults to 1 when variance is 0)
 * ```
 */
export function zScore(values: number[]): number[] {
  const n = values.length
  if (n === 0) return []
  const mean = values.reduce((a, b) => a + b, 0) / n
  const variance = values.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / n
  const std = Math.sqrt(variance) || 1
  return values.map((v) => (v - mean) / std)
}
