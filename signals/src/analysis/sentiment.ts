import { SentimentIntensityAnalyzer } from "vader-sentiment"

/**
 * Calculates sentiment score for a text using VADER sentiment analysis.
 *
 * VADER (Valence Aware Dictionary and sEntiment Reasoner) is particularly
 * suited for social media text as it handles:
 * - Emoticons and emoji
 * - Slang and abbreviations
 * - Capitalization emphasis
 * - Degree modifiers ("very", "extremely")
 *
 * @param text - Text to analyze (post title, body, or comment)
 * @returns Compound sentiment score in range [-1, 1]
 *   - -1.0 = extremely negative
 *   -  0.0 = neutral
 *   -  1.0 = extremely positive
 *
 * @example
 * ```typescript
 * scoreSentiment("This stock is amazing! 🚀")  // ~0.7
 * scoreSentiment("Terrible earnings, avoid")   // ~-0.5
 * scoreSentiment("Q3 report released")         // ~0.0
 * ```
 */
export function scoreSentiment(text: string): number {
  const result = SentimentIntensityAnalyzer.polarity_scores(text)
  return result.compound
}
