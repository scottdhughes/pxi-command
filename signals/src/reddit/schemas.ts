/**
 * Zod schemas for validating Reddit API responses.
 *
 * Using Zod provides runtime validation of external API data,
 * ensuring type safety even when Reddit's API returns unexpected formats.
 */
import { z } from "zod"

// ─────────────────────────────────────────────────────────────────────────────
// Core Schemas
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Schema for a single Reddit post's data.
 */
export const RedditPostDataSchema = z.object({
  id: z.string(),
  created_utc: z.number(),
  title: z.string().default(""),
  selftext: z.string().optional().default(""),
  permalink: z.string().startsWith("/r/"),
  score: z.number().default(0),
  num_comments: z.number().default(0),
})

/**
 * Schema for a Reddit comment's data.
 */
export const RedditCommentDataSchema = z.object({
  id: z.string(),
  created_utc: z.number(),
  body: z.string(),
  permalink: z.string().startsWith("/r/"),
})

/**
 * Schema for a single child in a listing.
 */
export const RedditListingChildSchema = z.object({
  kind: z.string().optional(),
  data: RedditPostDataSchema,
})

/**
 * Schema for Reddit's listing response format.
 * This is the standard paginated response from /r/subreddit endpoints.
 */
export const RedditListingSchema = z.object({
  kind: z.literal("Listing").optional(),
  data: z.object({
    children: z.array(RedditListingChildSchema),
    after: z.string().nullable(),
    before: z.string().nullable().optional(),
  }),
})

/**
 * Schema for comment listing children (can include "more" items).
 */
export const RedditCommentChildSchema = z.object({
  kind: z.string().optional(),
  data: z.object({
    id: z.string().optional(),
    created_utc: z.number().optional(),
    body: z.string().optional(),
    permalink: z.string().optional(),
  }).passthrough(),
})

/**
 * Schema for the comment listing response.
 */
export const RedditCommentListingSchema = z.object({
  kind: z.literal("Listing").optional(),
  data: z.object({
    children: z.array(RedditCommentChildSchema),
  }),
})

/**
 * Schema for OAuth token response.
 */
export const RedditOAuthResponseSchema = z.object({
  access_token: z.string(),
  token_type: z.string(),
  expires_in: z.number(),
  scope: z.string().optional(),
})

// ─────────────────────────────────────────────────────────────────────────────
// Type Exports
// ─────────────────────────────────────────────────────────────────────────────

export type RedditPostData = z.infer<typeof RedditPostDataSchema>
export type RedditCommentData = z.infer<typeof RedditCommentDataSchema>
export type RedditListing = z.infer<typeof RedditListingSchema>
export type RedditOAuthResponse = z.infer<typeof RedditOAuthResponseSchema>

// ─────────────────────────────────────────────────────────────────────────────
// Validation Helpers
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Safely parses a Reddit listing response with detailed error reporting.
 * Returns null if parsing fails, with error details logged.
 */
export function parseRedditListing(data: unknown): RedditListing | null {
  const result = RedditListingSchema.safeParse(data)
  if (!result.success) {
    console.warn("[Reddit Schema] Listing validation failed:", result.error.issues)
    return null
  }
  return result.data
}

/**
 * Safely parses OAuth response.
 */
export function parseOAuthResponse(data: unknown): RedditOAuthResponse | null {
  const result = RedditOAuthResponseSchema.safeParse(data)
  if (!result.success) {
    console.warn("[Reddit Schema] OAuth validation failed:", result.error.issues)
    return null
  }
  return result.data
}
