export interface RedditComment {
  id: string
  created_utc: number
  body: string
  permalink: string
}

export interface RedditPost {
  id: string
  subreddit: string
  created_utc: number
  title: string
  selftext: string
  permalink: string
  score: number
  num_comments: number
  comments?: RedditComment[]
}

export interface RedditDataset {
  generated_at_utc: string
  subreddits: string[]
  posts: RedditPost[]
}
