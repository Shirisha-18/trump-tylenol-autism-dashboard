import asyncio
import os
import pandas as pd
from src.extract import fetch_posts_batch, fetch_all_comments
from src.preprocess import preprocess_posts, preprocess_comments, merge_posts_comments

RAW_DIR = "data/raw"
INTERIM_DIR = "data/interim"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(INTERIM_DIR, exist_ok=True)


async def main():
    query = "Trump Tylenol autism"
    post_limit = 500  # Increase as needed, e.g., 10000+

    print("Fetching posts asynchronously...")
    posts_df = await fetch_posts_batch(query=query, limit=post_limit)
    print(f"Fetched {len(posts_df)} posts")

    # Save raw posts CSV
    raw_posts_path = os.path.join(RAW_DIR, "reddit_posts_raw.csv")
    posts_df.to_csv(raw_posts_path, index=False)
    print(f"Raw posts saved: {raw_posts_path}")

    print("Fetching comments asynchronously...")
    comments_df = await fetch_all_comments(posts_df)
    print(f"Fetched {len(comments_df)} comments")

    # Save raw comments CSV
    raw_comments_path = os.path.join(RAW_DIR, "reddit_comments_raw.csv")
    comments_df.to_csv(raw_comments_path, index=False)
    print(f"Raw comments saved: {raw_comments_path}")

    # Preprocess posts + comments
    posts_clean = preprocess_posts(posts_df)
    comments_clean = preprocess_comments(comments_df)
    combined_df = merge_posts_comments(posts_clean, comments_clean)

    # Save preprocessed merged CSV
    preprocessed_path = os.path.join(INTERIM_DIR, "reddit_preprocessed.csv")
    combined_df.to_csv(preprocessed_path, index=False)
    print(f"Preprocessed merged CSV saved: {preprocessed_path}")

    # Summary
    print("=== Summary ===")
    print(f"Raw posts: {len(posts_df)} rows, columns: {posts_df.shape[1]}")
    print(f"Raw comments: {len(comments_df)} rows, columns: {comments_df.shape[1]}")
    print(
        f"Preprocessed merged: {len(combined_df)} rows, columns: {combined_df.shape[1]}"
    )
    print(combined_df.head())


if __name__ == "__main__":
    asyncio.run(main())
