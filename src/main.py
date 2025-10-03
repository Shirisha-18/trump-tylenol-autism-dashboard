import pandas as pd
from src.data_extract import fetch_posts, fetch_comments
from src.preprocess import preprocess_posts, preprocess_comments, merge_posts_comments


def main():
    query = "Trump Tylenol autism"
    post_limit = 500  # number of posts to fetch
    print("Fetching posts...")
    posts_df = fetch_posts(query=query, limit=post_limit)

    print(f"Fetched {len(posts_df)} posts.")
    posts_clean = preprocess_posts(posts_df)

    # Fetch comments for each post (batch safe)
    all_comments = []
    print("Fetching comments for posts...")
    for i, post_id in enumerate(posts_clean["post_id"], 1):
        comments_df = fetch_comments(post_id)
        all_comments.append(comments_df)
        if i % 50 == 0:
            print(f"Processed {i}/{len(posts_clean)} posts for comments.")

    comments_clean = preprocess_comments(pd.concat(all_comments, ignore_index=True))

    # Merge posts + comments
    combined_df = merge_posts_comments(posts_clean, comments_clean)

    # Save to CSV for further analysis
    combined_df.to_csv("data/reddit_trump_tylenol_combined.csv", index=False)
    print("Data saved to data/reddit_trump_tylenol_combined.csv")
    print(combined_df.head())


if __name__ == "__main__":
    main()
