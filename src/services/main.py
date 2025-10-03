# main.py
from src.services.data_extract import search_reddit


def main():
    # Example query
    df = search_reddit("Trump tylenol autism", limit=50, subreddit="all")

    # Print top columns
    print(
        df[
            [
                "subreddit",
                "author",
                "title",
                "score",
                "num_comments",
                "url",
                "created",
            ]
        ].head(10)
    )

    # Save to CSV for later preprocessing
    #df.to_csv("data/raw_reddit_posts.csv", index=False)
    #print("Saved raw Reddit posts to data/raw_reddit_posts.csv")


if __name__ == "__main__":
    main()
