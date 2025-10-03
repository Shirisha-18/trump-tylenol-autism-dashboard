# data_extract.py
import pandas as pd
import requests
from src.services.config import get_reddit_token


def search_reddit(query, limit=50, subreddit="all"):
    """
    Search Reddit for a query string and return results as a DataFrame.

    Returns a rich DataFrame with:
    - subreddit, author, title, selftext, combined text
    - url, domain, score, upvotes, downvotes, upvote_ratio
    - num_comments, created_utc, created datetime
    - permalink
    """
    headers = get_reddit_token()
    url = f"https://oauth.reddit.com/r/{subreddit}/search"
    params = {"q": query, "limit": limit, "restrict_sr": False, "sort": "relevance"}

    res = requests.get(url, headers=headers, params=params)
    res.raise_for_status()
    posts = res.json().get("data", {}).get("children", [])

    posts_list = []
    for post in posts:
        data = post.get("data", {})
        posts_list.append(
            {
                "subreddit": data.get("subreddit"),
                "author": data.get("author"),
                "title": data.get("title"),
                "selftext": data.get("selftext"),
                "text": (
                    str(data.get("title", "")) + ". " + str(data.get("selftext", ""))
                ).strip(),
                "url": data.get("url"),
                "domain": data.get("domain"),
                "score": data.get("score"),
                "ups": data.get("ups"),
                "downs": data.get("downs"),
                "upvote_ratio": data.get("upvote_ratio"),
                "num_comments": data.get("num_comments"),
                "created_utc": data.get("created_utc"),
                "permalink": data.get("permalink"),
            }
        )

    df = pd.DataFrame(posts_list)

    # Convert created_utc to datetime
    if "created_utc" in df.columns:
        df["created"] = pd.to_datetime(df["created_utc"], unit="s")

    # Fill NaNs in text fields
    df["title"] = df["title"].fillna("")
    df["selftext"] = df["selftext"].fillna("")
    df["text"] = df["text"].fillna("")

    return df
