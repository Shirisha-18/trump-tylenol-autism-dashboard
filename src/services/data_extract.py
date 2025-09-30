import pandas as pd
import requests
from config import get_reddit_token


def search_reddit(query, limit=25, subreddit="all"):
    """Search Reddit posts and return a DataFrame."""
    headers = get_reddit_token()
    url = f"https://oauth.reddit.com/r/{subreddit}/search"
    params = {"q": query, "limit": limit, "restrict_sr": False, "sort": "relevance"}

    res = requests.get(url, headers=headers, params=params)
    res.raise_for_status()
    posts = res.json().get("data", {}).get("children", [])

    posts_list = [
        {
            "subreddit": p["data"].get("subreddit"),
            "title": p["data"].get("title"),
            "selftext": p["data"].get("selftext"),
            "url": p["data"].get("url"),
            "upvote_ratio": p["data"].get("upvote_ratio"),
            "ups": p["data"].get("ups"),
            "downs": p["data"].get("downs"),
            "score": p["data"].get("score"),
        }
        for p in posts
    ]
    return pd.DataFrame(posts_list)
