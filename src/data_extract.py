import pandas as pd
import requests
from src.services.config import get_reddit_token
import time


def fetch_posts(query, limit=100, subreddit="all"):
    """
    Search Reddit for posts with a query and return as DataFrame.
    Handles batch requests if limit > 100 (Reddit API returns max 100 per request).
    """
    headers = get_reddit_token()
    url = f"https://oauth.reddit.com/r/{subreddit}/search"
    all_posts = []
    after = None
    batch_size = min(limit, 100)
    fetched = 0

    while fetched < limit:
        params = {
            "q": query,
            "limit": batch_size,
            "restrict_sr": False,
            "sort": "relevance",
            "after": after,
        }

        res = requests.get(url, headers=headers, params=params)
        res.raise_for_status()
        posts = res.json().get("data", {}).get("children", [])

        if not posts:
            break

        for post in posts:
            data = post.get("data", {})
            all_posts.append(
                {
                    "post_id": data.get("id"),
                    "subreddit": data.get("subreddit"),
                    "title": data.get("title"),
                    "selftext": data.get("selftext"),
                    "url": data.get("url"),
                    "ups": data.get("ups"),
                    "downs": data.get("downs"),
                    "score": data.get("score"),
                    "created_utc": data.get("created_utc"),
                }
            )

        fetched += len(posts)
        after = posts[-1]["data"]["name"]  # for pagination
        time.sleep(1)  # avoid rate limiting

    return pd.DataFrame(all_posts)


def fetch_comments(post_id):
    """
    Fetch comments for a given post ID.
    """
    headers = get_reddit_token()
    url = f"https://oauth.reddit.com/comments/{post_id}"
    res = requests.get(url, headers=headers, params={"limit": 500})
    res.raise_for_status()
    comments_data = res.json()

    all_comments = []
    if len(comments_data) > 1:
        comments_list = comments_data[1].get("data", {}).get("children", [])
        for comment in comments_list:
            data = comment.get("data", {})
            all_comments.append(
                {
                    "post_id": post_id,
                    "comment_id": data.get("id"),
                    "body": data.get("body"),
                    "ups": data.get("ups"),
                    "downs": data.get("downs"),
                    "score": data.get("score"),
                }
            )

    return pd.DataFrame(all_comments)
