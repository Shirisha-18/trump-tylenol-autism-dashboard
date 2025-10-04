import aiohttp
import asyncio
import pandas as pd
from src.services.config import get_reddit_token

BATCH_SIZE = 100  # Reddit API max per request


async def fetch(session, url, headers, params=None):
    async with session.get(url, headers=headers, params=params) as response:
        response.raise_for_status()
        return await response.json()


async def fetch_posts_batch(query, limit=100, subreddit="all"):
    """Fetch Reddit posts asynchronously in batches of 100."""
    headers = get_reddit_token()
    url = f"https://oauth.reddit.com/r/{subreddit}/search"
    all_posts = []
    after = None
    fetched = 0

    async with aiohttp.ClientSession() as session:
        while fetched < limit:
            batch_size = min(BATCH_SIZE, limit - fetched)
            params = {
                "q": query,
                "limit": batch_size,
                "restrict_sr": "false",
                "sort": "relevance",
            }
            if after:
                params["after"] = after

            data = await fetch(session, url, headers, params)
            posts = data.get("data", {}).get("children", [])
            if not posts:
                break

            for post in posts:
                d = post.get("data", {})
                all_posts.append(
                    {
                        "post_id": d.get("id"),
                        "subreddit": d.get("subreddit"),
                        "author": d.get("author"),
                        "title": d.get("title"),
                        "selftext": d.get("selftext"),
                        "url": d.get("url"),
                        "ups": d.get("ups"),
                        "downs": d.get("downs"),
                        "score": d.get("score"),
                        "num_comments": d.get("num_comments"),
                        "created_utc": d.get("created_utc"),
                        "permalink": d.get("permalink"),
                        "domain": d.get("domain"),
                    }
                )

            fetched += len(posts)
            after = posts[-1]["data"]["name"] if posts else None
            await asyncio.sleep(1)  # avoid rate limiting

    return pd.DataFrame(all_posts)


async def fetch_comments_for_post(session, headers, post_id):
    """Fetch comments for a single post asynchronously."""
    url = f"https://oauth.reddit.com/comments/{post_id}"
    data = await fetch(session, url, headers, params={"limit": 500})
    all_comments = []

    if len(data) > 1:
        comments_list = data[1].get("data", {}).get("children", [])
        for comment in comments_list:
            d = comment.get("data", {})
            all_comments.append(
                {
                    "post_id": post_id,
                    "comment_id": d.get("id"),
                    "author": d.get("author"),
                    "body": d.get("body"),
                    "ups": d.get("ups"),
                    "downs": d.get("downs"),
                    "score": d.get("score"),
                    "created_utc": d.get("created_utc"),
                    "permalink": d.get("permalink"),
                }
            )
    return all_comments


async def fetch_all_comments(posts_df):
    """Fetch comments for all posts asynchronously."""
    headers = get_reddit_token()
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_comments_for_post(session, headers, post_id)
            for post_id in posts_df["post_id"]
        ]
        results = await asyncio.gather(*tasks)
    # Flatten list of lists
    all_comments = [item for sublist in results for item in sublist]
    return pd.DataFrame(all_comments)
