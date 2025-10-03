import pandas as pd
import numpy as np
import re
import html
from datetime import datetime


def clean_text(text):
    """Normalize text: remove HTML entities, multiple spaces, and newlines."""
    if pd.isna(text):
        return ""
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def preprocess_posts(posts_df):
    df = posts_df.copy()
    df["title"] = df["title"].map(clean_text)
    df["selftext"] = df["selftext"].map(clean_text)
    df["text"] = (df["title"] + ". " + df["selftext"]).str.strip()
    if "created_utc" in df.columns:
        df["created"] = pd.to_datetime(df["created_utc"], unit="s")
    df = df.drop_duplicates(subset=["post_id", "text"])
    return df


def preprocess_comments(comments_df):
    df = comments_df.copy()
    df["body"] = df["body"].map(clean_text)
    df = df.drop_duplicates(subset=["comment_id", "body"])
    return df


def merge_posts_comments(posts_df, comments_df):
    posts_df_ = posts_df.copy()
    posts_df_["source"] = "post"
    posts_df_ = posts_df_[
        ["post_id", "subreddit", "text", "score", "ups", "downs", "source"]
    ]

    comments_df_ = comments_df.copy()
    comments_df_["source"] = "comment"
    comments_df_ = comments_df_[["post_id", "comment_id", "body", "score", "source"]]
    comments_df_ = comments_df_.rename(columns={"body": "text"})

    combined_df = pd.concat([posts_df_, comments_df_], ignore_index=True, sort=False)
    return combined_df
