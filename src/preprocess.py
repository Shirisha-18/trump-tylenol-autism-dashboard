import pandas as pd
import numpy as np
import re
import html


def clean_text(text):
    if pd.isna(text):
        return ""
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def preprocess_posts(posts_df):
    df = posts_df.copy()
    df["title"] = df["title"].map(clean_text)
    df["selftext"] = df["selftext"].map(clean_text)
    df["text"] = (df["title"] + ". " + df["selftext"]).str.strip()
    df["created"] = pd.to_datetime(df["created_utc"], unit="s")
    df = df.drop_duplicates(subset=["post_id", "text"])
    return df


def preprocess_comments(comments_df):
    df = comments_df.copy()
    df["body"] = df["body"].map(clean_text)
    df["text"] = df["body"]
    df["created"] = pd.to_datetime(df["created_utc"], unit="s")
    df = df.drop_duplicates(subset=["comment_id", "text"])
    return df


def merge_posts_comments(posts_df, comments_df):
    posts_df_ = posts_df.copy()
    posts_df_["source"] = "post"
    posts_df_ = posts_df_[
        [
            "post_id",
            "subreddit",
            "author",
            "text",
            "score",
            "ups",
            "downs",
            "source",
            "created",
            "url",
            "permalink",
            "domain",
            "num_comments",
        ]
    ]

    comments_df_ = comments_df.copy()
    comments_df_["source"] = "comment"
    comments_df_ = comments_df_[
        [
            "post_id",
            "comment_id",
            "author",
            "text",
            "score",
            "ups",
            "downs",
            "source",
            "created",
            "permalink",
        ]
    ]
    comments_df_["url"] = np.nan
    comments_df_["domain"] = np.nan
    comments_df_["num_comments"] = np.nan

    combined_df = pd.concat([posts_df_, comments_df_], ignore_index=True, sort=False)
    combined_df["engagement"] = (
        combined_df["ups"] + combined_df["downs"] + combined_df["score"]
    )

    return combined_df
