import os
import pandas as pd
import numpy as np
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from datetime import datetime
import re

# Paths
INTERIM_DIR = "data/interim"
PREPROCESSED_FILE = os.path.join(INTERIM_DIR, "reddit_preprocessed.csv")
WRANGLED_FILE = os.path.join(INTERIM_DIR, "reddit_wrangled.csv")

# Load preprocessed data
df = pd.read_csv(PREPROCESSED_FILE, parse_dates=["created"])

print(f"Loaded preprocessed data: {df.shape[0]} rows, {df.shape[1]} columns")

# -------------------------------
# 1. Temporal Features
# -------------------------------
df['year'] = df['created'].dt.year
df['month'] = df['created'].dt.month
df['day'] = df['created'].dt.day
df['weekday'] = df['created'].dt.weekday
df['hour'] = df['created'].dt.hour

# -------------------------------
# 2. Text Features
# -------------------------------
# Ensure text column is string
df["text"] = df["text"].fillna("").astype(str)

df["word_count"] = df["text"].str.split().apply(len)
df["char_count"] = df["text"].str.len()
df["avg_word_len"] = df["char_count"] / df["word_count"].replace(0, 1)
df["has_url"] = df["text"].str.contains(r"http", regex=True).astype(int)


# -------------------------------
# 3. Engagement Features
# -------------------------------
df["engagement"] = df["ups"] + df["downs"] + df["score"]

# Comments per post
if "source" in df.columns and (df["source"] == "comment").any():
    comments_per_post = (
        df[df["source"] == "comment"].groupby("post_id").size().rename("num_comments")
    )
    df = df.merge(comments_per_post, on="post_id", how="left")

# Ensure 'num_comments' exists
if "num_comments" not in df.columns:
    df["num_comments"] = 0
else:
    df["num_comments"] = df["num_comments"].fillna(0).astype(int)


# -------------------------------
# 4. Author / Subreddit Stats
# -------------------------------
# Posts per author
author_post_count = (
    df[df['source']=='post'].groupby('author').size().rename('author_post_count')
)
df = df.merge(author_post_count, on='author', how='left')

# Comments per author
author_comment_count = (
    df[df['source']=='comment'].groupby('author').size().rename('author_comment_count')
)
df = df.merge(author_comment_count, on='author', how='left')

# Posts per subreddit
subreddit_post_count = (
    df[df['source']=='post'].groupby('subreddit').size().rename('subreddit_post_count')
)
df = df.merge(subreddit_post_count, on='subreddit', how='left')

# -------------------------------
# 5. Domain / URL Features
# -------------------------------
df['main_domain'] = df['url'].str.extract(r'https?://([^/]+)/')[0]

# -------------------------------
# 6. Sentiment Features (VADER)
# -------------------------------
sia = SentimentIntensityAnalyzer()
df['sentiment_compound'] = df['text'].apply(lambda x: sia.polarity_scores(str(x))['compound'])
df['sentiment_label'] = df['sentiment_compound'].apply(
    lambda x: 'positive' if x>0 else ('negative' if x<0 else 'neutral')
)

# -------------------------------
# 7. Optional: Keyword / Topic Placeholders
# -------------------------------
df['topic'] = np.nan  # can fill later using BERTopic or LDA
df['stance'] = np.nan  # optional stance labeling for misinformation analysis

# -------------------------------
# 8. Save munged dataset
# -------------------------------
df.to_csv(WRANGLED_FILE, index=False)
print(f"Wrangled dataset saved: {WRANGLED_FILE}")
print(f"Final shape: {df.shape[0]} rows, {df.shape[1]} columns")
print(df.head())
