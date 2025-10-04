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
