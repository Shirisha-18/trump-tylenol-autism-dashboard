import requests
import os
from dotenv import load_dotenv

# Load credentials from .env
load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
USERNAME = 'Shirisha_18'  # os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")


def get_reddit_token():
    """Authenticate with Reddit and return request headers containing the access token."""
    auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
    data = {"grant_type": "password", "username": USERNAME, "password": PASSWORD}
    headers = {"User-Agent": "DataForGood/0.0.1"}
    res = requests.post(
        "https://www.reddit.com/api/v1/access_token",
        auth=auth,
        data=data,
        headers=headers,
    )
    res.raise_for_status()
    TOKEN = res.json()["access_token"]
    return {"Authorization": f"bearer {TOKEN}", "User-Agent": "DataForGood/0.0.1"}
