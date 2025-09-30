from services.data_extract import search_reddit


def main():
    df = search_reddit("Asia Cup 2025", limit=25)
    print(df[["subreddit", "title", "score", "url"]])


if __name__ == "__main__":
    main()
