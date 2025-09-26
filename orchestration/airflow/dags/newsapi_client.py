import os
import requests
import json
from datetime import datetime

BRONZE_PATH = "/opt/airflow/data/bronze"

def fetch_and_store_news():

    url = f"https://newsapi.org/v2/top-headlines?language=en&pageSize=10&apiKey=8eff43b0196d4bcc9972a7c920e9c037"
    resp = requests.get(url).json()

    #  Error handling
    if resp.get("status") != "ok":
        raise RuntimeError(
            f"NewsAPI error: {resp.get('code')} - {resp.get('message')}"
        )

    articles = resp.get("articles", [])
    if not articles:
        raise RuntimeError("NewsAPI returned no articles.")

    os.makedirs(BRONZE_PATH, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    out_file = os.path.join(BRONZE_PATH, f"news_{ts}.json")

    with open(out_file, "w") as f:
        json.dump(articles, f)

    print(f" Saved {len(articles)} articles to {out_file}")
    return out_file

if __name__ == "__main__":
    fetch_and_store_news()
